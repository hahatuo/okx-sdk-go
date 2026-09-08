package okx

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// A run owns all reconnect attempts. It is published before starting its worker
// and remains installed until every connection worker has exited.
type wsRun struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// A session owns exactly one connection and its request registry. Old frames
// and connection teardown can never complete requests on a later session.
type wsSession struct {
	conn     *websocket.Conn
	ctx      context.Context
	cancel   context.CancelCauseFunc
	ready    atomic.Bool
	lastRead atomic.Int64
	mu       sync.Mutex
	pending  map[uint64]*wsPending
	controls *MultiRateLimiter
}

func newWSSession(ctx context.Context, conn *websocket.Conn) *wsSession {
	ctx, cancel := context.WithCancelCause(ctx)
	controls := DefaultRateLimiter()
	controls.quotas = &quotaState{windows: make(map[string]*quotaWindow)}
	return &wsSession{conn: conn, ctx: ctx, cancel: cancel, pending: make(map[uint64]*wsPending), controls: controls}
}

func (c *WSClient) connect(ctx context.Context, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.kind == Private && c.signer == nil {
		return ErrUnauthorized
	}
	runCtx, cancel := context.WithCancel(ctx)
	run := &wsRun{ctx: runCtx, cancel: cancel, done: make(chan struct{})}
	c.connMu.Lock()
	if c.run != nil {
		c.connMu.Unlock()
		cancel()
		return ErrWSAlreadyConnected
	}
	c.run = run
	c.connMu.Unlock()
	first := make(chan error, 1)
	go c.supervise(run, first)
	waitCtx := ctx
	if timeout > 0 {
		var stop context.CancelFunc
		waitCtx, stop = context.WithTimeout(ctx, timeout)
		defer stop()
	}
	select {
	case err := <-first:
		if err != nil {
			cancel()
		}
		return err
	case <-waitCtx.Done():
		cancel()
		// The supervisor retains run until teardown completes, preventing a
		// new Connect from overlapping a canceled initial handshake.
		return waitCtx.Err()
	}
}

func (c *WSClient) supervise(run *wsRun, first chan<- error) {
	defer func() {
		run.cancel()
		c.connMu.Lock()
		close(run.done)
		if c.run == run {
			c.run = nil
		}
		c.connMu.Unlock()
	}()
	reported := false
	report := func(err error) {
		if !reported {
			first <- err
			reported = true
		}
	}
	connected := false
	failures := 0
	backoff := c.reconnect.MinDelay
	for {
		if err := run.ctx.Err(); err != nil {
			report(err)
			return
		}
		err := waitRateRequests(run.ctx, c.rl, []RateLimitRequest{{Key: "ip:process:ws:connect", Cost: 1, Limit: 3, Window: time.Second}})
		var conn *websocket.Conn
		if err == nil {
			conn, err = c.dialFn(run.ctx, c.url)
		}
		if err == nil {
			conn.SetReadLimit(c.readLimit)
			s := newWSSession(run.ctx, conn)
			s.lastRead.Store(c.now().UnixNano())
			c.connMu.Lock()
			c.session = s
			c.connMu.Unlock()
			reachedReady := false
			err = c.serve(s, func() {
				reachedReady = true
				if connected {
					c.reconnects.Add(1)
				}
				connected = true
				report(nil)
				c.dispatchSystemEvent(WSSystemEvent{Event: "ready"})
			})
			c.connMu.Lock()
			if c.session == s {
				c.session = nil
			}
			c.connMu.Unlock()
			if reachedReady {
				failures = 0
				backoff = c.reconnect.MinDelay
			}
		}
		if !connected {
			report(err)
			return
		}
		if run.ctx.Err() != nil || !c.reconnect.Enabled {
			return
		}
		failures++
		if c.reconnect.MaxAttempts > 0 && failures > c.reconnect.MaxAttempts {
			return
		}
		c.dispatchSystemEvent(WSSystemEvent{Event: "reconnecting", Err: err})
		if !sleep(run.ctx, backoff) {
			return
		}
		backoff = min(backoff*2, c.reconnect.MaxDelay)
	}
}

func (c *WSClient) serve(s *wsSession, ready func()) error {
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		err := c.readLoop(s)
		s.ready.Store(false)
		s.cancel(err)
		c.invalidateSubscriptions(s, err)
	}()
	go func() {
		defer workers.Done()
		c.pingLoop(s)
	}()
	defer func() {
		s.ready.Store(false)
		s.cancel(ErrWSConnectionLost)
		_ = s.conn.CloseNow()
		s.fail(errors.Join(ErrWSConnectionLost, context.Cause(s.ctx)))
		workers.Wait()
		c.dispatchSystemEvent(WSSystemEvent{Event: "disconnected", Err: context.Cause(s.ctx)})
	}()
	if c.kind == Private || c.loginRequired.Load() {
		if err := c.loginSession(s.ctx, s); err != nil {
			return err
		}
	}
	if err := c.resubscribeSession(s.ctx, s); err != nil {
		return err
	}
	if err := s.ctx.Err(); err != nil {
		return context.Cause(s.ctx)
	}
	s.ready.Store(true)
	ready()
	<-s.ctx.Done()
	return context.Cause(s.ctx)
}

func (c *WSClient) readLoop(s *wsSession) error {
	var frame bytes.Buffer
	var in wsIncoming
	for {
		_, reader, err := s.conn.Reader(s.ctx)
		if err != nil {
			return err
		}
		frame.Reset()
		if _, err := frame.ReadFrom(reader); err != nil {
			return err
		}
		s.lastRead.Store(c.now().UnixNano())
		if err := c.dispatchSession(s, frame.Bytes(), &in); err != nil {
			return err
		}
	}
}

func (c *WSClient) pingLoop(s *wsSession) {
	t := time.NewTicker(c.pingInt)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			if c.now().UnixNano()-s.lastRead.Load() > int64(2*c.pingInt) {
				s.ready.Store(false)
				// Use a protocol close for the peer's diagnostic, bounded by the
				// library; other worker I/O is released by the session cancel.
				_ = s.conn.Close(websocket.StatusGoingAway, "read timeout")
				s.cancel(context.DeadlineExceeded)
				return
			}
			ctx, cancel := context.WithTimeout(s.ctx, c.pingInt)
			err := s.conn.Write(ctx, websocket.MessageText, []byte("ping"))
			cancel()
			if err != nil {
				s.cancel(err)
				return
			}
		}
	}
}

func (c *WSClient) getSession(requireReady bool) (*wsSession, error) {
	c.connMu.RLock()
	s := c.session
	c.connMu.RUnlock()
	if s == nil || s.ctx.Err() != nil || (requireReady && !s.ready.Load()) {
		return nil, ErrWSNotConnected
	}
	return s, nil
}

// Ready reports that authentication and all desired subscriptions have been
// acknowledged on the current connection. It does not imply order-book freshness.
func (c *WSClient) Ready() bool { _, err := c.getSession(true); return err == nil }

// RequestClose cancels the current run without waiting. It is safe in a Handler.
func (c *WSClient) RequestClose() {
	c.connMu.RLock()
	run := c.run
	c.connMu.RUnlock()
	if run != nil {
		run.cancel()
	}
}

// WaitClosed joins the current run. Do not call it from a Handler, which is one
// of the run's workers. Use RequestClose there and wait from the owner goroutine.
func (c *WSClient) WaitClosed(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.connMu.RLock()
	run := c.run
	c.connMu.RUnlock()
	if run == nil {
		return nil
	}
	select {
	case <-run.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close requests shutdown and joins exactly the run observed by this call.
// Handlers must use RequestClose instead of waiting for their own read loop.
func (c *WSClient) Close() error {
	c.connMu.RLock()
	run := c.run
	c.connMu.RUnlock()
	if run != nil {
		run.cancel()
		<-run.done
	}
	return nil
}

func (c *WSClient) requestRecovery(err error) {
	s, e := c.getSession(false)
	if e == nil {
		s.ready.Store(false)
		s.cancel(err)
	}
}

func (c *WSClient) invalidateSubscriptions(s *wsSession, err error) {
	if err == nil {
		err = ErrWSConnectionLost
	}
	for _, entry := range *c.handlers.Load() {
		if entry.active.CompareAndSwap(s, nil) && entry.onDisconnect != nil {
			entry.onDisconnect(err)
		}
	}
}

func sessionContext(ctx context.Context, s *wsSession) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.ctx, func() { cancel(context.Cause(s.ctx)) })
	if s.ctx.Err() != nil {
		cancel(context.Cause(s.ctx))
	}
	return ctx, func() { stop(); cancel(context.Canceled) }
}

func sessionError(s *wsSession, err error) error {
	if s.ctx.Err() != nil && !errors.Is(err, context.DeadlineExceeded) {
		return ErrWSConnectionLost
	}
	return err
}
