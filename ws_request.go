package okx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/coder/websocket"
	json "github.com/go-json-experiment/json"
)

type wsPending struct {
	op        string
	control   bool
	done      chan WSOperationResponse
	onSuccess func()
}

func (s *wsSession) finish(id uint64, response WSOperationResponse) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pending[id]
	if p == nil {
		return false
	}
	delete(s.pending, id)
	if response.Err == nil && p.onSuccess != nil {
		p.onSuccess()
	}
	p.done <- response
	return true
}

func (s *wsSession) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, p := range s.pending {
		delete(s.pending, id)
		p.done <- WSOperationResponse{Err: err}
	}
}

func (c *WSClient) request(ctx context.Context, s *wsSession, msg opMessage, control bool, onSuccess func()) (WSOperationResponse, error) {
	ctx, stop := sessionContext(ctx, s)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return WSOperationResponse{}, err
	}
	if control && c.rl != nil {
		if err := s.controls.WaitRequests(ctx, []RateLimitRequest{{Key: "control", Cost: 1, Limit: 480, Window: time.Hour}}); err != nil {
			return WSOperationResponse{}, err
		}
	}
	if msg.ID == 0 {
		msg.ID = c.reqID.Add(1)
	}
	p := &wsPending{op: msg.Op, control: control, done: make(chan WSOperationResponse, 1), onSuccess: onSuccess}
	s.mu.Lock()
	if s.ctx.Err() != nil {
		s.mu.Unlock()
		return WSOperationResponse{}, ErrWSNotConnected
	}
	if msg.Op == "login" {
		for _, existing := range s.pending {
			if existing.op == "login" {
				s.mu.Unlock()
				return WSOperationResponse{}, ErrWSConcurrentOperation
			}
		}
	}
	s.pending[msg.ID] = p
	s.mu.Unlock()
	if err := c.writeSession(ctx, s, msg); err != nil {
		s.finish(msg.ID, WSOperationResponse{Err: sessionError(s, err)})
	}
	select {
	case response := <-p.done:
		return response, response.Err
	case <-ctx.Done():
		err := context.Cause(ctx)
		if s.ctx.Err() != nil {
			err = errors.Join(ErrWSConnectionLost, context.Cause(s.ctx))
		}
		s.finish(msg.ID, WSOperationResponse{Err: err})
		// A simultaneous response and timeout compete through finish. Whichever
		// removes the registry entry owns completion; no success is discarded.
		response := <-p.done
		return response, response.Err
	}
}

func (c *WSClient) writeSession(ctx context.Context, s *wsSession, msg any) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer func() {
		if buf.Cap() <= int(c.readLimit) {
			bufPool.Put(buf)
		}
	}()
	if err := json.MarshalWrite(buf, msg); err != nil {
		return err
	}
	return s.conn.Write(ctx, websocket.MessageText, buf.Bytes())
}

func (c *WSClient) loginSession(ctx context.Context, s *wsSession) error {
	if c.signer == nil {
		return ErrUnauthorized
	}
	ts := strconv.FormatInt(c.now().Unix(), 10)
	msg := opMessage{Op: "login", Args: []any{loginArg{
		APIKey: c.cred.APIKey, Passphrase: c.cred.Passphrase, Timestamp: ts,
		Sign: c.signer.Sign(ts, "GET", "/users/self/verify", ""),
	}}}
	_, err := c.request(ctx, s, msg, true, nil)
	return err
}

func (c *WSClient) login(ctx context.Context) error {
	s, err := c.getSession(true)
	if err != nil {
		return err
	}
	if err := c.loginSession(ctx, s); err != nil {
		// Login acknowledgements may omit id. End the session on failure so a
		// late acknowledgement cannot authenticate a later login attempt.
		s.cancel(err)
		return err
	}
	c.loginRequired.Store(true)
	return nil
}

func (c *WSClient) sendControl(ctx context.Context, s *wsSession, op string, arg subArg, success func()) error {
	_, err := c.request(ctx, s, opMessage{Op: op, Args: []any{arg}}, true, success)
	return err
}

func (c *WSClient) Do(ctx context.Context, op string, args ...any) (WSOperationResponse, error) {
	if op == "" {
		return WSOperationResponse{}, required("op")
	}
	if op == "login" || op == "subscribe" || op == "unsubscribe" {
		return WSOperationResponse{}, fmt.Errorf("%w: use the dedicated %s method", ErrInvalidParameter, op)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	s, err := c.getSession(true)
	if err != nil {
		return WSOperationResponse{}, err
	}
	if c.rl != nil {
		requests, err := tradeRateRequests(c.rateScope, op, args, c.catalog)
		if err != nil {
			return WSOperationResponse{}, err
		}
		if requests == nil {
			requests = []RateLimitRequest{{Key: c.rateScope + ":ws:" + op, Cost: 1, Limit: 10, Window: time.Second}}
		}
		if err := waitRateRequests(ctx, c.rl, requests); err != nil {
			return WSOperationResponse{}, err
		}
	}
	return c.request(ctx, s, opMessage{Op: op, Args: args}, false, nil)
}

// resubscribeSession marks each desired subscription active only when its ACK
// arrives. The read worker can then deliver an immediately following snapshot.
func (c *WSClient) resubscribeSession(ctx context.Context, s *wsSession) error {
	for _, entry := range *c.handlers.Load() {
		if !entry.desired.Load() {
			continue
		}
		if err := c.sendControl(ctx, s, "subscribe", entry.arg, func() {
			if entry.desired.Load() {
				entry.active.Store(s)
			} else {
				s.cancel(ErrWSConnectionLost)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}
