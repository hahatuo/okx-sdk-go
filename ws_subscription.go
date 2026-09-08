package okx

import (
	"context"
	"errors"
)

var ErrWSAlreadySubscribed = errors.New("okx: subscription already exists")

func (c *WSClient) SubscribeArgHandle(ctx context.Context, arg WSChannelArg, handler Handler) (*WSSubscription, error) {
	return c.subscribeArgHandle(ctx, arg, handler, nil)
}

func (c *WSClient) subscribeArgHandle(ctx context.Context, arg WSChannelArg, handler Handler, onDisconnect func(error)) (*WSSubscription, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	wire := arg.subArg()
	if wire.Channel == "" {
		return nil, required("channel")
	}
	if handler == nil {
		return nil, required("handler")
	}
	if wire.UID != "" {
		return nil, errors.New("okx: uid is server metadata, not a subscription filter")
	}
	s, err := c.getSession(true)
	if err != nil {
		return nil, err
	}
	key := wire.key()
	entry := &subEntry{arg: wire, handler: handler, onDisconnect: onDisconnect}
	c.subMu.Lock()
	if c.subBusy[key] {
		c.subMu.Unlock()
		return nil, ErrWSConcurrentOperation
	}
	old := *c.handlers.Load()
	if old[key] != nil {
		c.subMu.Unlock()
		return nil, ErrWSAlreadySubscribed
	}
	c.subBusy[key] = true
	next := cloneHandlers(old)
	next[key] = entry
	c.handlers.Store(&next)
	c.subMu.Unlock()
	defer c.releaseSubscription(key)
	if err := c.sendControl(ctx, s, "subscribe", wire, func() { entry.desired.Store(true); entry.active.Store(s) }); err != nil {
		c.removeHandlerIfMatch(key, entry)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.cancel(err)
		}
		return nil, err
	}
	return &WSSubscription{c: c, arg: arg, entry: entry}, nil
}

func cloneHandlers(old map[string]*subEntry) map[string]*subEntry {
	next := make(map[string]*subEntry, len(old)+1)
	for k, v := range old {
		next[k] = v
	}
	return next
}

func (c *WSClient) releaseSubscription(key string) {
	c.subMu.Lock()
	delete(c.subBusy, key)
	c.subMu.Unlock()
}

func (c *WSClient) unsubscribeArg(ctx context.Context, arg WSChannelArg, removeBeforeAck bool, expected *subEntry) error {
	wire := arg.subArg()
	key := wire.key()
	c.subMu.Lock()
	entry := (*c.handlers.Load())[key]
	if entry == nil || (expected != nil && entry != expected) {
		c.subMu.Unlock()
		return nil
	}
	if c.subBusy[key] {
		c.subMu.Unlock()
		return ErrWSConcurrentOperation
	}
	c.subBusy[key] = true
	if removeBeforeAck {
		entry.desired.Store(false)
		entry.active.Store(nil)
		next := cloneHandlers(*c.handlers.Load())
		delete(next, key)
		c.handlers.Store(&next)
	}
	c.subMu.Unlock()
	defer c.releaseSubscription(key)
	s, err := c.getSession(true)
	if err != nil {
		// No active socket can acknowledge cancellation. Remove desired state
		// so recovery cannot resurrect a subscription explicitly closed by its owner.
		c.removeHandlerIfMatch(key, entry)
		if restoring, e := c.getSession(false); e == nil {
			restoring.cancel(ErrWSConnectionLost)
		}
		return nil
	}
	err = c.sendControl(ctx, s, "unsubscribe", wire, func() { entry.desired.Store(false); entry.active.Store(nil) })
	if err != nil {
		if removeBeforeAck || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.cancel(err)
		}
		return err
	}
	c.removeHandlerIfMatch(key, entry)
	return nil
}

func (c *WSClient) removeHandlerIfMatch(key string, entry *subEntry) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	old := *c.handlers.Load()
	if old[key] != entry {
		return
	}
	entry.desired.Store(false)
	entry.active.Store(nil)
	next := cloneHandlers(old)
	delete(next, key)
	c.handlers.Store(&next)
}
