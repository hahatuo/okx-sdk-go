package okx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type quotaEvent struct {
	at   time.Time
	cost int
}
type quotaWindow struct {
	events []quotaEvent
	used   int
}
type quotaState struct {
	mu      sync.Mutex
	windows map[string]*quotaWindow
}

var processQuotas = &quotaState{windows: make(map[string]*quotaWindow)}

func (m *MultiRateLimiter) WaitRequests(ctx context.Context, requests []RateLimitRequest) error {
	for _, req := range requests {
		if req.Cost <= 0 {
			return fmt.Errorf("%w: quota cost must be positive", ErrInvalidParameter)
		}
	}
	if m.quotas == nil {
		for _, req := range requests {
			if err := m.limiterFor(req.Key).WaitN(ctx, req.Cost); err != nil {
				return err
			}
		}
		return nil
	}
	// A default limiter's Set override is a conservative sliding window with
	// the configured burst and average rate. Configurations must agree across
	// clients that share the same scope.
	m.mu.RLock()
	if len(m.perKey) > 0 {
		copied := make([]RateLimitRequest, 0, len(requests))
		for _, req := range requests {
			if configured, ok := m.perKey[req.Key]; ok {
				if configured.limit == rate.Inf {
					continue
				}
				if configured.limit <= 0 {
					m.mu.RUnlock()
					return fmt.Errorf("%w: quota rate must be positive", ErrInvalidParameter)
				}
				req.Limit = configured.burst
				req.Window = time.Duration(float64(time.Second) * float64(configured.burst) / float64(configured.limit))
			}
			copied = append(copied, req)
		}
		requests = copied
	}
	m.mu.RUnlock()
	for i, req := range requests {
		for j := 0; j < i; j++ {
			if requests[j].Key == req.Key {
				return fmt.Errorf("%w: duplicate quota key", ErrInvalidParameter)
			}
		}
	}
	for _, req := range requests {
		if req.Cost <= 0 || req.Limit < req.Cost || req.Window <= 0 {
			return fmt.Errorf("%w: invalid quota cost or window", ErrInvalidParameter)
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		delay := time.Duration(0)
		m.quotas.mu.Lock()
		now := time.Now()
		for _, req := range requests {
			w := m.quotas.windows[req.Key]
			if w == nil {
				w = &quotaWindow{}
				m.quotas.windows[req.Key] = w
			}
			expired := 0
			for expired < len(w.events) && !w.events[expired].at.Add(req.Window).After(now) {
				w.used -= w.events[expired].cost
				expired++
			}
			if expired > 0 {
				copy(w.events, w.events[expired:])
				w.events = w.events[:len(w.events)-expired]
			}
			needed := w.used + req.Cost - req.Limit
			for _, event := range w.events {
				if needed <= 0 {
					break
				}
				needed -= event.cost
				delay = max(delay, event.at.Add(req.Window).Sub(now))
			}
		}
		if delay <= 0 {
			for _, req := range requests {
				w := m.quotas.windows[req.Key]
				w.events = append(w.events, quotaEvent{now, req.Cost})
				w.used += req.Cost
			}
			m.quotas.mu.Unlock()
			return nil
		}
		m.quotas.mu.Unlock()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
