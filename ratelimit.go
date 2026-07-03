package okx

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

type RateLimiter interface {
	Wait(ctx context.Context, key string) error
}

type MultiRateLimiter struct {
	mu       sync.Mutex
	defaults map[string]rateConfig
	limiters map[string]*rate.Limiter
}

type rateConfig struct {
	limit rate.Limit
	burst int
}

func NewMultiRateLimiter() *MultiRateLimiter {
	return &MultiRateLimiter{
		defaults: make(map[string]rateConfig),
		limiters: make(map[string]*rate.Limiter),
	}
}

func (m *MultiRateLimiter) Set(key string, limit rate.Limit, burst int) {
	if burst < 1 {
		burst = 1
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaults[key] = rateConfig{limit: limit, burst: burst}
	delete(m.limiters, key)
}

func (m *MultiRateLimiter) Wait(ctx context.Context, key string) error {
	if m == nil || key == "" {
		return nil
	}
	limiter := m.get(key)
	if limiter == nil {
		return nil
	}
	return limiter.Wait(ctx)
}

func (m *MultiRateLimiter) get(key string) *rate.Limiter {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l := m.limiters[key]; l != nil {
		return l
	}
	cfg, ok := m.defaults[key]
	if !ok {
		return nil
	}
	l := rate.NewLimiter(cfg.limit, cfg.burst)
	m.limiters[key] = l
	return l
}
