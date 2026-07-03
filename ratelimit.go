package okx

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type RateLimiter interface {
	Wait(ctx context.Context, key string) error
}

type MultiRateLimiter struct {
	mu           sync.Mutex
	defaults     map[string]rateConfig
	limiters     map[string]*rate.Limiter
	lastAccessed map[string]time.Time
	calls        uint64
}

type rateConfig struct {
	limit rate.Limit
	burst int
}

func NewMultiRateLimiter() *MultiRateLimiter {
	return &MultiRateLimiter{
		defaults:     make(map[string]rateConfig),
		limiters:     make(map[string]*rate.Limiter),
		lastAccessed: make(map[string]time.Time),
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
	delete(m.lastAccessed, key)
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

	m.calls++
	if m.calls%1000 == 0 {
		m.cleanupLocked()
	}

	if l := m.limiters[key]; l != nil {
		m.lastAccessed[key] = time.Now()
		return l
	}
	cfg, ok := m.defaults[key]
	if !ok {
		return nil
	}
	l := rate.NewLimiter(cfg.limit, cfg.burst)
	m.limiters[key] = l
	m.lastAccessed[key] = time.Now()
	return l
}

func (m *MultiRateLimiter) cleanupLocked() {
	now := time.Now()
	for k, t := range m.lastAccessed {
		// Clean up limiters not accessed in the last 15 minutes
		if now.Sub(t) > 15*time.Minute {
			delete(m.limiters, k)
			delete(m.lastAccessed, k)
		}
	}
}
