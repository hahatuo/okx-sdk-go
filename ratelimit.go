package okx

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// RateLimiter throttles outbound requests using canonical keys constructed by
// the endpoint policy. REST and WS trading operations share the same keys.
type RateLimiter interface {
	Wait(ctx context.Context, key string) error
}

// bucket is the (limit, burst) config for one key.
type bucket struct {
	limit rate.Limit
	burst int
}

// MultiRateLimiter supports custom token buckets and default shared sliding
// windows. Custom token buckets are created lazily on first use of a key.
type MultiRateLimiter struct {
	quotas   *quotaState
	mu       sync.RWMutex
	def      bucket
	perKey   map[string]bucket
	limiters map[string]*rate.Limiter
}

// NewRateLimiter builds an independent custom token-bucket limiter.
// This does not install the default OKX endpoint policies.
func NewRateLimiter(defLimit rate.Limit, defBurst int) *MultiRateLimiter {
	return &MultiRateLimiter{
		def:      bucket{limit: defLimit, burst: normalizeBurst(defBurst)},
		perKey:   make(map[string]bucket),
		limiters: make(map[string]*rate.Limiter),
	}
}

// DefaultRateLimiter uses documented sliding-window policies shared in this
// process. NewRateLimiter provides independently configured token buckets.
func DefaultRateLimiter() *MultiRateLimiter {
	m := NewRateLimiter(10, 20)
	m.quotas = processQuotas
	return m
}

// Set overrides the (limit, burst) for a specific key. Call before first use.
func (m *MultiRateLimiter) Set(key string, limit rate.Limit, burst int) {
	m.mu.Lock()
	m.perKey[key] = bucket{limit: limit, burst: normalizeBurst(burst)}
	delete(m.limiters, key) // force re-creation with new config
	m.mu.Unlock()
}

// Wait blocks until a token is available for key or ctx is done.
func (m *MultiRateLimiter) Wait(ctx context.Context, key string) error {
	return m.limiterFor(key).Wait(ctx)
}

func (m *MultiRateLimiter) limiterFor(key string) *rate.Limiter {
	m.mu.RLock()
	l := m.limiters[key]
	m.mu.RUnlock()
	if l != nil {
		return l
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if l = m.limiters[key]; l != nil { // re-check under write lock
		return l
	}
	b, ok := m.perKey[key]
	if !ok {
		b = m.def
	}
	l = rate.NewLimiter(b.limit, b.burst)
	m.limiters[key] = l
	return l
}

func normalizeBurst(burst int) int {
	if burst < 1 {
		return 1
	}
	return burst
}
