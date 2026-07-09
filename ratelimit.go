package okx

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

// RateLimiter throttles outbound requests. Keys are per-endpoint buckets (see
// the rateKey passed by each service method), letting independent endpoints
// consume their quotas without cross-blocking.
type RateLimiter interface {
	Wait(ctx context.Context, key string) error
}

// bucket is the (limit, burst) config for one key.
type bucket struct {
	limit rate.Limit
	burst int
}

// MultiRateLimiter is a per-key token-bucket limiter. Limiters are created
// lazily on first use of a key. Reads of an already-created limiter take the
// read lock only; the write lock is held solely on first insertion.
type MultiRateLimiter struct {
	mu       sync.RWMutex
	def      bucket
	perKey   map[string]bucket
	limiters map[string]*rate.Limiter
}

// NewRateLimiter builds a limiter with a default per-key rate. OKX's baseline
// public quota is ~20 req / 2s; tune per key via Set.
func NewRateLimiter(defLimit rate.Limit, defBurst int) *MultiRateLimiter {
	return &MultiRateLimiter{
		def:      bucket{limit: defLimit, burst: normalizeBurst(defBurst)},
		perKey:   make(map[string]bucket),
		limiters: make(map[string]*rate.Limiter),
	}
}

// DefaultRateLimiter is the limiter installed when WithRateLimiter is not given.
func DefaultRateLimiter() *MultiRateLimiter {
	return NewRateLimiter(10, 20) // 10 req/s sustained, burst 20
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
