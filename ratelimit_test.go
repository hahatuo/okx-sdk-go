package okx

import (
	"context"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestRateLimiterBucketsArePerKey(t *testing.T) {
	limiter := NewRateLimiter(rate.Limit(1), 1)
	if err := limiter.Wait(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if err := limiter.Wait(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := limiter.Wait(ctx, "a"); err == nil {
		t.Fatal("expected error after exhausting key a")
	}
}

func TestRateLimiterSetRecreatesLimiter(t *testing.T) {
	limiter := NewRateLimiter(rate.Limit(1), 1)
	if err := limiter.Wait(context.Background(), "orders"); err != nil {
		t.Fatal(err)
	}

	limiter.Set("orders", rate.Inf, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := limiter.Wait(ctx, "orders"); err != nil {
		t.Fatalf("Set should recreate key with new config: %v", err)
	}
}

func TestRateLimiterSetNormalizesBurst(t *testing.T) {
	limiter := NewRateLimiter(rate.Inf, 0)
	limiter.Set("zero-burst", rate.Inf, 0)
	if err := limiter.Wait(context.Background(), "zero-burst"); err != nil {
		t.Fatal(err)
	}
}
