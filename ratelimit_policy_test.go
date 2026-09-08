package okx

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"testing"
	"time"
)

func TestPolicyRESTAndWSShareOrderCosts(t *testing.T) {
	catalog := &InstrumentCatalog{}
	if err := catalog.Update([]Instrument{{InstID: "BTC-USDT", InstIDCode: 42}}); err != nil {
		t.Fatal(err)
	}
	rest := NewClient(WithInstrumentCatalog(catalog), WithRateLimitScope("test-user"))
	rows := []PlaceOrderRequest{{InstID: "BTC-USDT"}, {InstIDCode: 42}, {InstID: "ETH-USDT"}}
	requests, err := rest.restRateRequests(requestSpec{method: "POST", path: "/api/v5/trade/batch-orders", body: rows, auth: true})
	if err != nil {
		t.Fatal(err)
	}
	ws, err := tradeRateRequests(accountRateScope("test-user", "different-key"), "batch-orders", []any{rows[0], rows[1], rows[2]}, catalog)
	if err != nil || !reflect.DeepEqual(requests, ws) {
		t.Fatalf("protocol quota mismatch: %v %v %v", requests, ws, err)
	}
	if len(requests) != 3 || requests[0].Cost != 2 || requests[0].Limit != 300 || requests[2].Cost != 3 || requests[2].Limit != 1000 {
		t.Fatalf("incorrect batch costs: %+v", requests)
	}
	single, err := tradeRateRequests("scope", "batch-orders", []any{rows[0]}, catalog)
	if err != nil || single[0].Limit != 60 {
		t.Fatalf("one-row batch must consume single quota: %+v %v", single, err)
	}
}

func TestPolicyOptionsShareFamilyAndPublicUsesIP(t *testing.T) {
	requests, err := tradeRateRequests("scope", "cancel-batch-orders", []any{CancelOrderRequest{InstID: "BTC-USD-260925-100000-C"}, CancelOrderRequest{InstID: "BTC-USD-261225-120000-P"}}, nil)
	if err != nil || len(requests) != 1 || requests[0].Cost != 2 {
		t.Fatalf("option family quota: %+v %v", requests, err)
	}
	c := NewClient()
	first, _ := c.restRateRequests(requestSpec{method: "GET", path: "/api/v5/market/ticker", query: url.Values{"instId": {"BTC-USDT"}}})
	second, _ := c.restRateRequests(requestSpec{method: "GET", path: "/api/v5/market/ticker", query: url.Values{"instId": {"ETH-USDT"}}})
	if first[0].Key != second[0].Key {
		t.Fatal("public endpoint split by instrument")
	}
}

func TestPolicyDefaultClientsShareSlidingWindow(t *testing.T) {
	first, second := DefaultRateLimiter(), DefaultRateLimiter()
	req := RateLimitRequest{Key: t.Name(), Cost: 2, Limit: 2, Window: time.Second}
	if err := first.WaitRequests(t.Context(), []RateLimitRequest{req}); err != nil {
		t.Fatal(err)
	}
	req.Cost = 1
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := second.WaitRequests(ctx, []RateLimitRequest{req}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared quota was bypassed: %v", err)
	}
}

func TestPolicyCancellationDoesNotConsumeOtherDimensions(t *testing.T) {
	limiter := DefaultRateLimiter()
	limiter.quotas = &quotaState{windows: make(map[string]*quotaWindow)}
	a := RateLimitRequest{Key: "a", Cost: 1, Limit: 1, Window: time.Second}
	b := RateLimitRequest{Key: "b", Cost: 1, Limit: 1, Window: time.Second}
	if err := limiter.WaitRequests(t.Context(), []RateLimitRequest{a}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := limiter.WaitRequests(ctx, []RateLimitRequest{b, a}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := limiter.WaitRequests(t.Context(), []RateLimitRequest{b}); err != nil {
		t.Fatal("partial reservation consumed b", err)
	}
}

func TestInstrumentCatalogVersionAndIdentity(t *testing.T) {
	c := &InstrumentCatalog{}
	inst := Instrument{InstID: "BTC-USDT", InstIDCode: 42, InstType: InstSpot, TickSz: "0.1", LotSz: "0.01", MinSz: "0.01"}
	if err := c.Update([]Instrument{inst}); err != nil {
		t.Fatal(err)
	}
	old, version, err := c.Rules(inst.InstID)
	if err != nil {
		t.Fatal(err)
	}
	inst.TickSz = "0.5"
	if err := c.Update([]Instrument{inst}); err != nil {
		t.Fatal(err)
	}
	updated, newVersion, err := c.Rules(inst.InstID)
	if err != nil || version >= newVersion || old.TickSize.String() != "0.1" || updated.TickSize.String() != "0.5" {
		t.Fatal("rule snapshot was mutated")
	}
	if _, err = c.resolve("ETH-USDT", 42); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("mismatched code accepted")
	}
	if _, err = c.resolve("", 99); !errors.Is(err, ErrInvalidParameter) {
		t.Fatal("unknown code accepted")
	}
	if id, err := c.resolve("", 42); err != nil || id != "BTC-USDT" {
		t.Fatal(id, err)
	}
}
