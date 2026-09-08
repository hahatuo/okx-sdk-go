package okx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// RateLimitRequest describes one dimension of a request's quota consumption.
// The default sliding-window limiter admits all dimensions atomically.
// Custom implementations should document their reservation/cancellation semantics.
type RateLimitRequest struct {
	Key    string
	Cost   int
	Limit  int
	Window time.Duration
}

type PolicyRateLimiter interface {
	RateLimiter
	WaitRequests(context.Context, []RateLimitRequest) error
}

// WithRateLimitScope joins API keys belonging to the same OKX user ID. The same
// value must be used on REST and WS clients. By default the API key hash is used.
func WithRateLimitScope(scope string) Option     { return func(c *Client) { c.rateScope = scope } }
func WithWSRateLimitScope(scope string) WSOption { return func(c *WSClient) { c.rateScope = scope } }

func accountRateScope(scope, key string) string {
	if scope != "" {
		return "user:" + scope
	}
	hash := sha256.Sum256([]byte(key))
	return "key:" + hex.EncodeToString(hash[:])
}

func waitRateRequests(ctx context.Context, limiter RateLimiter, requests []RateLimitRequest) error {
	if limiter == nil {
		return nil
	}
	if p, ok := limiter.(PolicyRateLimiter); ok {
		return p.WaitRequests(ctx, requests)
	}
	// Legacy adapters receive the canonical cross-protocol key once per order.
	for _, req := range requests {
		for i := 0; i < req.Cost; i++ {
			if err := limiter.Wait(ctx, req.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func tradeRateRequests(scope, op string, args []any, catalog *InstrumentCatalog) ([]RateLimitRequest, error) {
	action := ""
	switch op {
	case "order", "batch-orders":
		action = "place"
	case "cancel-order", "cancel-batch-orders", "batch-cancel-orders":
		action = "cancel"
	case "amend-order", "amend-batch-orders", "batch-amend-orders":
		action = "amend"
	default:
		return nil, nil
	}
	if len(args) == 0 || len(args) > MaxBatchOrderRequests {
		return nil, ErrInvalidParameter
	}
	limit := 60
	bucket := "single"
	if len(args) > 1 {
		limit = 300
		bucket = "batch"
	}
	requests := make([]RateLimitRequest, 0, len(args)+1)
	subaccountCost := 0
	for _, arg := range args {
		var id string
		var code int64
		switch req := arg.(type) {
		case PlaceOrderRequest:
			id, code = req.InstID, req.InstIDCode
		case CancelOrderRequest:
			id, code = req.InstID, req.InstIDCode
		case AmendOrderRequest:
			id, code = req.InstID, req.InstIDCode
		case map[string]any:
			id, _ = req["instId"].(string)
			if req["instIdCode"] != nil {
				return nil, fmt.Errorf("%w: use typed order for instIdCode", ErrInvalidParameter)
			}
		default:
			return nil, fmt.Errorf("%w: use typed trading arguments", ErrInvalidParameter)
		}
		var err error
		id, err = catalog.resolve(id, code)
		if err != nil {
			return nil, err
		}
		// Only authoritative metadata grants an exemption. Unknown instrument
		// types retain the conservative sub-account limit.
		inst, _, _ := catalog.Lookup(id)
		if action != "cancel" && inst.InstType != InstSpot && inst.InstType != InstMargin {
			subaccountCost++
		}
		// OKX option instrument IDs end in strike-C/P and share the family quota.
		id = rateInstrument(id)
		key := scope + ":trade:" + action + ":" + bucket + ":" + id
		found := false
		for i := range requests {
			if requests[i].Key == key {
				requests[i].Cost++
				found = true
				break
			}
		}
		if !found {
			requests = append(requests, RateLimitRequest{Key: key, Cost: 1, Limit: limit, Window: 2 * time.Second})
		}
	}
	if subaccountCost > 0 {
		requests = append(requests, RateLimitRequest{Key: scope + ":trade:subaccount", Cost: subaccountCost, Limit: 1000, Window: 2 * time.Second})
	}
	return requests, nil
}

func (c *Client) restRateRequests(spec requestSpec) ([]RateLimitRequest, error) {
	scope := c.rateScope
	if spec.method == "POST" && strings.HasPrefix(spec.path, "/api/v5/trade/") {
		var args []any
		switch body := spec.body.(type) {
		case []PlaceOrderRequest:
			for _, r := range body {
				args = append(args, r)
			}
		case []CancelOrderRequest:
			for _, r := range body {
				args = append(args, r)
			}
		case []AmendOrderRequest:
			for _, r := range body {
				args = append(args, r)
			}
		default:
			args = []any{body}
		}
		if requests, err := tradeRateRequests(scope, strings.TrimPrefix(spec.path, "/api/v5/trade/"), args, c.catalog); requests != nil || err != nil {
			return requests, err
		}
	}
	if !spec.auth {
		scope = "ip:process"
	}
	limit, window := 20, 2*time.Second
	switch spec.path {
	case "/api/v5/public/time", "/api/v5/account/balance", "/api/v5/account/positions", "/api/v5/trade/fills-history", "/api/v5/market/history-index-candles", "/api/v5/public/funding-rate":
		limit = 10
	case "/api/v5/account/set-fee-type":
		limit = 5
	case "/api/v5/asset/balances", "/api/v5/asset/currencies":
		limit = 6
		window = time.Second
	case "/api/v5/asset/transfer":
		limit = 2
		window = time.Second
	case "/api/v5/market/books", "/api/v5/market/candles":
		limit = 40
	case "/api/v5/market/trades":
		limit = 100
	case "/api/v5/market/platform-24-volume":
		limit = 2
	case "/api/v5/trade/order", "/api/v5/trade/orders-pending":
		limit = 60
	}
	key := scope + ":" + spec.method + ":" + spec.path
	if spec.path == "/api/v5/trade/order" {
		key += ":" + rateInstrument(spec.query.Get("instId"))
	}
	if spec.path == "/api/v5/public/funding-rate" {
		key += ":" + spec.query.Get("instId")
	}
	if spec.path == "/api/v5/public/instruments" {
		key += ":" + spec.query.Get("instType")
	}
	if req, ok := spec.body.(TransferRequest); ok {
		key += ":" + req.Ccy
	}
	return []RateLimitRequest{{Key: key, Cost: 1, Limit: limit, Window: window}}, nil
}

func rateInstrument(id string) string {
	parts := strings.Split(id, "-")
	if len(parts) >= 5 && (parts[len(parts)-1] == "C" || parts[len(parts)-1] == "P") {
		return strings.Join(parts[:2], "-")
	}
	return id
}
