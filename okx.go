// Package okx is a high-performance Go SDK for the OKX v5 API (REST + WebSocket).
//
// Design constraints (see Agents.md):
//   - JSON via github.com/go-json-experiment/json.
//   - WebSocket via github.com/coder/websocket.
//   - Critical paths (order-book maintenance, request signing) minimise heap
//     allocation and reuse buffers.
//   - Every goroutine is owned by a context.Context; no bare `go` statements,
//     no goroutine leaks.
//   - High-frequency data flow uses direct handler dispatch, not channels.
package okx

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/hahatuo/okx-sdk-go/internal/sign"
)

// Client is the OKX REST client and the entry point to the SDK. It is safe for
// concurrent use. The WebSocket client is created separately via NewWSClient.
type Client struct {
	cred    Credentials
	baseURL string
	http    *http.Client
	timeout time.Duration
	log     Logger
	rl      RateLimiter
	demo    bool
	now     func() time.Time // injectable clock (tests)
	signer  *sign.Signer     // nil when unauthenticated

	// Service handles. Grouped by OKX API section.
	Account *AccountService
	Market  *MarketService
	Public  *PublicService
	Trade   *TradeService
	Asset   *AssetService
}

// NewClient builds a REST client. With no WithCredentials option it can still
// reach public endpoints.
func NewClient(opts ...Option) *Client {
	c := &Client{
		baseURL: DefaultRestURL,
		timeout: 30 * time.Second,
		log:     noopLogger{},
		rl:      DefaultRateLimiter(),
		now:     time.Now,
	}
	for _, o := range opts {
		o(c)
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: c.timeout}
	}
	if c.cred.SecretKey != "" {
		c.signer = sign.New([]byte(c.cred.SecretKey))
	}

	c.Account = &AccountService{c}
	c.Market = &MarketService{c}
	c.Public = &PublicService{c}
	c.Trade = &TradeService{c}
	c.Asset = &AssetService{c}
	return c
}

func executeGet[T any](ctx context.Context, c *Client, path string, q url.Values, rateKey string, auth bool) ([]T, error) {
	var out []T
	err := c.do(ctx, requestSpec{
		method:  "GET",
		path:    path,
		query:   q,
		auth:    auth,
		rateKey: rateKey,
	}, &out)
	return out, err
}
