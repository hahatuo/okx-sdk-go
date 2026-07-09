package okx

import (
	"net/http"
	"strings"
	"time"
)

// Default endpoints. See https://www.okx.com/docs-v5/en/ .
const (
	DefaultRestURL = "https://www.okx.com"

	wsPublicURL   = "wss://ws.okx.com:8443/ws/v5/public"
	wsPrivateURL  = "wss://ws.okx.com:8443/ws/v5/private"
	wsBusinessURL = "wss://ws.okx.com:8443/ws/v5/business"

	wsPublicDemoURL   = "wss://wspap.okx.com:8443/ws/v5/public"
	wsPrivateDemoURL  = "wss://wspap.okx.com:8443/ws/v5/private"
	wsBusinessDemoURL = "wss://wspap.okx.com:8443/ws/v5/business"
)

// Credentials holds OKX API-key material. Zero value is an unauthenticated
// client (public endpoints only).
type Credentials struct {
	APIKey     string
	SecretKey  string
	Passphrase string
}

// Option configures a Client at construction time.
type Option func(*Client)

// WithCredentials enables authenticated (private) REST and WS calls.
func WithCredentials(apiKey, secretKey, passphrase string) Option {
	return func(c *Client) {
		c.cred = Credentials{APIKey: apiKey, SecretKey: secretKey, Passphrase: passphrase}
	}
}

// WithBaseURL overrides the REST base URL (default DefaultRestURL).
func WithBaseURL(u string) Option {
	return func(c *Client) {
		if u != "" {
			c.baseURL = strings.TrimRight(u, "/")
		}
	}
}

// WithHTTPClient supplies a custom *http.Client (connection pooling, proxies…).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithTimeout sets the per-request timeout on the default HTTP client. Ignored
// if WithHTTPClient is also supplied.
func WithTimeout(d time.Duration) Option { return func(c *Client) { c.timeout = d } }

// WithDemoTrading routes requests to OKX demo trading (adds x-simulated-trading).
func WithDemoTrading(enabled bool) Option { return func(c *Client) { c.demo = enabled } }

// WithLogger installs a logger (default: no-op).
func WithLogger(l Logger) Option { return func(c *Client) { c.log = l } }

// WithRateLimiter installs a rate limiter (default: DefaultRateLimiter).
func WithRateLimiter(r RateLimiter) Option { return func(c *Client) { c.rl = r } }

func withClock(now func() time.Time) Option {
	return func(c *Client) {
		if now != nil {
			c.now = now
		}
	}
}
