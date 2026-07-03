package okx

import (
	"net/http"
	"strings"
	"time"
)

type Option func(*Client)

func WithCredentials(apiKey, secretKey, passphrase string) Option {
	return func(c *Client) {
		c.credentials = Credentials{
			APIKey:     apiKey,
			SecretKey:  secretKey,
			Passphrase: passphrase,
		}
	}
}

func WithBaseURL(baseURL string) Option {
	return func(c *Client) {
		c.baseURL = strings.TrimRight(baseURL, "/")
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.httpClient = client
		}
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		if c.httpClient == nil {
			c.httpClient = &http.Client{}
		}
		c.httpClient.Timeout = timeout
	}
}

func WithDemoTrading(enabled bool) Option {
	return func(c *Client) {
		c.demoTrading = enabled
	}
}

func WithLogger(logger Logger) Option {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}

func WithRateLimiter(limiter RateLimiter) Option {
	return func(c *Client) {
		c.rateLimiter = limiter
	}
}

func withClock(now func() time.Time) Option {
	return func(c *Client) {
		if now != nil {
			c.now = now
		}
	}
}
