package okx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type RestClient struct {
	*Client

	Account *AccountService
	Trade   *TradeService
	Market  *MarketService
	Public  *PublicDataService
	Asset   *AssetService
}

type Client struct {
	credentials Credentials
	baseURL     string
	httpClient  *http.Client
	logger      Logger
	rateLimiter RateLimiter
	demoTrading bool
	now         func() time.Time
}

func NewRestClient(opts ...Option) *RestClient {
	c := NewClient(opts...)
	return &RestClient{
		Client:  c,
		Account: &AccountService{client: c},
		Trade:   &TradeService{client: c},
		Market:  &MarketService{client: c},
		Public:  &PublicDataService{client: c},
		Asset:   &AssetService{client: c},
	}
}

func NewClient(opts ...Option) *Client {
	c := &Client{
		baseURL:    DefaultRESTURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		logger:     noopLogger{},
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type requestSpec struct {
	method  string
	path    string
	query   url.Values
	body    any
	auth    bool
	rateKey string
}

func (c *Client) do(ctx context.Context, spec requestSpec, out any) error {
	if spec.rateKey != "" && c.rateLimiter != nil {
		if err := c.rateLimiter.Wait(ctx, spec.rateKey); err != nil {
			return err
		}
	}

	bodyBytes, err := encodeBody(spec.body)
	if err != nil {
		return err
	}

	requestPath := spec.path
	if len(spec.query) > 0 {
		requestPath += "?" + spec.query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, spec.method, c.baseURL+requestPath, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("okx: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.demoTrading {
		req.Header.Set("x-simulated-trading", "1")
	}
	if spec.auth {
		c.signRequest(req, spec.method, requestPath, bodyBytes)
	}

	c.logger.Debug("okx rest request", "method", spec.method, "path", requestPath)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("okx: execute request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Debug("okx rest response close failed", "error", closeErr)
		}
	}()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("okx: read response: %w", err)
	}
	if !statusIsOK(resp.StatusCode) {
		return &HTTPError{StatusCode: resp.StatusCode, Body: respBody}
	}

	var envelope struct {
		Code string          `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return fmt.Errorf("okx: decode envelope: %w", err)
	}
	if envelope.Code != "0" {
		parsed, _ := ParseErrorEnvelope(respBody)
		okxErr := &OKXError{
			Code:     envelope.Code,
			Message:  envelope.Msg,
			Data:     append(json.RawMessage(nil), envelope.Data...),
			Raw:      respBody,
			Envelope: parsed,
		}
		if envelope.Code == "2" {
			if out != nil && len(envelope.Data) > 0 && string(envelope.Data) != "null" {
				if err := json.Unmarshal(envelope.Data, out); err != nil {
					return fmt.Errorf("okx: decode partial data: %w", err)
				}
			}
			return &PartialError{Err: okxErr}
		}
		return wrapOKXError(okxErr)
	}
	if out == nil || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return nil
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		return fmt.Errorf("okx: decode data: %w", err)
	}
	return nil
}

func (c *Client) signRequest(req *http.Request, method, requestPath string, body []byte) {
	timestamp := restTimestamp(c.now())
	signature := Sign(c.credentials.SecretKey, timestamp, method, requestPath, string(body))
	req.Header.Set("OK-ACCESS-KEY", c.credentials.APIKey)
	req.Header.Set("OK-ACCESS-SIGN", signature)
	req.Header.Set("OK-ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("OK-ACCESS-PASSPHRASE", c.credentials.Passphrase)
}

func encodeBody(body any) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("okx: encode body: %w", err)
	}
	return bodyBytes, nil
}

func values(pairs ...string) url.Values {
	v := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		setIfNotEmpty(v, pairs[i], pairs[i+1])
	}
	return v
}

func setIfNotEmpty(q url.Values, key, value string) {
	if key == "" || value == "" {
		return
	}
	q.Set(key, value)
}

func setBoolIfNotNil(q url.Values, key string, value *bool) {
	if key == "" || value == nil {
		return
	}
	if *value {
		q.Set(key, "true")
		return
	}
	q.Set(key, "false")
}
