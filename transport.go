package okx

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

const maxResponseBodyBytes = 10 * 1024 * 1024

// requestSpec is the internal description a service method hands to do(). It is
// a value type so it never escapes to the heap on its own.
type requestSpec struct {
	method  string     // canonical upper-case verb ("GET"/"POST")
	path    string     // e.g. "/api/v5/account/balance"
	query   url.Values // GET query params (nil for none)
	body    any        // POST body struct (nil for none)
	auth    bool       // whether to sign the request
	rateKey string     // rate-limiter bucket key ("" to skip)
}

// envelope is the OKX standard response wrapper. Data is left raw and decoded
// into the caller's out only on success.
type envelope struct {
	Code string         `json:"code"`
	Msg  string         `json:"msg"`
	Data jsontext.Value `json:"data"`
}

// bufPool recycles the JSON body buffers used to marshal request bodies.
var bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// do executes one request and, on success (code=="0"), decodes Data into out.
// out may be nil for endpoints whose payload is ignored.
func (c *Client) do(ctx context.Context, spec requestSpec, out any) error {
	if spec.rateKey != "" && c.rl != nil {
		if err := c.rl.Wait(ctx, spec.rateKey); err != nil {
			return err
		}
	}

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	var body io.Reader
	var bodyStr string
	if spec.body != nil {
		if err := json.MarshalWrite(buf, spec.body); err != nil {
			return err
		}
		// buf backs both the reader (Do reads it) and bodyStr (signing). Both
		// are consumed before this call returns, so aliasing is safe.
		body = bytes.NewReader(buf.Bytes())
		bodyStr = buf.String()
	}

	reqPath := spec.path
	if len(spec.query) > 0 {
		reqPath += "?" + spec.query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, spec.method, c.baseURL+reqPath, body)
	if err != nil {
		return err
	}
	if spec.body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if spec.auth {
		if err := c.signRequest(req, spec.method, reqPath, bodyStr); err != nil {
			return err
		}
	}
	if c.demo {
		req.Header.Set("x-simulated-trading", "1")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > maxResponseBodyBytes {
		return ErrResponseTooLarge
	}

	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		// Not a valid OKX envelope: surface the transport failure.
		if !statusIsOK(resp.StatusCode) {
			return &HTTPError{StatusCode: resp.StatusCode, Body: raw}
		}
		return err
	}
	if !statusIsOK(resp.StatusCode) {
		return &HTTPError{StatusCode: resp.StatusCode, Body: raw}
	}
	if env.Code != "0" {
		apiErr := &APIError{Code: env.Code, Msg: env.Msg, Data: env.Data}
		if env.Code == "2" {
			if out != nil && len(env.Data) > 0 {
				if err := json.Unmarshal(env.Data, out); err != nil {
					return err
				}
			}
			return &PartialError{Err: apiErr}
		}
		return wrapAPIError(apiErr)
	}
	if out != nil && len(env.Data) > 0 {
		return json.Unmarshal(env.Data, out)
	}
	return nil
}

// signRequest injects the OK-ACCESS-* headers. Timestamp is ISO-8601 with
// millisecond precision, per the REST spec.
func (c *Client) signRequest(req *http.Request, method, reqPath, body string) error {
	if c.signer == nil {
		return ErrUnauthorized
	}
	ts := c.now().UTC().Format("2006-01-02T15:04:05.000Z")
	req.Header.Set("OK-ACCESS-KEY", c.cred.APIKey)
	req.Header.Set("OK-ACCESS-SIGN", c.signer.Sign(ts, method, reqPath, body))
	req.Header.Set("OK-ACCESS-TIMESTAMP", ts)
	req.Header.Set("OK-ACCESS-PASSPHRASE", c.cred.Passphrase)
	return nil
}

// itoa is a small allocation-free-ish helper kept local to avoid importing
// strconv at every call site in services.
func itoa(i int) string { return strconv.Itoa(i) }

func setIfNotEmpty(q url.Values, key, value string) {
	if value != "" {
		q.Set(key, value)
	}
}

func setBoolIfNotNil(q url.Values, key string, value *bool) {
	if value == nil {
		return
	}
	if *value {
		q.Set(key, "true")
		return
	}
	q.Set(key, "false")
}
