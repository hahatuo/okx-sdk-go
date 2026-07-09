package okx

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

// Sentinel errors let callers branch with errors.Is without matching on OKX
// numeric codes directly. A returned *APIError is wrapped with one of these
// whenever its code maps to a known category (see sentinelForCode).
var (
	ErrUnauthorized        = errors.New("okx: unauthorized")
	ErrRateLimited         = errors.New("okx: rate limited")
	ErrInsufficientBalance = errors.New("okx: insufficient balance")
	ErrInvalidParameter    = errors.New("okx: invalid parameter")
	ErrInvalidOrder        = errors.New("okx: invalid order")
	ErrBelowMinSize        = errors.New("okx: order size below minimum size")
	ErrNotFound            = errors.New("okx: not found")
	ErrBadRequest          = errors.New("okx: bad request")
	ErrServiceUnavailable  = errors.New("okx: service unavailable")
	ErrResponseTooLarge    = errors.New("okx: response body too large")
)

// APIError is a non-zero OKX business error (envelope code != "0").
type APIError struct {
	Code string         // OKX error code, e.g. "51000"
	Msg  string         // Human-readable message from the envelope
	Data jsontext.Value // Per-item error rows, when present (raw)
}

func (e *APIError) Error() string {
	return fmt.Sprintf("okx: api error code=%s msg=%q", e.Code, e.Msg)
}

// DecodeData unmarshals the per-item error rows (e.g. batch-order failures).
func (e *APIError) DecodeData(out any) error {
	if len(e.Data) == 0 {
		return nil
	}
	return json.Unmarshal(e.Data, out)
}

// APIErrorCode extracts the OKX business error code from err.
func APIErrorCode(err error) (string, bool) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr == nil {
		return "", false
	}
	return apiErr.Code, true
}

// DecodeErrorData decodes the OKX business-error data rows carried by err.
func DecodeErrorData(err error, out any) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr == nil {
		return false
	}
	if decodeErr := apiErr.DecodeData(out); decodeErr != nil {
		return false
	}
	return true
}

// OrderAcksFromError extracts per-order ack rows carried by a batch-order
// APIError or PartialError.
func OrderAcksFromError(err error) ([]OrderAck, bool) {
	var acks []OrderAck
	if !DecodeErrorData(err, &acks) || len(acks) == 0 {
		return nil, false
	}
	return acks, true
}

// PartialError is returned for OKX batch envelopes with code "2". The decoded
// data rows are still written into the caller's output value before returning.
type PartialError struct {
	Err *APIError
}

func (e *PartialError) Error() string {
	if e == nil || e.Err == nil {
		return "okx: partial success"
	}
	return "okx: partial success: " + e.Err.Error()
}

func (e *PartialError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ErrorEnvelope and ErrorRow preserve OKX per-row batch errors for callers that
// need to distinguish total failure from partial acceptance.
type ErrorEnvelope struct {
	Code string     `json:"code"`
	Msg  string     `json:"msg"`
	Data []ErrorRow `json:"data"`
}

type ErrorRow struct {
	OrdID   string `json:"ordId"`
	ClOrdID string `json:"clOrdId"`
	ReqID   string `json:"reqId"`
	SCode   string `json:"sCode"`
	SMsg    string `json:"sMsg"`
	Tag     string `json:"tag"`
	TS      string `json:"ts"`
}

func ParseErrorEnvelope(raw []byte) (*ErrorEnvelope, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var env ErrorEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	return &env, nil
}

// ClassifyErrorCode maps an OKX numeric business code to a sentinel error.
func ClassifyErrorCode(code string) error {
	return sentinelForCode(code)
}

// HTTPError is a transport-level failure (non-2xx with no parseable envelope).
type HTTPError struct {
	StatusCode int
	Body       []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("okx: http status %d", e.StatusCode)
}

func required(name string) error {
	return fmt.Errorf("okx: missing required field %s", name)
}

// wrapAPIError attaches a sentinel category to an *APIError when the code is
// recognized, so callers can errors.Is(err, ErrRateLimited) etc.
func wrapAPIError(e *APIError) error {
	if s := sentinelForCode(e.Code); s != nil {
		return fmt.Errorf("%w: %w", s, e)
	}
	return e
}

// sentinelForCode maps OKX error-code ranges to sentinel categories. Ranges
// follow the public error-code reference; extend as needed.
func sentinelForCode(code string) error {
	switch code {
	case "50011", "50061": // request too frequent
		return ErrRateLimited
	case "51008", "58001", "58350":
		return ErrInsufficientBalance
	case "51000":
		return errors.Join(ErrInvalidParameter, ErrNotFound)
	case "50001", "50014", "51001", "51603": // not found
		return ErrNotFound
	case "50003": // service temporarily unavailable
		return ErrServiceUnavailable
	}

	if c, err := strconv.Atoi(code); err == nil {
		switch {
		case (c >= 50100 && c <= 50113) || (c >= 60004 && c <= 60033): // API-key / auth, WS auth
			return ErrUnauthorized
		case (c >= 51000 && c <= 51999) || (c >= 50012 && c <= 50060): // trade parameter errors, request parameter errors
			return ErrInvalidParameter
		case c >= 50000 && c <= 50010:
			return ErrBadRequest
		}
	}
	return nil
}

func statusIsOK(status int) bool {
	return status >= http.StatusOK && status < http.StatusMultipleChoices
}
