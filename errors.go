package okx

import (
	"errors"
	"fmt"
	"net/http"
)

var (
	ErrUnauthorized       = errors.New("okx: unauthorized")
	ErrRateLimited        = errors.New("okx: rate limited")
	ErrInvalidParameter   = errors.New("okx: invalid parameter")
	ErrNotFound           = errors.New("okx: resource not found")
	ErrBadRequest         = errors.New("okx: bad request")
	ErrServiceUnavailable = errors.New("okx: service unavailable")
)

type OKXError struct {
	Code    string
	Message string
	Raw     []byte
}

func (e *OKXError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("okx: api error code=%s msg=%s", e.Code, e.Message)
}

type HTTPError struct {
	StatusCode int
	Body       []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("okx: http status=%d body=%s", e.StatusCode, string(e.Body))
}

func sentinelForCode(code string) error {
	switch code {
	case "50011":
		return ErrRateLimited
	case "50014":
		return ErrNotFound
	case "50012", "50013", "60013", "60027", "60033":
		return ErrInvalidParameter
	case "50100", "50101", "50102", "50103", "50104", "50105", "50106", "50107", "50108", "50109", "50110", "50111", "50112", "50113", "60004", "60005", "60006", "60007", "60009", "60024", "60032":
		return ErrUnauthorized
	case "50003":
		return ErrServiceUnavailable
	case "50000", "50001", "50002", "50004", "50005", "50006", "50007", "50008", "50009", "50010", "60012":
		return ErrBadRequest
	default:
		return nil
	}
}

func wrapOKXError(err *OKXError) error {
	if err == nil {
		return nil
	}
	if sentinel := sentinelForCode(err.Code); sentinel != nil {
		return fmt.Errorf("%w: %w", sentinel, err)
	}
	return err
}

func statusIsOK(status int) bool {
	return status >= http.StatusOK && status < http.StatusMultipleChoices
}
