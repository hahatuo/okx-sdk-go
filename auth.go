package okx

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultRESTURL is OKX's REST API base URL shown in the current API docs.
	DefaultRESTURL = "https://openapi.okx.com"

	// AlternateRESTURL is OKX's main-site API hostname.
	AlternateRESTURL = "https://www.okx.com"

	WSPublicURL   = "wss://ws.okx.com:8443/ws/v5/public"
	WSPrivateURL  = "wss://ws.okx.com:8443/ws/v5/private"
	WSBusinessURL = "wss://ws.okx.com:8443/ws/v5/business"

	WSDemoPublicURL   = "wss://wspap.okx.com:8443/ws/v5/public"
	WSDemoPrivateURL  = "wss://wspap.okx.com:8443/ws/v5/private"
	WSDemoBusinessURL = "wss://wspap.okx.com:8443/ws/v5/business"
)

type Credentials struct {
	APIKey     string
	SecretKey  string
	Passphrase string
}

func Sign(secretKey, timestamp, method, requestPath, body string) string {
	prehash := timestamp + strings.ToUpper(method) + requestPath + body
	h := hmac.New(sha256.New, []byte(secretKey))
	_, _ = h.Write([]byte(prehash))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func restTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func wsTimestamp(t time.Time) string {
	return strconvFormatUnix(t.UTC().Unix())
}

func strconvFormatUnix(v int64) string {
	return strconv.FormatInt(v, 10)
}
