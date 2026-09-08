// Package sign implements OKX request authentication using HMAC-SHA256.
package sign

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"sync"
	"unsafe"
)

const encodedDigestSize = (sha256.Size + 2) / 3 * 4

type pooledMAC struct {
	mac    hash.Hash
	digest [sha256.Size]byte
}

// Signer produces OK-ACCESS-SIGN values. It is safe for concurrent use.
// A Signer must not be copied after first use.
type Signer struct {
	pool sync.Pool
}

// New returns a Signer bound to secret.
// The pool factory retains secret; callers must not mutate its bytes afterwards.
func New(secret []byte) *Signer {
	s := &Signer{}
	s.pool.New = func() any {
		return &pooledMAC{mac: hmac.New(sha256.New, secret)}
	}
	return s
}

// Sign returns base64(HMAC-SHA256(timestamp + method + requestPath + body)).
// method must be upper-case. requestPath includes the encoded query string.
// body must match the bytes sent in the request, or be empty when absent.
func (s *Signer) Sign(timestamp, method, requestPath, body string) string {
	entry := s.pool.Get().(*pooledMAC)
	entry.mac.Reset()
	writeString(entry.mac, timestamp)
	writeString(entry.mac, method)
	writeString(entry.mac, requestPath)
	writeString(entry.mac, body)

	sum := entry.mac.Sum(entry.digest[:0])
	var enc [encodedDigestSize]byte
	base64.StdEncoding.Encode(enc[:], sum)

	// Encode must finish before Put: digest belongs to the pooled entry.
	// enc belongs to this call and is independent of subsequent pool reuse.
	s.pool.Put(entry)
	return string(enc[:])
}

// writeString supplies a read-only view to the standard HMAC implementation,
// which consumes the bytes synchronously without modifying or retaining them.
func writeString(h hash.Hash, s string) {
	if len(s) == 0 {
		return
	}
	_, _ = h.Write(unsafe.Slice(unsafe.StringData(s), len(s)))
}
