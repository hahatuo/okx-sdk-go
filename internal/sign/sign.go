// Package sign implements OKX request authentication (HMAC-SHA256 over the
// canonical prehash string). It is shared by the REST transport and the
// WebSocket login flow.
//
// The Signer is on the critical path for every authenticated request, so the
// hot method (Sign) performs no heap allocation beyond the returned string:
//   - the keyed hash.Hash is pooled and reset, never reallocated;
//   - prehash parts are streamed into the hash instead of being concatenated;
//   - the digest and its base64 encoding use stack-resident arrays.
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

// Signer produces OK-ACCESS-SIGN values. It is safe for concurrent use.
type Signer struct {
	pool sync.Pool
}

// New returns a Signer bound to the given API secret. The secret is captured by
// the pooled HMAC state; callers must not mutate it afterwards.
func New(secret []byte) *Signer {
	s := &Signer{}
	s.pool.New = func() any { return hmac.New(sha256.New, secret) }
	return s
}

// Sign returns the base64(HMAC-SHA256(prehash)) where
//
//	prehash = timestamp + method + requestPath + body
//
// method must already be upper-case ("GET"/"POST"); the transport passes
// canonical verbs so no per-call uppercasing is needed. body is "" for GET.
func (s *Signer) Sign(timestamp, method, requestPath, body string) string {
	mac := s.pool.Get().(hash.Hash)
	mac.Reset()
	writeString(mac, timestamp)
	writeString(mac, method)
	writeString(mac, requestPath)
	writeString(mac, body)

	var digest [sha256.Size]byte
	sum := mac.Sum(digest[:0])
	s.pool.Put(mac)

	var enc [encodedDigestSize]byte
	base64.StdEncoding.Encode(enc[:], sum)
	return string(enc[:])
}

// writeString feeds s into h without the []byte(s) copy. The hash consumes the
// bytes synchronously and never mutates or retains the input slice.
func writeString(h hash.Hash, s string) {
	if len(s) == 0 {
		return
	}
	_, _ = h.Write(unsafe.Slice(unsafe.StringData(s), len(s))) // #nosec G103 -- audited read-only string-to-byte view for synchronous hash.Hash.Write.
}
