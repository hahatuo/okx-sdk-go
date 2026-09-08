package sign

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

func reference(secret, ts, method, path, body string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(ts + method + path + body))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func TestSignMatchesReference(t *testing.T) {
	secret := "topsecret"
	s := New([]byte(secret))
	cases := []struct{ ts, method, path, body string }{
		{"2020-12-08T09:08:57.715Z", "GET", "/api/v5/account/balance", ""},
		{"1700000000", "GET", "/users/self/verify", ""},
		{"2020-12-08T09:08:57.715Z", "POST", "/api/v5/trade/order", `{"instId":"BTC-USDT"}`},
	}
	for _, c := range cases {
		got := s.Sign(c.ts, c.method, c.path, c.body)
		want := reference(secret, c.ts, c.method, c.path, c.body)
		if got != want {
			t.Errorf("Sign(%q,%q,%q,%q)=%q want %q", c.ts, c.method, c.path, c.body, got, want)
		}
	}
}

func TestSignConcurrent(t *testing.T) {
	s := New([]byte("k"))
	want := reference("k", "1", "GET", "/p", "")
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func() {
			for i := 0; i < 1000; i++ {
				if s.Sign("1", "GET", "/p", "") != want {
					t.Error("mismatch under concurrency")
					break
				}
			}
			done <- struct{}{}
		}()
	}
	for g := 0; g < 8; g++ {
		<-done
	}
}

func BenchmarkSign(b *testing.B) {
	s := New([]byte("topsecret"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Sign("2020-12-08T09:08:57.715Z", "POST", "/api/v5/trade/order", `{"instId":"BTC-USDT"}`)
	}
}
