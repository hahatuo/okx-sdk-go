package okx

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	json "github.com/go-json-experiment/json"
)

type quotaRecorder struct {
	mu    sync.Mutex
	calls [][]RateLimitRequest
	err   error
}

func (r *quotaRecorder) Wait(context.Context, string) error {
	return errors.New("unexpected legacy limiter call")
}
func (r *quotaRecorder) WaitRequests(_ context.Context, requests []RateLimitRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, append([]RateLimitRequest(nil), requests...))
	return r.err
}
func (r *quotaRecorder) take() [][]RateLimitRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := r.calls
	r.calls = nil
	return calls
}

func TestWSBatchMethodsShareRESTQuotas(t *testing.T) {
	for _, op := range []string{"batch-cancel-orders", "batch-amend-orders"} {
		for _, count := range []int{1, 2} {
			t.Run(op+"/"+itoa(count), func(t *testing.T) {
				sent := make(chan string, 1)
				server := regressionWSServer(t, func(ctx context.Context, conn *websocket.Conn) {
					req, err := readRegressionRequest(ctx, conn)
					if err != nil {
						return
					}
					sent <- req.Op
					response, _ := json.Marshal(map[string]any{"id": req.ID, "op": req.Op, "code": "0", "data": []OrderAck{{OrdID: "1", SCode: "0"}}})
					if err = conn.Write(ctx, websocket.MessageText, response); err != nil {
						return
					}
					_, _, _ = conn.Read(ctx)
				})
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				recorder := &quotaRecorder{}
				catalog := testTradingCatalog(t)
				client := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSRateLimiter(recorder), WithWSRateLimitScope(t.Name()), WithWSInstrumentCatalog(catalog))
				if err := client.Connect(ctx); err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				recorder.take() // Connection admission has its own quota.
				spec := requestSpec{method: "POST", auth: true}
				var err error
				if op == "batch-cancel-orders" {
					req := make([]CancelOrderRequest, count)
					for i := range req {
						req[i] = CancelOrderRequest{InstID: "BTC-USDT", OrdID: itoa(i + 1)}
					}
					spec.path, spec.body = "/api/v5/trade/cancel-batch-orders", req
					_, err = client.CancelMultipleOrders(ctx, req)
				} else {
					req := make([]AmendOrderRequest, count)
					for i := range req {
						req[i] = AmendOrderRequest{InstID: "BTC-USDT", OrdID: itoa(i + 1), NewSz: "1"}
					}
					spec.path, spec.body = "/api/v5/trade/amend-batch-orders", req
					_, err = client.AmendMultipleOrders(ctx, req)
				}
				if err != nil {
					t.Fatal(err)
				}
				select {
				case actual := <-sent:
					if actual != op {
						t.Fatalf("wire op=%s, want %s", actual, op)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				rest := NewClient(WithRateLimitScope(t.Name()), WithInstrumentCatalog(catalog))
				expected, err := rest.restRateRequests(spec)
				if err != nil {
					t.Fatal(err)
				}
				calls := recorder.take()
				if len(calls) != 1 || !reflect.DeepEqual(calls[0], expected) {
					t.Fatalf("WS quotas=%+v, REST quotas=%+v", calls, expected)
				}
				limit := 60
				if count > 1 {
					limit = 300
				}
				if calls[0][0].Cost != count || calls[0][0].Limit != limit {
					t.Fatalf("incorrect per-order consumption: %+v", calls[0])
				}
				dimensions := 1 // SPOT is exempt from the sub-account quota.
				if len(calls[0]) != dimensions {
					t.Fatalf("missing quota dimension: %+v", calls[0])
				}
			})
		}
	}
}

func TestRESTQuotaAdmissionAndInternalBypass(t *testing.T) {
	recorder := &quotaRecorder{err: ErrRateLimited}
	networkCalls := 0
	c := NewClient(WithRateLimiter(recorder), WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		networkCalls++
		return jsonResponse(map[string]any{"code": "0", "data": []any{}})
	})}))
	if err := c.do(t.Context(), requestSpec{method: "GET", path: "/internal"}, nil); err != nil {
		t.Fatal(err)
	}
	if len(recorder.take()) != 0 || networkCalls != 1 {
		t.Fatal("internal bypass changed")
	}
	if _, err := c.Market.Ticker(t.Context(), TickerRequest{InstID: "BTC-USDT"}); !errors.Is(err, ErrRateLimited) {
		t.Fatal("public endpoint bypassed policy", err)
	}
	calls := recorder.take()
	if networkCalls != 1 || len(calls) != 1 || calls[0][0].Key != "ip:process:GET:/api/v5/market/ticker" {
		t.Fatalf("quota admission failed: calls=%+v network=%d", calls, networkCalls)
	}
}
