package okx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRESTSignsGETWithQuery(t *testing.T) {
	now := time.Date(2026, 7, 3, 1, 2, 3, 4*int(time.Millisecond), time.UTC)
	var gotPath, gotSign string
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.RequestURI()
		gotSign = r.Header.Get("OK-ACCESS-SIGN")
		return jsonResponse(map[string]any{
			"code": "0",
			"msg":  "",
			"data": []map[string]any{{"totalEq": "1", "details": []map[string]string{}}},
		})
	})}

	client := NewRestClient(
		WithBaseURL("https://local.test"),
		WithHTTPClient(httpClient),
		WithCredentials("key", "secret", "pass"),
		withClock(func() time.Time { return now }),
	)
	_, err := client.Account.Balance(context.Background(), BalanceRequest{Ccy: "BTC"})
	if err != nil {
		t.Fatal(err)
	}
	wantPath := "/api/v5/account/balance?ccy=BTC"
	if gotPath != wantPath {
		t.Fatalf("path = %q, want %q", gotPath, wantPath)
	}
	wantSign := Sign("secret", restTimestamp(now), http.MethodGet, wantPath, "")
	if gotSign != wantSign {
		t.Fatalf("sign = %q, want %q", gotSign, wantSign)
	}
}

func TestRESTErrorSupportsIsAndAs(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(map[string]any{
			"code": "50011",
			"msg":  "Rate limit reached",
			"data": []any{},
		})
	})}

	client := NewRestClient(WithBaseURL("https://local.test"), WithHTTPClient(httpClient))
	_, err := client.Market.Ticker(context.Background(), TickerRequest{InstID: "BTC-USDT"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("errors.Is rate limited = false, err=%v", err)
	}
	var okxErr *OKXError
	if !errors.As(err, &okxErr) {
		t.Fatalf("errors.As OKXError = false, err=%v", err)
	}
	if okxErr.Code != "50011" {
		t.Fatalf("code = %q", okxErr.Code)
	}
}

func TestRESTErrorPreservesEnvelopeRows(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(map[string]any{
			"code": "1",
			"msg":  "All operations failed",
			"data": []map[string]string{{
				"ordId":   "1",
				"clOrdId": "client-1",
				"reqId":   "req-1",
				"sCode":   "51008",
				"sMsg":    "Insufficient balance",
			}},
		})
	})}

	client := NewRestClient(WithBaseURL("https://local.test"), WithHTTPClient(httpClient), WithCredentials("key", "secret", "pass"))
	_, err := client.Trade.PlaceMultipleOrders(context.Background(), []PlaceOrderRequest{{
		InstID:  "BTC-USDT",
		TdMode:  "cash",
		Side:    "buy",
		OrdType: "limit",
		Sz:      "1",
		Px:      "100",
	}})
	if err == nil {
		t.Fatal("expected error")
	}
	var okxErr *OKXError
	if !errors.As(err, &okxErr) {
		t.Fatalf("errors.As OKXError = false, err=%v", err)
	}
	if okxErr.Envelope == nil || len(okxErr.Envelope.Data) != 1 {
		t.Fatalf("missing envelope rows: %#v", okxErr.Envelope)
	}
	row := okxErr.Envelope.Data[0]
	if row.SCode != "51008" || row.ClOrdID != "client-1" || row.ReqID != "req-1" {
		t.Fatalf("unexpected envelope row: %+v", row)
	}
	var rows []ErrorRow
	if err := okxErr.DecodeData(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].OrdID != "1" {
		t.Fatalf("unexpected decoded rows: %+v", rows)
	}
}

func TestRESTBatchPartialSuccessReturnsRowsAndError(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(map[string]any{
			"code": "2",
			"msg":  "Bulk operation partially succeeded.",
			"data": []map[string]string{
				{"ordId": "1", "clOrdId": "client-1", "sCode": "0"},
				{"ordId": "", "clOrdId": "client-2", "sCode": "51008", "sMsg": "Insufficient balance"},
			},
		})
	})}

	client := NewRestClient(WithBaseURL("https://local.test"), WithHTTPClient(httpClient), WithCredentials("key", "secret", "pass"))
	acks, err := client.Trade.PlaceMultipleOrders(context.Background(), []PlaceOrderRequest{{
		InstID:  "BTC-USDT",
		TdMode:  "cash",
		Side:    "buy",
		OrdType: "limit",
		Sz:      "1",
		Px:      "100",
	}})
	if err == nil {
		t.Fatal("expected partial error")
	}
	var partial *PartialError
	if !errors.As(err, &partial) {
		t.Fatalf("errors.As PartialError = false, err=%v", err)
	}
	var okxErr *OKXError
	if !errors.As(err, &okxErr) || okxErr.Code != "2" {
		t.Fatalf("unexpected OKXError: %#v err=%v", okxErr, err)
	}
	if len(acks) != 2 || acks[0].SCode != "0" || acks[1].SCode != "51008" || acks[1].ClOrdID != "client-2" {
		t.Fatalf("unexpected acks: %+v", acks)
	}
	var rows []ErrorRow
	if err := okxErr.DecodeData(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1].SCode != "51008" {
		t.Fatalf("unexpected decoded rows: %+v", rows)
	}
}

func TestIndexComponentsDecodesObjectData(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(map[string]any{
			"code": "0",
			"msg":  "",
			"data": map[string]any{
				"index": "BTC-USDT",
				"last":  "1",
				"ts":    "1597026383085",
				"components": []map[string]string{{
					"exch":   "OKX",
					"symbol": "BTC/USDT",
					"symPx":  "1",
					"cnvPx":  "1",
					"wgt":    "0.25",
				}},
			},
		})
	})}

	client := NewRestClient(WithBaseURL("https://local.test"), WithHTTPClient(httpClient))
	got, err := client.Market.IndexComponents(context.Background(), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != "BTC-USDT" || len(got.Components) != 1 {
		t.Fatalf("unexpected components: %+v", got)
	}
}

func TestTradeFillsHistoryEndpoint(t *testing.T) {
	var gotPath string
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.RequestURI()
		return jsonResponse(map[string]any{
			"code": "0",
			"msg":  "",
			"data": []map[string]string{{
				"instType": "SPOT",
				"instId":   "BTC-USDT",
				"tradeId":  "1",
				"ordId":    "2",
				"fillPx":   "100",
				"fillSz":   "0.1",
			}},
		})
	})}

	client := NewRestClient(WithBaseURL("https://local.test"), WithHTTPClient(httpClient), WithCredentials("key", "secret", "pass"))
	fills, err := client.Trade.FillsHistory(context.Background(), FillsHistoryRequest{
		InstType: "SPOT",
		InstID:   "BTC-USDT",
		Limit:    "100",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/v5/trade/fills-history?instId=BTC-USDT&instType=SPOT&limit=100" {
		t.Fatalf("path = %q", gotPath)
	}
	if len(fills) != 1 || fills[0].TradeID != "1" || fills[0].FillPx != "100" {
		t.Fatalf("unexpected fills: %+v", fills)
	}
}

func TestCandleUnmarshal(t *testing.T) {
	var candle Candle
	if err := json.Unmarshal([]byte(`["1597026383085","3.721","3.743","3.677","3.708","8422410","22698348.04","12698348.04","0"]`), &candle); err != nil {
		t.Fatal(err)
	}
	if candle.VolCcyQuote != "12698348.04" || candle.Confirm != "0" {
		t.Fatalf("unexpected candle: %+v", candle)
	}
}

func TestTradeBatchEndpoints(t *testing.T) {
	tests := []struct {
		name     string
		call     func(context.Context, *RestClient) ([]OrderAck, error)
		wantPath string
		wantBody string
	}{
		{
			name: "place multiple",
			call: func(ctx context.Context, client *RestClient) ([]OrderAck, error) {
				return client.Trade.PlaceMultipleOrders(ctx, []PlaceOrderRequest{{
					InstID:  "BTC-USDT",
					TdMode:  "cash",
					Side:    "buy",
					OrdType: "limit",
					Px:      "1",
					Sz:      "2",
				}})
			},
			wantPath: "/api/v5/trade/batch-orders",
			wantBody: `[{"instId":"BTC-USDT","tdMode":"cash","side":"buy","ordType":"limit","sz":"2","px":"1"}]`,
		},
		{
			name: "cancel multiple",
			call: func(ctx context.Context, client *RestClient) ([]OrderAck, error) {
				return client.Trade.CancelMultipleOrders(ctx, []CancelOrderRequest{{
					InstID: "BTC-USDT",
					OrdID:  "1",
				}})
			},
			wantPath: "/api/v5/trade/cancel-batch-orders",
			wantBody: `[{"instId":"BTC-USDT","ordId":"1"}]`,
		},
		{
			name: "amend multiple",
			call: func(ctx context.Context, client *RestClient) ([]OrderAck, error) {
				return client.Trade.AmendMultipleOrders(ctx, []AmendOrderRequest{{
					InstID: "BTC-USDT",
					OrdID:  "1",
					NewPx:  "3",
				}})
			},
			wantPath: "/api/v5/trade/amend-batch-orders",
			wantBody: `[{"instId":"BTC-USDT","ordId":"1","newPx":"3"}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath string
			var gotBody []byte
			httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotPath = r.URL.RequestURI()
				var err error
				gotBody, err = io.ReadAll(r.Body)
				if err != nil {
					return nil, err
				}
				return jsonResponse(map[string]any{
					"code": "0",
					"msg":  "",
					"data": []map[string]string{{"ordId": "1", "sCode": "0"}},
				})
			})}
			client := NewRestClient(WithBaseURL("https://local.test"), WithHTTPClient(httpClient), WithCredentials("key", "secret", "pass"))
			if _, err := tt.call(context.Background(), client); err != nil {
				t.Fatal(err)
			}
			if gotPath != tt.wantPath {
				t.Fatalf("path = %q, want %q", gotPath, tt.wantPath)
			}
			if !jsonEqual(gotBody, []byte(tt.wantBody)) {
				t.Fatalf("body = %s, want %s", gotBody, tt.wantBody)
			}
		})
	}
}

func TestAccountMaxAvailAndSetLeverageEndpoints(t *testing.T) {
	var requests []string
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		switch r.URL.Path {
		case "/api/v5/account/max-avail-size":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []map[string]string{{"instId": "BTC-USDT", "maxBuy": "1", "maxSell": "2"}},
			})
		case "/api/v5/account/set-leverage":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []map[string]string{{"instId": "BTC-USDT-SWAP", "lever": "2", "mgnMode": "cross"}},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
			return nil, nil
		}
	})}

	client := NewRestClient(WithBaseURL("https://local.test"), WithHTTPClient(httpClient), WithCredentials("key", "secret", "pass"))
	reduceOnly := false
	if _, err := client.Account.MaxAvailSize(context.Background(), MaxAvailSizeRequest{
		InstID:     "BTC-USDT",
		TdMode:     "cash",
		ReduceOnly: &reduceOnly,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Account.SetLeverage(context.Background(), SetLeverageRequest{
		InstID:  "BTC-USDT-SWAP",
		Lever:   "2",
		MgnMode: "cross",
	}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %v", requests)
	}
	if requests[0] != "GET /api/v5/account/max-avail-size?instId=BTC-USDT&reduceOnly=false&tdMode=cash" {
		t.Fatalf("max avail request = %q", requests[0])
	}
	if requests[1] != "POST /api/v5/account/set-leverage" {
		t.Fatalf("set leverage request = %q", requests[1])
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(v any) (*http.Response, error) {
	var b strings.Builder
	if err := json.NewEncoder(&b).Encode(v); err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(b.String())),
	}, nil
}

func jsonEqual(a, b []byte) bool {
	var av any
	var bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	aj, _ := json.Marshal(av)
	bj, _ := json.Marshal(bv)
	return bytes.Equal(aj, bj)
}
