package okx

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	json "github.com/go-json-experiment/json"
)

func TestRESTSignsGETWithQuery(t *testing.T) {
	now := time.Date(2026, 7, 3, 1, 2, 3, 4*int(time.Millisecond), time.UTC)
	secret := "secret"
	var gotPath, gotSign string

	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.RequestURI()
		gotSign = r.Header.Get("OK-ACCESS-SIGN")
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("OK-ACCESS-KEY") != "key" {
			t.Fatal("missing access key")
		}
		if r.Header.Get("OK-ACCESS-PASSPHRASE") != "pass" {
			t.Fatal("missing passphrase")
		}
		if r.Header.Get("OK-ACCESS-TIMESTAMP") != "2026-07-03T01:02:03.004Z" {
			t.Fatalf("timestamp = %s", r.Header.Get("OK-ACCESS-TIMESTAMP"))
		}
		return jsonResponse(map[string]any{
			"code": "0",
			"msg":  "",
			"data": []map[string]any{{"totalEq": "1", "uTime": "2", "details": []any{}}},
		})
	})}

	client := NewClient(
		WithBaseURL("https://unit.test/"),
		WithHTTPClient(httpClient),
		WithCredentials("key", secret, "pass"),
		WithRateLimiter(nil),
		withClock(func() time.Time { return now }),
	)
	balances, err := client.Account.Balance(context.Background(), "BTC")
	if err != nil {
		t.Fatal(err)
	}
	if len(balances) != 1 || balances[0].TotalEq != "1" {
		t.Fatalf("unexpected balances: %+v", balances)
	}
	wantPath := "/api/v5/account/balance?ccy=BTC"
	if gotPath != wantPath {
		t.Fatalf("path = %q, want %q", gotPath, wantPath)
	}
	wantSign := referenceSign(secret, "2026-07-03T01:02:03.004Z", http.MethodGet, wantPath, "")
	if gotSign != wantSign {
		t.Fatalf("sign = %q, want %q", gotSign, wantSign)
	}
}

func TestRESTSignsPOSTWithExactJSONBody(t *testing.T) {
	now := time.Date(2026, 7, 3, 1, 2, 3, 4*int(time.Millisecond), time.UTC)
	secret := "secret"
	var gotBody, gotPath, gotSign string

	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		gotBody = string(raw)
		gotPath = r.URL.RequestURI()
		gotSign = r.Header.Get("OK-ACCESS-SIGN")
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("content-type = %q", r.Header.Get("Content-Type"))
		}
		return jsonResponse(map[string]any{
			"code": "0",
			"msg":  "",
			"data": []map[string]string{{"ordId": "1", "clOrdId": "client-1", "sCode": "0", "sMsg": ""}},
		})
	})}

	client := NewClient(
		WithBaseURL("https://unit.test"),
		WithHTTPClient(httpClient),
		WithCredentials("key", secret, "pass"),
		WithRateLimiter(nil),
		withClock(func() time.Time { return now }),
	)
	ack, err := client.Trade.PlaceOrder(context.Background(), PlaceOrderRequest{
		InstID:  "BTC-USDT",
		TdMode:  TdCash,
		ClOrdID: "client-1",
		Side:    Buy,
		OrdType: OrdLimit,
		Sz:      "1",
		Px:      "100",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.OrdID != "1" || ack.ClOrdID != "client-1" {
		t.Fatalf("unexpected ack: %+v", ack)
	}
	wantBody := `{"instId":"BTC-USDT","tdMode":"cash","clOrdId":"client-1","side":"buy","ordType":"limit","sz":"1","px":"100"}`
	if gotBody != wantBody {
		t.Fatalf("body = %s, want %s", gotBody, wantBody)
	}
	wantPath := "/api/v5/trade/order"
	if gotPath != wantPath {
		t.Fatalf("path = %q, want %q", gotPath, wantPath)
	}
	wantSign := referenceSign(secret, "2026-07-03T01:02:03.004Z", http.MethodPost, wantPath, wantBody)
	if gotSign != wantSign {
		t.Fatalf("sign = %q, want %q", gotSign, wantSign)
	}
}

func TestAssetTransferEndpoint(t *testing.T) {
	now := time.Date(2026, 7, 3, 1, 2, 3, 4*int(time.Millisecond), time.UTC)
	secret := "secret"
	var gotBody, gotPath, gotSign string

	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		gotBody = string(raw)
		gotPath = r.URL.RequestURI()
		gotSign = r.Header.Get("OK-ACCESS-SIGN")
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("content-type = %q", r.Header.Get("Content-Type"))
		}
		return jsonResponse(map[string]any{
			"code": "0",
			"msg":  "",
			"data": []map[string]string{{"transId": "123", "ccy": "DOGE", "amt": "5", "from": "6", "to": "18"}},
		})
	})}

	client := NewClient(
		WithBaseURL("https://unit.test"),
		WithHTTPClient(httpClient),
		WithCredentials("key", secret, "pass"),
		WithRateLimiter(nil),
		withClock(func() time.Time { return now }),
	)
	transfers, err := client.Asset.Transfer(context.Background(), TransferRequest{
		Ccy:  "DOGE",
		Amt:  "5",
		From: AccountFunding,
		To:   AccountTrading,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(transfers) != 1 || transfers[0].TransID != "123" || transfers[0].Ccy != "DOGE" {
		t.Fatalf("unexpected transfer response: %+v", transfers)
	}
	wantPath := "/api/v5/asset/transfer"
	if gotPath != wantPath {
		t.Fatalf("path = %q, want %q", gotPath, wantPath)
	}
	wantBody := `{"ccy":"DOGE","amt":"5","from":"6","to":"18"}`
	if gotBody != wantBody {
		t.Fatalf("body = %s, want %s", gotBody, wantBody)
	}
	wantSign := referenceSign(secret, "2026-07-03T01:02:03.004Z", http.MethodPost, wantPath, wantBody)
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
	client := NewClient(WithBaseURL("https://unit.test"), WithHTTPClient(httpClient), WithRateLimiter(nil))

	_, err := client.Market.Ticker(context.Background(), TickerRequest{InstID: "BTC-USDT"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("errors.Is rate limited = false, err=%v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("errors.As APIError = false, err=%v", err)
	}
	if apiErr.Code != "50011" {
		t.Fatalf("code = %q", apiErr.Code)
	}
}

func TestErrorHelpersClassifyTradingCodes(t *testing.T) {
	tests := []struct {
		code string
		want error
	}{
		{code: "51008", want: ErrInsufficientBalance},
		{code: "58001", want: ErrInsufficientBalance},
		{code: "50011", want: ErrRateLimited},
		{code: "50001", want: ErrNotFound},
		{code: "51001", want: ErrNotFound},
		{code: "50000", want: ErrBadRequest},
		{code: "50100", want: ErrUnauthorized},
		{code: "60033", want: ErrUnauthorized},
		{code: "51002", want: ErrInvalidParameter},
	}
	for _, tt := range tests {
		if got := ClassifyErrorCode(tt.code); !errors.Is(got, tt.want) {
			t.Fatalf("ClassifyErrorCode(%q) = %v, want %v", tt.code, got, tt.want)
		}
	}

	err := ClassifyErrorCode("51000")
	if !errors.Is(err, ErrInvalidParameter) || !errors.Is(err, ErrNotFound) {
		t.Fatalf("ClassifyErrorCode(51000) = %v, want invalid parameter and not found", err)
	}
	if got := ClassifyErrorCode("5001"); got != nil {
		t.Fatalf("ClassifyErrorCode(5001) = %v, want nil", got)
	}
	if got := ClassifyErrorCode("not-a-code"); got != nil {
		t.Fatalf("ClassifyErrorCode(not-a-code) = %v, want nil", got)
	}
}

func TestRESTResponseBodyLimit(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(strings.Repeat("x", maxResponseBodyBytes+1))),
		}, nil
	})}
	client := NewClient(WithBaseURL("https://unit.test"), WithHTTPClient(httpClient), WithRateLimiter(nil))

	_, err := client.Market.Ticker(context.Background(), TickerRequest{InstID: "BTC-USDT"})
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("Ticker error = %v, want ErrResponseTooLarge", err)
	}
}

func TestOrderAckHelpers(t *testing.T) {
	ok := OrderAck{SCode: "0"}
	if !ok.OK() || ok.APIError() != nil {
		t.Fatalf("accepted ack helpers failed: %+v", ok)
	}

	rejected := OrderAck{SCode: "51008", SMsg: "Insufficient balance"}
	if rejected.OK() {
		t.Fatalf("rejected ack reported OK: %+v", rejected)
	}
	if err := rejected.APIError(); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("ack APIError = %v, want insufficient balance", err)
	}
}

func TestRESTBatchPartialSuccessReturnsRowsAndError(t *testing.T) {
	var gotPath string
	var gotBody []byte
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.RequestURI()
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		return jsonResponse(map[string]any{
			"code": "2",
			"msg":  "Bulk operation partially succeeded.",
			"data": []map[string]string{
				{"ordId": "1", "clOrdId": "client-1", "sCode": "0"},
				{"ordId": "", "clOrdId": "client-2", "sCode": "51008", "sMsg": "Insufficient balance"},
			},
		})
	})}
	client := NewClient(
		WithBaseURL("https://unit.test"),
		WithHTTPClient(httpClient),
		WithCredentials("key", "secret", "pass"),
		WithRateLimiter(nil),
	)

	acks, err := client.Trade.PlaceMultipleOrders(context.Background(), []PlaceOrderRequest{{
		InstID:  "BTC-USDT",
		TdMode:  TdCash,
		Side:    Buy,
		OrdType: OrdLimit,
		Sz:      "1",
		Px:      "100",
		ClOrdID: "client-1",
	}})
	if err == nil {
		t.Fatal("expected partial error")
	}
	var partial *PartialError
	if !errors.As(err, &partial) {
		t.Fatalf("errors.As PartialError = false, err=%v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "2" {
		t.Fatalf("unexpected APIError: %#v err=%v", apiErr, err)
	}
	if len(acks) != 2 || acks[0].SCode != "0" || acks[1].SCode != "51008" {
		t.Fatalf("unexpected acks: %+v", acks)
	}
	errAcks, ok := OrderAcksFromError(err)
	if !ok || len(errAcks) != 2 || errAcks[1].ClOrdID != "client-2" {
		t.Fatalf("OrderAcksFromError = %+v ok=%v", errAcks, ok)
	}
	code, ok := APIErrorCode(err)
	if !ok || code != "2" {
		t.Fatalf("APIErrorCode = %q ok=%v", code, ok)
	}
	var rows []ErrorRow
	if err := apiErr.DecodeData(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1].ClOrdID != "client-2" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if gotPath != "/api/v5/trade/batch-orders" {
		t.Fatalf("path = %q", gotPath)
	}
	if !bytes.Contains(gotBody, []byte(`"instId":"BTC-USDT"`)) {
		t.Fatalf("body missing instId: %s", gotBody)
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
	client := NewClient(
		WithBaseURL("https://unit.test"),
		WithHTTPClient(httpClient),
		WithCredentials("key", "secret", "pass"),
		WithRateLimiter(nil),
	)

	_, err := client.Trade.PlaceMultipleOrders(context.Background(), []PlaceOrderRequest{{
		InstID:  "BTC-USDT",
		TdMode:  TdCash,
		Side:    Buy,
		OrdType: OrdLimit,
		Sz:      "1",
		Px:      "100",
	}})
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("errors.As APIError = false, err=%v", err)
	}
	var rows []ErrorRow
	if err := apiErr.DecodeData(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].SCode != "51008" || rows[0].ReqID != "req-1" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestPublicSystemTimeAndInstrumentsSeriesID(t *testing.T) {
	var paths []string
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.RequestURI())
		switch r.URL.Path {
		case "/api/v5/public/instruments":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []map[string]any{{
					"instId":     "BTC-USDT",
					"instIdCode": 1000000000,
					"instType":   "SPOT",
					"tickSz":     "0.1",
					"lotSz":      "0.00001",
					"minSz":      "0.00001",
					"state":      "live",
				}, {
					"instId":     "NEW-USDT",
					"instIdCode": nil,
					"instType":   "SPOT",
					"tickSz":     "0.01",
					"lotSz":      "0.01",
					"minSz":      "0.01",
					"state":      "preopen",
				}},
			})
		case "/api/v5/public/time":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []map[string]string{{"ts": "1597026383085"}},
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}
	client := NewClient(WithBaseURL("https://unit.test"), WithHTTPClient(httpClient), WithRateLimiter(nil))

	instruments, err := client.Public.Instruments(context.Background(), InstrumentsRequest{
		InstType: InstSpot,
		SeriesID: "series-1",
		InstID:   "BTC-USDT",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(instruments) != 2 ||
		instruments[0].InstID != "BTC-USDT" ||
		instruments[0].InstIDCode != 1000000000 ||
		instruments[1].InstID != "NEW-USDT" ||
		instruments[1].InstIDCode != 0 {
		t.Fatalf("unexpected instruments: %+v", instruments)
	}
	times, err := client.Public.SystemTime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(times) != 1 || times[0].TS != "1597026383085" {
		t.Fatalf("unexpected time: %+v", times)
	}
	want := []string{
		"/api/v5/public/instruments?instId=BTC-USDT&instType=SPOT&seriesId=series-1",
		"/api/v5/public/time",
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("path[%d] = %q, want %q", i, paths[i], want[i])
		}
	}
}

func TestMarketTradesEndpoints(t *testing.T) {
	var paths []string
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.RequestURI())
		switch r.URL.Path {
		case "/api/v5/market/trades":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []map[string]any{{
					"instId":  "BTC-USDT",
					"tradeId": "242720720",
					"px":      "29963.2",
					"sz":      "0.00001",
					"side":    "sell",
					"source":  "0",
					"ts":      "1654161646974",
				}},
			})
		case "/api/v5/market/history-trades":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []map[string]any{{
					"instId":  "BTC-USDT",
					"tradeId": "242720719",
					"px":      "29964.1",
					"sz":      "0.00002",
					"side":    "buy",
					"source":  "0",
					"count":   2,
					"ts":      "1654161641568",
				}},
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}
	client := NewClient(WithBaseURL("https://unit.test"), WithHTTPClient(httpClient), WithRateLimiter(nil))

	trades, err := client.Market.Trades(context.Background(), TradesRequest{InstID: "BTC-USDT", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 || trades[0].TradeID != "242720720" || trades[0].Px != "29963.2" || trades[0].Side != Sell {
		t.Fatalf("unexpected trades: %+v", trades)
	}
	history, err := client.Market.HistoryTrades(context.Background(), HistoryTradesRequest{
		InstID: "BTC-USDT",
		Type:   "1",
		After:  "242720720",
		Before: "242720718",
		Limit:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].TradeID != "242720719" || history[0].Count.String() != "2" {
		t.Fatalf("unexpected history trades: %+v", history)
	}
	want := []string{
		"/api/v5/market/trades?instId=BTC-USDT&limit=2",
		"/api/v5/market/history-trades?after=242720720&before=242720718&instId=BTC-USDT&limit=1&type=1",
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("path[%d] = %q, want %q", i, paths[i], want[i])
		}
	}
}

func TestMarketCandlesEndpoints(t *testing.T) {
	var paths []string
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.RequestURI())
		switch r.URL.Path {
		case "/api/v5/market/candles",
			"/api/v5/market/history-candles",
			"/api/v5/market/index-candles",
			"/api/v5/market/history-index-candles",
			"/api/v5/market/mark-price-candles",
			"/api/v5/market/history-mark-price-candles":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []any{[]string{
					"1597026383085",
					"1",
					"2",
					"0.5",
					"1.5",
					"10",
					"11",
					"12",
					"1",
				}},
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}
	client := NewClient(WithBaseURL("https://unit.test"), WithHTTPClient(httpClient), WithRateLimiter(nil))

	tests := []struct {
		name     string
		wantPath string
		call     func(context.Context, *Client) ([]Candle, error)
	}{
		{
			name:     "recent",
			wantPath: "/api/v5/market/candles?bar=1m&instId=BTC-USDT&limit=2",
			call: func(ctx context.Context, client *Client) ([]Candle, error) {
				return client.Market.Candles(ctx, "BTC-USDT", "1m", 2)
			},
		},
		{
			name:     "history",
			wantPath: "/api/v5/market/history-candles?after=3&bar=5m&before=1&instId=BTC-USDT&limit=1",
			call: func(ctx context.Context, client *Client) ([]Candle, error) {
				return client.Market.HistoryCandles(ctx, CandlesRequest{
					InstID: "BTC-USDT",
					Bar:    "5m",
					After:  "3",
					Before: "1",
					Limit:  1,
				})
			},
		},
		{
			name:     "index",
			wantPath: "/api/v5/market/index-candles?bar=1H&instId=BTC-USD&limit=1",
			call: func(ctx context.Context, client *Client) ([]Candle, error) {
				return client.Market.IndexCandles(ctx, CandlesRequest{InstID: "BTC-USD", Bar: "1H", Limit: 1})
			},
		},
		{
			name:     "history index",
			wantPath: "/api/v5/market/history-index-candles?after=2&instId=BTC-USD&limit=1",
			call: func(ctx context.Context, client *Client) ([]Candle, error) {
				return client.Market.HistoryIndexCandles(ctx, CandlesRequest{InstID: "BTC-USD", After: "2", Limit: 1})
			},
		},
		{
			name:     "mark price",
			wantPath: "/api/v5/market/mark-price-candles?bar=1H&instId=BTC-USD-SWAP&limit=1",
			call: func(ctx context.Context, client *Client) ([]Candle, error) {
				return client.Market.MarkPriceCandles(ctx, CandlesRequest{InstID: "BTC-USD-SWAP", Bar: "1H", Limit: 1})
			},
		},
		{
			name:     "history mark price",
			wantPath: "/api/v5/market/history-mark-price-candles?before=4&instId=BTC-USD-SWAP&limit=1",
			call: func(ctx context.Context, client *Client) ([]Candle, error) {
				return client.Market.HistoryMarkPriceCandles(ctx, CandlesRequest{InstID: "BTC-USD-SWAP", Before: "4", Limit: 1})
			},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candles, err := tt.call(context.Background(), client)
			if err != nil {
				t.Fatal(err)
			}
			if paths[i] != tt.wantPath {
				t.Fatalf("path = %q, want %q", paths[i], tt.wantPath)
			}
			if len(candles) != 1 || candles[0].TS != "1597026383085" || candles[0].Open != "1" || candles[0].VolCcyQuote != "12" || candles[0].Confirm != "1" {
				t.Fatalf("unexpected candles: %+v", candles)
			}
		})
	}
}

func TestIndexComponentsDecodesObjectData(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.RequestURI() != "/api/v5/market/index-components?index=BTC-USDT" {
			t.Fatalf("path = %q", r.URL.RequestURI())
		}
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
	client := NewClient(WithBaseURL("https://unit.test"), WithHTTPClient(httpClient), WithRateLimiter(nil))

	got, err := client.Market.IndexComponents(context.Background(), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != "BTC-USDT" || len(got.Components) != 1 || got.Components[0].Wgt != "0.25" {
		t.Fatalf("unexpected index components: %+v", got)
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
				"fillSz":   "1",
				"side":     "buy",
				"execType": "T",
				"feeCcy":   "USDT",
				"fee":      "0",
				"ts":       "3",
			}},
		})
	})}
	client := NewClient(
		WithBaseURL("https://unit.test"),
		WithHTTPClient(httpClient),
		WithCredentials("key", "secret", "pass"),
		WithRateLimiter(nil),
	)

	fills, err := client.Trade.FillsHistory(context.Background(), FillsHistoryRequest{
		InstType: InstSpot,
		InstID:   "BTC-USDT",
		OrdID:    "2",
		Limit:    10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/v5/trade/fills-history?instId=BTC-USDT&instType=SPOT&limit=10&ordId=2" {
		t.Fatalf("path = %q", gotPath)
	}
	if len(fills) != 1 || fills[0].TradeID != "1" || fills[0].FillPx != "100" {
		t.Fatalf("unexpected fills: %+v", fills)
	}
}

func TestTradeBatchEndpoints(t *testing.T) {
	tests := []struct {
		name     string
		wantPath string
		call     func(context.Context, *Client) ([]OrderAck, error)
	}{
		{
			name:     "cancel",
			wantPath: "/api/v5/trade/cancel-batch-orders",
			call: func(ctx context.Context, client *Client) ([]OrderAck, error) {
				return client.Trade.CancelMultipleOrders(ctx, []CancelOrderRequest{{InstID: "BTC-USDT", OrdID: "1"}})
			},
		},
		{
			name:     "amend",
			wantPath: "/api/v5/trade/amend-batch-orders",
			call: func(ctx context.Context, client *Client) ([]OrderAck, error) {
				return client.Trade.AmendMultipleOrders(ctx, []AmendOrderRequest{{InstID: "BTC-USDT", OrdID: "1", NewPx: "101"}})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath string
			httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotPath = r.URL.RequestURI()
				return jsonResponse(map[string]any{
					"code": "0",
					"msg":  "",
					"data": []map[string]string{{"ordId": "1", "sCode": "0", "sMsg": ""}},
				})
			})}
			client := NewClient(
				WithBaseURL("https://unit.test"),
				WithHTTPClient(httpClient),
				WithCredentials("key", "secret", "pass"),
				WithRateLimiter(nil),
			)
			acks, err := tt.call(context.Background(), client)
			if err != nil {
				t.Fatal(err)
			}
			if gotPath != tt.wantPath {
				t.Fatalf("path = %q, want %q", gotPath, tt.wantPath)
			}
			if len(acks) != 1 || acks[0].SCode != "0" {
				t.Fatalf("unexpected acks: %+v", acks)
			}
		})
	}
}

func TestAccountMaxAvailSetLeverageAndFeeTypeEndpoints(t *testing.T) {
	var paths []string
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.RequestURI())
		switch r.URL.Path {
		case "/api/v5/account/max-avail-size":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []map[string]string{{"instId": "BTC-USDT", "ccy": "USDT", "maxBuy": "1", "maxSell": "2"}},
			})
		case "/api/v5/account/set-leverage":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []map[string]string{{"instId": "BTC-USDT", "lever": "2", "mgnMode": "cross", "posSide": "net"}},
			})
		case "/api/v5/account/set-fee-type":
			return jsonResponse(map[string]any{
				"code": "0",
				"msg":  "",
				"data": []map[string]string{{"feeType": "1"}},
			})
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
			return nil, nil
		}
	})}
	client := NewClient(
		WithBaseURL("https://unit.test"),
		WithHTTPClient(httpClient),
		WithCredentials("key", "secret", "pass"),
		WithRateLimiter(nil),
	)

	maxAvail, err := client.Account.MaxAvailSize(context.Background(), MaxAvailSizeRequest{InstID: "BTC-USDT", TdMode: TdCash, Ccy: "USDT"})
	if err != nil {
		t.Fatal(err)
	}
	if len(maxAvail) != 1 || maxAvail[0].MaxBuy != "1" {
		t.Fatalf("unexpected max avail: %+v", maxAvail)
	}
	lever, err := client.Account.SetLeverage(context.Background(), SetLeverageRequest{InstID: "BTC-USDT", Lever: "2", MgnMode: MgnCross})
	if err != nil {
		t.Fatal(err)
	}
	if len(lever) != 1 || lever[0].Lever != "2" {
		t.Fatalf("unexpected leverage: %+v", lever)
	}
	feeType, err := client.Account.SetFeeType(context.Background(), SetFeeTypeRequest{FeeType: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(feeType) != 1 || feeType[0].FeeType != "1" {
		t.Fatalf("unexpected fee type: %+v", feeType)
	}
	wantFirst := "/api/v5/account/max-avail-size?ccy=USDT&instId=BTC-USDT&tdMode=cash"
	if len(paths) != 3 || paths[0] != wantFirst || paths[1] != "/api/v5/account/set-leverage" || paths[2] != "/api/v5/account/set-fee-type" {
		t.Fatalf("unexpected paths: %+v", paths)
	}
}

func TestCandleAndOrderBookUnmarshal(t *testing.T) {
	var candles []Candle
	if err := json.Unmarshal([]byte(`[["1597026383085","1","2","0.5","1.5","10","11","12","1"]]`), &candles); err != nil {
		t.Fatal(err)
	}
	if len(candles) != 1 || candles[0].Open != "1" || candles[0].VolCcyQuote != "12" {
		t.Fatalf("unexpected candles: %+v", candles)
	}
	if err := json.Unmarshal([]byte(`[["1597026383086","2","3","1.5","2.5","0"]]`), &candles); err != nil {
		t.Fatal(err)
	}
	if len(candles) != 1 || candles[0].TS != "1597026383086" || candles[0].Confirm != "0" || candles[0].Vol != "" {
		t.Fatalf("unexpected short candle row: %+v", candles)
	}

	var books []OrderBook
	if err := json.Unmarshal([]byte(`[{"asks":[["2","1","0","1"]],"bids":[["1","2","0","1"]],"ts":"1","seqId":2,"prevSeqId":"1"}]`), &books); err != nil {
		t.Fatal(err)
	}
	if len(books) != 1 || books[0].SeqID.String() != "2" || books[0].PrevSeqID.String() != "1" {
		t.Fatalf("unexpected books: %+v", books)
	}
}

func TestValidationErrors(t *testing.T) {
	client := NewClient(WithRateLimiter(nil))
	if _, err := client.Trade.Order(context.Background(), OrderRequest{InstID: "BTC-USDT"}); err == nil {
		t.Fatal("expected missing order id error")
	}
	if _, err := client.Account.MaxAvailSize(context.Background(), MaxAvailSizeRequest{InstID: "BTC-USDT"}); err == nil {
		t.Fatal("expected missing tdMode error")
	}
	if _, err := client.Market.Trades(context.Background(), TradesRequest{}); err == nil {
		t.Fatal("expected missing trades instId error")
	}
	if _, err := client.Market.HistoryTrades(context.Background(), HistoryTradesRequest{}); err == nil {
		t.Fatal("expected missing history trades instId error")
	}
	if _, err := client.Market.CandlesWithRequest(context.Background(), CandlesRequest{}); err == nil {
		t.Fatal("expected missing candles instId error")
	}
	if _, err := client.Market.IndexCandles(context.Background(), CandlesRequest{}); err == nil {
		t.Fatal("expected missing index candles instId error")
	}
	if _, err := client.Market.MarkPriceCandles(context.Background(), CandlesRequest{}); err == nil {
		t.Fatal("expected missing mark price candles instId error")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func jsonResponse(v any) (*http.Response, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(string(body))),
	}, nil
}

func referenceSign(secret, timestamp, method, requestPath, body string) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write([]byte(timestamp + method + requestPath + body))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
