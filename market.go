package okx

import (
	"context"
	"net/url"
)

// MarketService covers /api/v5/market/* (public).
type MarketService struct{ c *Client }

// Tickers returns the latest ticker snapshot for an instrument type.
//
// GET /api/v5/market/tickers
func (s *MarketService) Tickers(ctx context.Context, req TickersRequest) ([]Ticker, error) {
	if req.InstType == "" {
		return nil, required("instType")
	}
	q := url.Values{"instType": {string(req.InstType)}}
	setIfNotEmpty(q, "instFamily", req.InstFamily)
	setIfNotEmpty(q, "uly", req.Uly)
	return executeGet[Ticker](ctx, s.c, "/api/v5/market/tickers", q, false)
}

// Ticker returns the latest ticker snapshot for one instrument.
//
// GET /api/v5/market/ticker
func (s *MarketService) Ticker(ctx context.Context, req TickerRequest) ([]Ticker, error) {
	if req.InstID == "" {
		return nil, required("instId")
	}
	q := url.Values{"instId": {req.InstID}}
	return executeGet[Ticker](ctx, s.c, "/api/v5/market/ticker", q, false)
}

// OrderBook returns a REST order book snapshot.
//
// GET /api/v5/market/books
func (s *MarketService) OrderBook(ctx context.Context, req OrderBookRequest) ([]OrderBook, error) {
	if req.InstID == "" {
		return nil, required("instId")
	}
	q := url.Values{"instId": {req.InstID}}
	if req.Size > 0 {
		q.Set("sz", itoa(req.Size))
	}
	return executeGet[OrderBook](ctx, s.c, "/api/v5/market/books", q, false)
}

// Trades returns recent transactions for one instrument.
//
// GET /api/v5/market/trades
func (s *MarketService) Trades(ctx context.Context, req TradesRequest) ([]MarketTrade, error) {
	if req.InstID == "" {
		return nil, required("instId")
	}
	q := url.Values{"instId": {req.InstID}}
	if req.Limit > 0 {
		q.Set("limit", itoa(req.Limit))
	}
	return executeGet[MarketTrade](ctx, s.c, "/api/v5/market/trades", q, false)
}

// HistoryTrades returns historical transactions for one instrument with
// pagination controls.
//
// GET /api/v5/market/history-trades
func (s *MarketService) HistoryTrades(ctx context.Context, req HistoryTradesRequest) ([]MarketTrade, error) {
	if req.InstID == "" {
		return nil, required("instId")
	}
	q := url.Values{"instId": {req.InstID}}
	setIfNotEmpty(q, "type", req.Type)
	setIfNotEmpty(q, "after", req.After)
	setIfNotEmpty(q, "before", req.Before)
	if req.Limit > 0 {
		q.Set("limit", itoa(req.Limit))
	}
	return executeGet[MarketTrade](ctx, s.c, "/api/v5/market/history-trades", q, false)
}

// Candles returns historical candlesticks. bar is e.g. "1m","1H","1D"; limit is
// capped at 300 by OKX (0 uses the API default).
//
// GET /api/v5/market/candles
func (s *MarketService) Candles(ctx context.Context, instID, bar string, limit int) ([]Candle, error) {
	return s.CandlesWithRequest(ctx, CandlesRequest{InstID: instID, Bar: bar, Limit: limit})
}

// CandlesWithRequest returns recent candlesticks with full query controls.
//
// GET /api/v5/market/candles
func (s *MarketService) CandlesWithRequest(ctx context.Context, req CandlesRequest) ([]Candle, error) {
	req.History = false
	return s.candles(ctx, req)
}

// HistoryCandles returns historical candlesticks.
//
// GET /api/v5/market/history-candles
func (s *MarketService) HistoryCandles(ctx context.Context, req CandlesRequest) ([]Candle, error) {
	req.History = true
	return s.candles(ctx, req)
}

// IndexCandles returns recent index candlesticks.
//
// GET /api/v5/market/index-candles
func (s *MarketService) IndexCandles(ctx context.Context, req CandlesRequest) ([]Candle, error) {
	return s.candlesEndpoint(ctx, req, "/api/v5/market/index-candles")
}

// HistoryIndexCandles returns historical index candlesticks.
//
// GET /api/v5/market/history-index-candles
func (s *MarketService) HistoryIndexCandles(ctx context.Context, req CandlesRequest) ([]Candle, error) {
	return s.candlesEndpoint(ctx, req, "/api/v5/market/history-index-candles")
}

// MarkPriceCandles returns recent mark-price candlesticks.
//
// GET /api/v5/market/mark-price-candles
func (s *MarketService) MarkPriceCandles(ctx context.Context, req CandlesRequest) ([]Candle, error) {
	return s.candlesEndpoint(ctx, req, "/api/v5/market/mark-price-candles")
}

// HistoryMarkPriceCandles returns historical mark-price candlesticks.
//
// GET /api/v5/market/history-mark-price-candles
func (s *MarketService) HistoryMarkPriceCandles(ctx context.Context, req CandlesRequest) ([]Candle, error) {
	return s.candlesEndpoint(ctx, req, "/api/v5/market/history-mark-price-candles")
}

func (s *MarketService) candles(ctx context.Context, req CandlesRequest) ([]Candle, error) {
	path := "/api/v5/market/candles"
	if req.History {
		path = "/api/v5/market/history-candles"
	}
	return s.candlesEndpoint(ctx, req, path)
}

func (s *MarketService) candlesEndpoint(ctx context.Context, req CandlesRequest, path string) ([]Candle, error) {
	if req.InstID == "" {
		return nil, required("instId")
	}
	q := url.Values{"instId": {req.InstID}}
	setIfNotEmpty(q, "bar", req.Bar)
	setIfNotEmpty(q, "after", req.After)
	setIfNotEmpty(q, "before", req.Before)
	if req.Limit > 0 {
		q.Set("limit", itoa(req.Limit))
	}
	return executeGet[Candle](ctx, s.c, path, q, false)
}

// IndexComponents returns component prices and weights for an index.
//
// GET /api/v5/market/index-components
func (s *MarketService) IndexComponents(ctx context.Context, index string) (IndexComponents, error) {
	if index == "" {
		return IndexComponents{}, required("index")
	}
	q := url.Values{"index": {index}}
	return execute[IndexComponents](ctx, s.c, requestSpec{
		method:      "GET",
		path:        "/api/v5/market/index-components",
		query:       q,
		rateLimited: true,
	})
}

// Platform24Volume returns OKX platform-wide 24h volume.
//
// GET /api/v5/market/platform-24-volume
func (s *MarketService) Platform24Volume(ctx context.Context) ([]Platform24Volume, error) {
	return executeGet[Platform24Volume](ctx, s.c, "/api/v5/market/platform-24-volume", nil, false)
}
