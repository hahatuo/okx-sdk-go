package okx

import (
	"context"
	"net/http"
)

type MarketService struct {
	client *Client
}

type TickersRequest struct {
	InstType   string
	InstFamily string
}

func (s *MarketService) Tickers(ctx context.Context, req TickersRequest) ([]Ticker, error) {
	q := values("instType", req.InstType)
	setIfNotEmpty(q, "instFamily", req.InstFamily)
	var out []Ticker
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/market/tickers",
		query:   q,
		rateKey: "public:market:tickers:" + req.InstType,
	}, &out)
	return out, err
}

type TickerRequest struct {
	InstID string
}

func (s *MarketService) Ticker(ctx context.Context, req TickerRequest) ([]Ticker, error) {
	q := values("instId", req.InstID)
	var out []Ticker
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/market/ticker",
		query:   q,
		rateKey: "public:market:ticker:" + req.InstID,
	}, &out)
	return out, err
}

type OrderBookRequest struct {
	InstID string
	Size   string
}

func (s *MarketService) OrderBook(ctx context.Context, req OrderBookRequest) ([]OrderBook, error) {
	q := values("instId", req.InstID)
	setIfNotEmpty(q, "sz", req.Size)
	var out []OrderBook
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/market/books",
		query:   q,
		rateKey: "public:market:books:" + req.InstID,
	}, &out)
	return out, err
}

type CandlesRequest struct {
	InstID  string
	Bar     string
	After   string
	Before  string
	Limit   string
	History bool
}

func (s *MarketService) Candles(ctx context.Context, req CandlesRequest) ([]Candle, error) {
	q := values("instId", req.InstID)
	setIfNotEmpty(q, "bar", req.Bar)
	setIfNotEmpty(q, "after", req.After)
	setIfNotEmpty(q, "before", req.Before)
	setIfNotEmpty(q, "limit", req.Limit)

	path := "/api/v5/market/candles"
	if req.History {
		path = "/api/v5/market/history-candles"
	}
	var out []Candle
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    path,
		query:   q,
		rateKey: "public:market:candles:" + req.InstID,
	}, &out)
	return out, err
}

func (s *MarketService) IndexComponents(ctx context.Context, index string) (IndexComponents, error) {
	q := values("index", index)
	var out IndexComponents
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/market/index-components",
		query:   q,
		rateKey: "public:market:index-components:" + index,
	}, &out)
	return out, err
}

func (s *MarketService) Platform24Volume(ctx context.Context) ([]Platform24Volume, error) {
	var out []Platform24Volume
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/market/platform-24-volume",
		rateKey: "public:market:platform-24-volume",
	}, &out)
	return out, err
}
