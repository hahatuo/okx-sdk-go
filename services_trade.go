package okx

import (
	"context"
	"net/http"
)

type TradeService struct {
	client *Client
}

func (s *TradeService) PlaceOrder(ctx context.Context, req PlaceOrderRequest) ([]OrderAck, error) {
	var out []OrderAck
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodPost,
		path:    "/api/v5/trade/order",
		body:    req,
		auth:    true,
		rateKey: "private:trade:order:" + req.InstID,
	}, &out)
	return out, err
}

func (s *TradeService) PlaceMultipleOrders(ctx context.Context, req []PlaceOrderRequest) ([]OrderAck, error) {
	var out []OrderAck
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodPost,
		path:    "/api/v5/trade/batch-orders",
		body:    req,
		auth:    true,
		rateKey: "private:trade:batch-orders",
	}, &out)
	return out, err
}

func (s *TradeService) CancelOrder(ctx context.Context, req CancelOrderRequest) ([]OrderAck, error) {
	var out []OrderAck
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodPost,
		path:    "/api/v5/trade/cancel-order",
		body:    req,
		auth:    true,
		rateKey: "private:trade:cancel-order:" + req.InstID,
	}, &out)
	return out, err
}

func (s *TradeService) CancelMultipleOrders(ctx context.Context, req []CancelOrderRequest) ([]OrderAck, error) {
	var out []OrderAck
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodPost,
		path:    "/api/v5/trade/cancel-batch-orders",
		body:    req,
		auth:    true,
		rateKey: "private:trade:cancel-batch-orders",
	}, &out)
	return out, err
}

func (s *TradeService) AmendOrder(ctx context.Context, req AmendOrderRequest) ([]OrderAck, error) {
	var out []OrderAck
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodPost,
		path:    "/api/v5/trade/amend-order",
		body:    req,
		auth:    true,
		rateKey: "private:trade:amend-order:" + req.InstID,
	}, &out)
	return out, err
}

func (s *TradeService) AmendMultipleOrders(ctx context.Context, req []AmendOrderRequest) ([]OrderAck, error) {
	var out []OrderAck
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodPost,
		path:    "/api/v5/trade/amend-batch-orders",
		body:    req,
		auth:    true,
		rateKey: "private:trade:amend-batch-orders",
	}, &out)
	return out, err
}

type OrderRequest struct {
	InstID  string
	OrdID   string
	ClOrdID string
}

func (s *TradeService) Order(ctx context.Context, req OrderRequest) ([]Order, error) {
	q := values("instId", req.InstID)
	setIfNotEmpty(q, "ordId", req.OrdID)
	setIfNotEmpty(q, "clOrdId", req.ClOrdID)
	var out []Order
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/trade/order",
		query:   q,
		auth:    true,
		rateKey: "private:trade:get-order:" + req.InstID,
	}, &out)
	return out, err
}

type OrdersPendingRequest struct {
	InstType   string
	InstFamily string
	InstID     string
	OrdType    string
	State      string
	After      string
	Before     string
	Limit      string
}

func (s *TradeService) OrdersPending(ctx context.Context, req OrdersPendingRequest) ([]Order, error) {
	q := values()
	setIfNotEmpty(q, "instType", req.InstType)
	setIfNotEmpty(q, "instFamily", req.InstFamily)
	setIfNotEmpty(q, "instId", req.InstID)
	setIfNotEmpty(q, "ordType", req.OrdType)
	setIfNotEmpty(q, "state", req.State)
	setIfNotEmpty(q, "after", req.After)
	setIfNotEmpty(q, "before", req.Before)
	setIfNotEmpty(q, "limit", req.Limit)
	var out []Order
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/trade/orders-pending",
		query:   q,
		auth:    true,
		rateKey: "private:trade:orders-pending",
	}, &out)
	return out, err
}

type FillsHistoryRequest struct {
	InstType   string
	Uly        string
	InstFamily string
	InstID     string
	OrdID      string
	After      string
	Before     string
	Begin      string
	End        string
	Limit      string
}

func (s *TradeService) FillsHistory(ctx context.Context, req FillsHistoryRequest) ([]Fill, error) {
	q := values()
	setIfNotEmpty(q, "instType", req.InstType)
	setIfNotEmpty(q, "uly", req.Uly)
	setIfNotEmpty(q, "instFamily", req.InstFamily)
	setIfNotEmpty(q, "instId", req.InstID)
	setIfNotEmpty(q, "ordId", req.OrdID)
	setIfNotEmpty(q, "after", req.After)
	setIfNotEmpty(q, "before", req.Before)
	setIfNotEmpty(q, "begin", req.Begin)
	setIfNotEmpty(q, "end", req.End)
	setIfNotEmpty(q, "limit", req.Limit)
	var out []Fill
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/trade/fills-history",
		query:   q,
		auth:    true,
		rateKey: "private:trade:fills-history",
	}, &out)
	return out, err
}
