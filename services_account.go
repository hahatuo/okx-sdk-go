package okx

import (
	"context"
	"net/http"
)

type AccountService struct {
	client *Client
}

type BalanceRequest struct {
	Ccy string
}

func (s *AccountService) Balance(ctx context.Context, req BalanceRequest) ([]Balance, error) {
	q := values()
	setIfNotEmpty(q, "ccy", req.Ccy)
	var out []Balance
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/account/balance",
		query:   q,
		auth:    true,
		rateKey: "private:account:balance",
	}, &out)
	return out, err
}

type PositionsRequest struct {
	InstType string
	InstID   string
}

func (s *AccountService) Positions(ctx context.Context, req PositionsRequest) ([]Position, error) {
	q := values()
	setIfNotEmpty(q, "instType", req.InstType)
	setIfNotEmpty(q, "instId", req.InstID)
	var out []Position
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/account/positions",
		query:   q,
		auth:    true,
		rateKey: "private:account:positions",
	}, &out)
	return out, err
}

func (s *AccountService) MaxAvailSize(ctx context.Context, req MaxAvailSizeRequest) ([]MaxAvailSize, error) {
	q := values("instId", req.InstID, "tdMode", req.TdMode)
	setIfNotEmpty(q, "ccy", req.Ccy)
	setBoolIfNotNil(q, "reduceOnly", req.ReduceOnly)
	setIfNotEmpty(q, "px", req.Px)
	setIfNotEmpty(q, "tradeQuoteCcy", req.TradeQuoteCcy)
	var out []MaxAvailSize
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/account/max-avail-size",
		query:   q,
		auth:    true,
		rateKey: "private:account:max-avail-size",
	}, &out)
	return out, err
}

func (s *AccountService) SetLeverage(ctx context.Context, req SetLeverageRequest) ([]Leverage, error) {
	var out []Leverage
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodPost,
		path:    "/api/v5/account/set-leverage",
		body:    req,
		auth:    true,
		rateKey: "private:account:set-leverage",
	}, &out)
	return out, err
}

func (s *AccountService) SetFeeType(ctx context.Context, req SetFeeTypeRequest) ([]FeeType, error) {
	var out []FeeType
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodPost,
		path:    "/api/v5/account/set-fee-type",
		body:    req,
		auth:    true,
		rateKey: "private:account:set-fee-type",
	}, &out)
	return out, err
}
