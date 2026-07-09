package okx

import (
	"context"
	"net/url"
)

// AccountService covers /api/v5/account/* (private).
type AccountService struct{ c *Client }

// Balance returns the trading-account balance. ccy is an optional
// comma-separated currency filter ("" for all).
//
// GET /api/v5/account/balance
func (s *AccountService) Balance(ctx context.Context, ccy string) ([]Balance, error) {
	var q url.Values
	if ccy != "" {
		q = url.Values{"ccy": {ccy}}
	}
	return executeGet[Balance](ctx, s.c, "/api/v5/account/balance", q, "private:account:balance", true)
}

// BalanceByRequest returns the trading-account balance using a request struct.
func (s *AccountService) BalanceByRequest(ctx context.Context, req BalanceRequest) ([]Balance, error) {
	return s.Balance(ctx, req.Ccy)
}

// Positions retrieves current positions.
//
// GET /api/v5/account/positions
func (s *AccountService) Positions(ctx context.Context, req PositionsRequest) ([]Position, error) {
	q := url.Values{}
	setIfNotEmpty(q, "instType", string(req.InstType))
	setIfNotEmpty(q, "instId", req.InstID)
	setIfNotEmpty(q, "posId", req.PosID)
	return executeGet[Position](ctx, s.c, "/api/v5/account/positions", q, "private:account:positions", true)
}

// MaxAvailSize returns the maximum buy/sell size available for an instrument.
//
// GET /api/v5/account/max-avail-size
func (s *AccountService) MaxAvailSize(ctx context.Context, req MaxAvailSizeRequest) ([]MaxAvailSize, error) {
	if req.InstID == "" {
		return nil, required("instId")
	}
	if req.TdMode == "" {
		return nil, required("tdMode")
	}
	q := url.Values{"instId": {req.InstID}, "tdMode": {string(req.TdMode)}}
	setIfNotEmpty(q, "ccy", req.Ccy)
	setBoolIfNotNil(q, "reduceOnly", req.ReduceOnly)
	setIfNotEmpty(q, "px", req.Px)
	setIfNotEmpty(q, "tradeQuoteCcy", req.TradeQuoteCcy)
	var out []MaxAvailSize
	err := s.c.do(ctx, requestSpec{
		method:  "GET",
		path:    "/api/v5/account/max-avail-size",
		query:   q,
		auth:    true,
		rateKey: "private:account:max-avail-size:" + req.InstID,
	}, &out)
	return out, err
}

// SetLeverage sets account leverage for the requested instrument or currency.
//
// POST /api/v5/account/set-leverage
func (s *AccountService) SetLeverage(ctx context.Context, req SetLeverageRequest) ([]Leverage, error) {
	if req.Lever == "" {
		return nil, required("lever")
	}
	if req.MgnMode == "" {
		return nil, required("mgnMode")
	}
	if req.InstID == "" && req.Ccy == "" {
		return nil, required("instId or ccy")
	}
	var out []Leverage
	err := s.c.do(ctx, requestSpec{
		method:  "POST",
		path:    "/api/v5/account/set-leverage",
		body:    req,
		auth:    true,
		rateKey: "private:account:set-leverage",
	}, &out)
	return out, err
}

// SetFeeType sets the account fee type.
//
// POST /api/v5/account/set-fee-type
func (s *AccountService) SetFeeType(ctx context.Context, req SetFeeTypeRequest) ([]FeeType, error) {
	if req.FeeType == "" {
		return nil, required("feeType")
	}
	var out []FeeType
	err := s.c.do(ctx, requestSpec{
		method:  "POST",
		path:    "/api/v5/account/set-fee-type",
		body:    req,
		auth:    true,
		rateKey: "private:account:set-fee-type",
	}, &out)
	return out, err
}
