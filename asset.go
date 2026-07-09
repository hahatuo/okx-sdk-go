package okx

import (
	"context"
	"net/url"
)

// AssetService covers /api/v5/asset/* (funding account, private).
type AssetService struct{ c *Client }

// Balances returns funding-account balances. ccy is an optional comma-separated
// filter ("" for all).
//
// GET /api/v5/asset/balances
func (s *AssetService) Balances(ctx context.Context, ccy string) ([]AssetBalance, error) {
	var q url.Values
	if ccy != "" {
		q = url.Values{"ccy": {ccy}}
	}
	return executeGet[AssetBalance](ctx, s.c, "/api/v5/asset/balances", q, "private:asset:balances", true)
}

// Currencies returns funding currency metadata, including chain-level
// deposit/withdrawal settings.
//
// GET /api/v5/asset/currencies
func (s *AssetService) Currencies(ctx context.Context, ccy string) ([]Currency, error) {
	var q url.Values
	if ccy != "" {
		q = url.Values{"ccy": {ccy}}
	}
	return executeGet[Currency](ctx, s.c, "/api/v5/asset/currencies", q, "private:asset:currencies", true)
}

// Transfer moves funds between OKX account types.
//
// POST /api/v5/asset/transfer
func (s *AssetService) Transfer(ctx context.Context, req TransferRequest) ([]Transfer, error) {
	if req.Ccy == "" {
		return nil, required("ccy")
	}
	if req.Amt == "" {
		return nil, required("amt")
	}
	if req.From == "" {
		return nil, required("from")
	}
	if req.To == "" {
		return nil, required("to")
	}
	var out []Transfer
	err := s.c.do(ctx, requestSpec{
		method:  "POST",
		path:    "/api/v5/asset/transfer",
		body:    req,
		auth:    true,
		rateKey: "private:asset:transfer",
	}, &out)
	return out, err
}
