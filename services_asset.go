package okx

import (
	"context"
	"net/http"
)

type AssetService struct {
	client *Client
}

func (s *AssetService) Currencies(ctx context.Context, ccy string) ([]Currency, error) {
	q := values()
	setIfNotEmpty(q, "ccy", ccy)
	var out []Currency
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/asset/currencies",
		query:   q,
		auth:    true,
		rateKey: "private:asset:currencies",
	}, &out)
	return out, err
}

func (s *AssetService) Balances(ctx context.Context, ccy string) ([]AssetBalance, error) {
	q := values()
	setIfNotEmpty(q, "ccy", ccy)
	var out []AssetBalance
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/asset/balances",
		query:   q,
		auth:    true,
		rateKey: "private:asset:balances",
	}, &out)
	return out, err
}
