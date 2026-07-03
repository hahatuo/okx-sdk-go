package okx

import (
	"context"
	"net/http"
)

type PublicDataService struct {
	client *Client
}

type InstrumentsRequest struct {
	InstType   string
	SeriesID   string
	InstFamily string
	InstID     string
}

func (s *PublicDataService) Instruments(ctx context.Context, req InstrumentsRequest) ([]Instrument, error) {
	q := values("instType", req.InstType)
	setIfNotEmpty(q, "seriesId", req.SeriesID)
	setIfNotEmpty(q, "instFamily", req.InstFamily)
	setIfNotEmpty(q, "instId", req.InstID)
	var out []Instrument
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/public/instruments",
		query:   q,
		rateKey: "public:data:instruments:" + req.InstType,
	}, &out)
	return out, err
}

func (s *PublicDataService) Time(ctx context.Context) ([]SystemTime, error) {
	var out []SystemTime
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/public/time",
		rateKey: "public:data:time",
	}, &out)
	return out, err
}

func (s *PublicDataService) Underlying(ctx context.Context, instType string) ([]Underlying, error) {
	q := values("instType", instType)
	var out []Underlying
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/public/underlying",
		query:   q,
		rateKey: "public:data:underlying:" + instType,
	}, &out)
	return out, err
}

func (s *PublicDataService) FundingRate(ctx context.Context, instID string) ([]FundingRate, error) {
	q := values("instId", instID)
	var out []FundingRate
	err := s.client.do(ctx, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v5/public/funding-rate",
		query:   q,
		rateKey: "public:data:funding-rate:" + instID,
	}, &out)
	return out, err
}
