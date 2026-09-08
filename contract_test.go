package okx

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	json "github.com/go-json-experiment/json"
)

type contractFixture struct {
	File      string `json:"file"`
	Source    string `json:"source"`
	Retrieved string `json:"retrieved"`
	Endpoint  string `json:"endpoint"`
	Method    string `json:"method"`
	Kind      string `json:"kind"`
}

// TestContractOfficialSamples uses fixed official examples, not live API calls.
// manifest.json records source, date and any correction to invalid sample JSON.
func TestContractOfficialSamples(t *testing.T) {
	raw, err := os.ReadFile("testdata/contracts/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []contractFixture
	if err = json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		t.Run(fixture.File, func(t *testing.T) {
			if fixture.Source == "" || fixture.Retrieved == "" {
				t.Fatal("missing provenance")
			}
			raw, err := os.ReadFile(filepath.Join("testdata/contracts", fixture.File))
			if err != nil {
				t.Fatal(err)
			}
			var target any
			if fixture.Kind == "ws" {
				var in wsIncoming
				if err = json.Unmarshal(raw, &in); err != nil {
					t.Fatal(err)
				}
				switch in.Arg.Channel {
				case "account":
					target = new([]AccountUpdate)
					if in.Arg.UID == "" || in.EventType != "snapshot" || in.CurPage != 1 || !in.LastPage {
						t.Fatal("lost account snapshot metadata")
					}
				case "orders":
					target = new([]Order)
				case "books", "books5", "books50-l2-tbt", "books-l2-tbt", "bbo-tbt":
					target = new([]OrderBook)
				default:
					t.Fatalf("unhandled channel %q", in.Arg.Channel)
				}
				if err = json.Unmarshal(in.Data, target); err != nil {
					t.Fatal(err)
				}
				return
			}
			var env envelope
			if err = json.Unmarshal(raw, &env); err != nil {
				t.Fatal(err)
			}
			if env.Code != "0" {
				t.Fatalf("unexpected example code %s", env.Code)
			}
			switch fixture.Endpoint {
			case "/api/v5/account/balance":
				target = new([]Balance)
			case "/api/v5/account/positions":
				target = new([]Position)
			case "/api/v5/account/max-avail-size":
				target = new([]MaxAvailSize)
			case "/api/v5/account/set-leverage":
				target = new([]Leverage)
			case "/api/v5/account/set-fee-type":
				target = new([]FeeType)
			case "/api/v5/asset/balances":
				target = new([]AssetBalance)
			case "/api/v5/asset/currencies":
				target = new([]Currency)
			case "/api/v5/asset/transfer":
				target = new([]Transfer)
			case "/api/v5/public/instruments":
				target = new([]Instrument)
			case "/api/v5/public/time":
				target = new([]SystemTime)
			case "/api/v5/public/underlying":
				client := NewClient(WithRateLimiter(nil), WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw))}, nil
				})}))
				out, err := client.Public.Underlying(t.Context(), InstFutures)
				if err != nil || len(out) != 3 || out[0].Uly != "LTC-USDT" {
					t.Fatalf("underlying: %v %v", out, err)
				}
				return
			case "/api/v5/public/funding-rate":
				target = new([]FundingRate)
			case "/api/v5/market/tickers", "/api/v5/market/ticker":
				target = new([]Ticker)
			case "/api/v5/market/books":
				target = new([]OrderBook)
			case "/api/v5/market/trades", "/api/v5/market/history-trades":
				target = new([]MarketTrade)
			case "/api/v5/market/index-components":
				target = new(IndexComponents)
			case "/api/v5/market/platform-24-volume":
				target = new([]Platform24Volume)
			case "/api/v5/trade/fills-history":
				target = new([]Fill)
			case "/api/v5/trade/orders-pending":
				target = new([]Order)
			case "/api/v5/trade/order":
				if fixture.Method == "GET" {
					target = new([]Order)
				} else {
					target = new([]OrderAck)
				}
			default:
				if strings.Contains(fixture.Endpoint, "candles") {
					target = new([]Candle)
				} else if strings.HasPrefix(fixture.Endpoint, "/api/v5/trade/") {
					target = new([]OrderAck)
				} else {
					t.Fatalf("unhandled endpoint %s", fixture.Endpoint)
				}
			}
			if err = json.Unmarshal(env.Data, target); err != nil {
				t.Fatal(err)
			}
		})
	}
}
