GO ?= go
PKGS ?= ./...
INTEGRATION_TEST_FLAGS ?= -count=1 -v

.PHONY: build test test-contract test-public-rest test-live-readonly test-demo-trading test-demo-market-roundtrip test-demo-ws-private test-ws-public vet bench tidy race clean

build:
	$(GO) build $(PKGS)

test:
	$(GO) test -skip '^TestIntegration' $(PKGS)

# Offline unit/contract/golden tests. These must not require OKX network access.
test-contract:
	$(GO) test -run '^TestContract' $(PKGS)

# Public REST integration; no account credentials required.
test-public-rest:
	OKX_PUBLIC_INTEGRATION=1 $(GO) test $(PKGS) -run TestIntegrationPublicMarketData $(INTEGRATION_TEST_FLAGS)

# Live account read-only integration. This must not place, amend, or cancel orders.
test-live-readonly:
	OKX_RUN_INTEGRATION=1 $(GO) test $(PKGS) -run TestIntegrationReadOnlyAccountBalance $(INTEGRATION_TEST_FLAGS)

# Demo trading low-side-effect test: place a far-from-market limit order, then cancel it.
test-demo-trading:
	OKX_DEMO_TRADING_INTEGRATION=1 $(GO) test $(PKGS) -run TestIntegrationDemoTradingPlaceAndCancelOrder $(INTEGRATION_TEST_FLAGS)

# Demo trading fill-producing test: fetch BTC-USDT minSz, market buy minSz, then market sell minSz.
test-demo-market-roundtrip:
	OKX_DEMO_MARKET_ROUNDTRIP_INTEGRATION=1 $(GO) test $(PKGS) -run TestIntegrationDemoTradingMarketBuyAndSellMinSize $(INTEGRATION_TEST_FLAGS)

# Demo private WebSocket login/connect integration.
test-demo-ws-private:
	OKX_DEMO_WS_INTEGRATION=1 $(GO) test $(PKGS) -run TestIntegrationDemoPrivateWebSocketConnect $(INTEGRATION_TEST_FLAGS)

# Public/business WebSocket market-data integrations.
test-ws-public:
	OKX_WS_INTEGRATION=1 $(GO) test $(PKGS) -run 'TestIntegrationPublicOrderBookDepthChannels|TestIntegrationPublicTradesChannel|TestIntegrationBusinessCandlesChannels' $(INTEGRATION_TEST_FLAGS)

race:
	$(GO) test -race -skip '^TestIntegration' $(PKGS)

# Apply allocation assertions run in ordinary tests; benchmarks report decode costs.
bench:
	$(GO) test -run=^$$ -bench=. -benchmem $(PKGS)

vet:
	$(GO) vet $(PKGS)

tidy:
	$(GO) mod tidy

clean:
	$(GO) clean $(PKGS)
