# GitHub Workflow

This repository uses separate GitHub Actions workflows for CI, integration,
security, dependency maintenance, and release publishing.

## Core CI

File:

- `.github/workflows/ci.yml`

Triggers:

- pull requests targeting `main`
- pull requests targeting `dev`
- pushes to `main`
- pushes to `dev`
- manual `workflow_dispatch`

Checks:

```bash
go mod download
gofmt -l .
golangci-lint
go vet ./...
go test ./...
go test -race ./...
go test -run=^$ -bench=. -benchmem ./...
```

The benchmark step runs every package benchmark that exists in the repository.
If no benchmarks exist yet, the command still validates that benchmark targets
compile and gives future benchmark tests a stable CI slot.

## OKX Integration

File:

- `.github/workflows/integration.yml`

Triggers:

- pushes to `main`
- pushes to `dev`
- daily schedule
- manual `workflow_dispatch`

The workflow always runs the public WebSocket order book integration test with:

```bash
OKX_WS_INTEGRATION=1 go test ./... -run TestIntegrationPublicOrderBookDepthChannels -count=1
```

By default that test covers the stable `bbo-tbt` and `books5` channels. To
exercise deeper books as well, set:

```bash
OKX_WS_DEPTH_CHANNELS=bbo-tbt,books5,books50-l2-tbt,books-l2-tbt
```

Private REST integration runs only when these repository secrets are configured:

- `OKX_API_KEY`
- `OKX_SECRET_KEY`
- `OKX_PASSPHRASE`

Demo trading integration runs only when these repository secrets are configured:

- `OKX_DEMO_API_KEY`
- `OKX_DEMO_SECRET_KEY`
- `OKX_DEMO_PASSPHRASE`

It runs:

```bash
OKX_DEMO_TRADING_INTEGRATION=1 go test ./... -run TestIntegrationDemoTrading -count=1
```

The demo trading tests verify:

- REST simulated trading limit order placement
- REST simulated trading order cancellation
- private demo WebSocket login

Optional repository variables:

- `OKX_REST_URL`
- `OKX_WS_PUBLIC_URL`
- `OKX_WS_DEPTH_INST_ID`
- `OKX_DEMO_REST_URL`
- `OKX_DEMO_WS_PRIVATE_URL`
- `OKX_DEMO_INST_ID`
- `OKX_DEMO_TD_MODE`
- `OKX_DEMO_SIDE`
- `OKX_DEMO_SZ`
- `OKX_DEMO_PX`

Default demo order settings:

- `OKX_DEMO_INST_ID=BTC-USDT`
- `OKX_DEMO_TD_MODE=cash`
- `OKX_DEMO_SIDE=buy`
- `OKX_DEMO_SZ=0.0001`
- `OKX_DEMO_PX` is derived from the live BTC-USDT ticker when omitted

If `OKX_DEMO_INST_ID` is changed from `BTC-USDT`, set `OKX_DEMO_PX` explicitly
so the test does not guess tick size or a safe off-market price.

## Security

File:

- `.github/workflows/security.yml`

Triggers:

- pull requests targeting `main`
- pull requests targeting `dev`
- pushes to `main`
- pushes to `dev`
- weekly schedule
- manual `workflow_dispatch`

Checks:

```bash
gosec ./...
```

The workflow installs `github.com/securego/gosec/v2/cmd/gosec@v2.27.1`.

## Dependency Updates

File:

- `.github/dependabot.yml`

Dependabot checks weekly for:

- Go module updates
- GitHub Actions updates

## Release

File:

- `.github/workflows/release.yml`

Trigger:

- tags matching `v*.*.*`

The release workflow runs tests, generates `RELEASE_NOTES.md` from Git commit
history since the previous tag, and publishes a GitHub Release with
`softprops/action-gh-release@v3.0.1`.

## Actions

Pinned action versions:

- `actions/checkout@v7`
- `actions/setup-go@v6`
- `golangci/golangci-lint-action@v9.2.1`
- `softprops/action-gh-release@v3.0.1`

Review these pins during major repository maintenance.
