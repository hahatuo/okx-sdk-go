package okx

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestIntegrationReadOnlyAccountBalance(t *testing.T) {
	apiKey, secretKey, passphrase := okxCredentialsFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewRestClient(
		WithCredentials(apiKey, secretKey, passphrase),
		WithTimeout(10*time.Second),
	)

	if baseURL := strings.TrimSpace(os.Getenv("OKX_REST_URL")); baseURL != "" {
		WithBaseURL(baseURL)(client.Client)
	}

	if _, err := client.Account.Balance(ctx, BalanceRequest{}); err != nil {
		t.Fatalf("read-only account balance request failed: %v", err)
	}
}

func TestIntegrationPublicOrderBookDepthChannels(t *testing.T) {
	if strings.TrimSpace(os.Getenv("OKX_WS_INTEGRATION")) != "1" {
		t.Skip("set OKX_WS_INTEGRATION=1 to run OKX public WebSocket integration tests")
	}

	instID := strings.TrimSpace(os.Getenv("OKX_WS_DEPTH_INST_ID"))
	if instID == "" {
		instID = "BTC-USDT"
	}

	tests := []struct {
		name      string
		channel   string
		maxLevels int
	}{
		{name: "best bid offer", channel: "bbo-tbt", maxLevels: 1},
		{name: "five levels", channel: "books5", maxLevels: 5},
		{name: "fifty levels", channel: "books50-l2-tbt", maxLevels: 50},
		{name: "four hundred levels", channel: "books-l2-tbt", maxLevels: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			ws := NewWSClient()
			if wsURL := strings.TrimSpace(os.Getenv("OKX_WS_PUBLIC_URL")); wsURL != "" {
				WithWSURL(wsURL)(ws)
			}
			if err := ws.Connect(ctx); err != nil {
				t.Fatalf("connect public ws: %v", err)
			}
			defer ws.Close()

			ch, err := ws.Subscribe(ctx, Subscription{
				Channel: tt.channel,
				Args:    map[string]string{"instId": instID},
			})
			if err != nil {
				t.Fatalf("subscribe %s %s: %v", tt.channel, instID, err)
			}

			msg := waitForWSMessage(t, ch, 20*time.Second)
			if msg.Arg["channel"] != tt.channel {
				t.Fatalf("channel = %q, want %q", msg.Arg["channel"], tt.channel)
			}
			if msg.Arg["instId"] != instID {
				t.Fatalf("instId = %q, want %q", msg.Arg["instId"], instID)
			}

			books := decodeOrderBookData(t, msg.Data)
			if len(books) == 0 {
				t.Fatal("empty order book data")
			}
			if len(books[0].Asks) == 0 {
				t.Fatal("empty asks")
			}
			if len(books[0].Bids) == 0 {
				t.Fatal("empty bids")
			}
			if len(books[0].Asks) > tt.maxLevels {
				t.Fatalf("asks length = %d, want <= %d", len(books[0].Asks), tt.maxLevels)
			}
			if len(books[0].Bids) > tt.maxLevels {
				t.Fatalf("bids length = %d, want <= %d", len(books[0].Bids), tt.maxLevels)
			}
		})
	}
}

func okxCredentialsFromEnv(t *testing.T) (string, string, string) {
	t.Helper()

	apiKey := strings.TrimSpace(os.Getenv("OKX_API_KEY"))
	secretKey := strings.TrimSpace(os.Getenv("OKX_SECRET_KEY"))
	passphrase := strings.TrimSpace(os.Getenv("OKX_PASSPHRASE"))
	if apiKey == "" || secretKey == "" || passphrase == "" {
		t.Skip("set OKX_API_KEY, OKX_SECRET_KEY, and OKX_PASSPHRASE to run OKX integration tests")
	}
	return apiKey, secretKey, passphrase
}

func waitForWSMessage(t *testing.T, ch <-chan WSMessage, timeout time.Duration) WSMessage {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("websocket message channel closed")
		}
		return msg
	case <-timer.C:
		t.Fatalf("timed out waiting for websocket message after %s", timeout)
		return WSMessage{}
	}
}
