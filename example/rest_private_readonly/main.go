package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	okx "github.com/hahatuo/okx-sdk-go"
)

func main() {
	apiKey := strings.TrimSpace(os.Getenv("OKX_API_KEY"))
	secretKey := strings.TrimSpace(os.Getenv("OKX_SECRET_KEY"))
	passphrase := strings.TrimSpace(os.Getenv("OKX_PASSPHRASE"))
	if apiKey == "" || secretKey == "" || passphrase == "" {
		log.Fatal("set OKX_API_KEY, OKX_SECRET_KEY, and OKX_PASSPHRASE")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := okx.NewRestClient(
		okx.WithCredentials(apiKey, secretKey, passphrase),
		okx.WithTimeout(10*time.Second),
	)

	balances, err := client.Account.Balance(ctx, okx.BalanceRequest{Ccy: strings.TrimSpace(os.Getenv("OKX_CCY"))})
	if err != nil {
		log.Fatal(err)
	}
	if len(balances) == 0 {
		fmt.Println("empty balance response")
		return
	}

	balance := balances[0]
	fmt.Printf("totalEq=%s uTime=%s detailCount=%d\n", balance.TotalEq, balance.UTime, len(balance.Details))
	for _, detail := range balance.Details {
		fmt.Printf("%s eq=%s avail=%s cash=%s\n", detail.Ccy, detail.Eq, detail.AvailBal, detail.CashBal)
	}
}
