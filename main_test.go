package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
)

var spannerClient *spanner.Client

func TestMain(m *testing.M) {
	spannerDatabase := os.Getenv("SPANNER_DATABASE")
	fmt.Printf("Env SPANNER_DATABASE:%s\n", spannerDatabase)

	ctx := context.Background()
	client, err := createClient(ctx, spannerDatabase)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	spannerClient = client

	fmt.Println(m.Run())
}
