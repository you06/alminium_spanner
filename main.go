package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"golang.org/x/net/context"
)

func main() {
	fmt.Printf("Env HELLO:%s\n", os.Getenv("HELLO"))

	spannerDatabase := os.Getenv("SPANNER_DATABASE")
	fmt.Printf("Env SPANNER_DATABASE:%s\n", spannerDatabase)

	ctx := context.Background()
	client := createClient(ctx, spannerDatabase)

	ts := NewTweetStore(client)

	for {
		if err := ts.Insert(ctx, &Tweet{
			ID:         uuid.New().String(),
			Author:     "sinmetal",
			Content:    uuid.New().String(),
			Favos:      []string{"vvakame"},
			Sort:       rand.Int(),
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
			CommitedAt: spanner.CommitTimestamp,
		}); err != nil {
			panic(err)
		}
	}
}

func createClient(ctx context.Context, db string) *spanner.Client {
	dataClient, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	return dataClient
}
