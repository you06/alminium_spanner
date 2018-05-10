package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
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
			Author:     getAuthor(),
			Content:    uuid.New().String(),
			Favos:      getAuthors(),
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

func getAuthor() string {
	c := []string{"gold", "silver", "dia", "ruby", "sapphire"}
	return c[rand.Intn(len(c))]
}

func getAuthors() []string {
	exists := make(map[string]string)

	count := rand.Intn(4)
	for i := 0; i < count; i++ {
		a := getAuthor()
		exists[a] = a
	}

	authors := []string{}
	for k, _ := range exists {
		authors = append(authors, k)
	}
	return authors
}
