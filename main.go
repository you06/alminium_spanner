package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/trace"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func main() {
	fmt.Printf("Env HELLO:%s\n", os.Getenv("HELLO"))

	spannerDatabase := os.Getenv("SPANNER_DATABASE")
	fmt.Printf("Env SPANNER_DATABASE:%s\n", spannerDatabase)

	cloudTraceProject := os.Getenv("CLOUD_TRACE_PROJECT")
	fmt.Printf("Env CLOUD_TRACE_PROJECT:%s\n", cloudTraceProject)

	ctx := context.Background()

	tc, err := trace.NewClient(ctx, cloudTraceProject)
	if err != nil {
		panic(err)
	}
	do := grpc.WithUnaryInterceptor(tc.GRPCClientInterceptor())
	o := option.WithGRPCDialOption(do)

	sc, err := createClient(ctx, spannerDatabase, o)
	if err != nil {
		panic(err)
	}

	ts := NewTweetStore(tc, sc)

	for {
		ctx := context.Background()
		id := uuid.New().String()
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
		fmt.Printf("TWEET_INSERT ID = %s\n", id)
	}
}

func createClient(ctx context.Context, db string, o ...option.ClientOption) (*spanner.Client, error) {
	dataClient, err := spanner.NewClient(ctx, db, o...)
	if err != nil {
		return nil, err
	}

	return dataClient, nil
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
