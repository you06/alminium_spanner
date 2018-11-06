package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"go.opencensus.io/trace"
)

func RunUpdateBenchmarkTweet(ts TweetStore, endCh chan<- error) {
	go func() {
		fmt.Println("Start UpdateTweet")

		var wg sync.WaitGroup
		for i := 0; i < 80; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				for {
					ctx := context.Background()
					id := uuid.New().String()
					fmt.Printf("WORKING... %d:%s\n", i, id)
					workUpdateBenchmarkTweet(ctx, id, ts, endCh)
				}
			}(i)
		}
		fmt.Println("Wait")
		wg.Wait()
		endCh <- errors.New("DONE")
	}()
}

func workUpdateBenchmarkTweet(ctx context.Context, id string, ts TweetStore, endCh chan<- error) {
	ctx, span := trace.StartSpan(ctx, "/tweetupdate/update/work")
	defer span.End()

	now := time.Now()

	t := &Tweet{
		ID:         id,
		Author:     getAuthor(),
		Content:    uuid.New().String(),
		Favos:      getAuthors(),
		Sort:       rand.Int(),
		CreatedAt:  now,
		UpdatedAt:  now,
		CommitedAt: spanner.CommitTimestamp,
	}
	if err := ts.Insert(ctx, t); err != nil {
		endCh <- err
		return
	}

	if err := ts.Update(ctx, id); err != nil {
		endCh <- err
		return
	}

	if err := ts.Update(ctx, id); err != nil {
		endCh <- err
		return
	}

	if err := ts.Update(ctx, id); err != nil {
		endCh <- err
		return
	}
}
