package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"go.opencensus.io/trace"
	"sync"
	"github.com/sinmetal/alminium_spanner/pkg/timer"
)

func RunUpdateBenchmarkTweet(ts TweetStore, goroutine int, endCh chan<- error) {
	go func() {
		fmt.Println("Start UpdateTweet")

		var (
			wg sync.WaitGroup
			ti = timer.New()
		)

		ti.SetAutoCheck(1000)
		for i := 0; i < goroutine; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				for {
					ctx := context.Background()
					id := uuid.New().String()
					// fmt.Printf("WORKING... %d:%s\n", i, id)
					workUpdateBenchmarkTweet(ctx, id, ts, endCh)
					ti.Add()
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

	if err := ts.InsertBench(ctx, id); err != nil {
		endCh <- err
		return
	}

	for i := 0; i < 10; i++ {
		if err := ts.Update(ctx, id); err != nil {
			endCh <- err
			return
		}
	}
}
