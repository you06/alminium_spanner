package main

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/profiler"
	"cloud.google.com/go/spanner"
	sadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/trace"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func main() {
	spannerProject := os.Getenv("SPANNER_PROJECT")
	fmt.Printf("Env SPANNER_PROJECT:%s\n", spannerProject)

	spannerInstance := os.Getenv("SPANNER_INSTANCE")
	fmt.Printf("Env SPANNER_INSTANCE:%s\n", spannerInstance)

	spannerDatabase := os.Getenv("SPANNER_DATABASE")
	fmt.Printf("Env SPANNER_DATABASE:%s\n", spannerDatabase)

	stackdriverProject := os.Getenv("STACKDRIVER_PROJECT")
	fmt.Printf("Env STACKDRIVER_PROJECT:%s\n", stackdriverProject)

	runWorks := os.Getenv("RUN_WORKS")
	fmt.Printf("Env RUN_WORKS:%s\n", runWorks)
	wm := newWorkManager(runWorks)

	benchmarkDatabaseName := os.Getenv("BENCHMARK_DATABASE_NAME")
	fmt.Printf("Env BENCHMARK_DATABASE_NAME:%s\n", benchmarkDatabaseName)

	benchmarkTableName := os.Getenv("BENCHMARK_TABLE_NAME")
	fmt.Printf("Env BENCHMARK_TABLE_NAME:%s\n", benchmarkTableName)

	benchmarkCountParam := os.Getenv("BENCHMARK_COUNT")
	fmt.Printf("Env BENCHMARK_COUNT:%s\n", benchmarkCountParam)
	var benchmarkCount int
	var err error
	if benchmarkCountParam != "" {
		benchmarkCount, err = strconv.Atoi(benchmarkCountParam)
		if err != nil {
			panic(err)
		}
	}

	benchmarkItemCountParam := os.Getenv("BENCHMARK_ITEM_COUNT")
	fmt.Printf("Env BENCHMARK_ITEM_COUNT:%s\n", benchmarkItemCountParam)
	var benchmarkItemCount int
	if benchmarkItemCountParam != "" {
		benchmarkItemCount, err = strconv.Atoi(benchmarkItemCountParam)
		if err != nil {
			panic(err)
		}
	}

	benchmarkUserCountParam := os.Getenv("BENCHMARK_USER_COUNT")
	fmt.Printf("Env BENCHMARK_USER_COUNT:%s\n", benchmarkUserCountParam)
	var benchmarkUserCount int
	if benchmarkUserCountParam != "" {
		benchmarkUserCount, err = strconv.Atoi(benchmarkUserCountParam)
		if err != nil {
			panic(err)
		}
	}

	benchmarkOrderCountParam := os.Getenv("BENCHMARK_ORDER_COUNT")
	fmt.Printf("Env BENCHMARK_ORDER_COUNT:%s\n", benchmarkOrderCountParam)
	var benchmarkOrderCount int
	if benchmarkOrderCountParam != "" {
		benchmarkOrderCount, err = strconv.Atoi(benchmarkOrderCountParam)
		if err != nil {
			panic(err)
		}
	}

	// Profiler initialization, best done as early as possible.
	if err := profiler.Start(profiler.Config{ProjectID: stackdriverProject, Service: "alminium_spanner", ServiceVersion: "0.0.1"}); err != nil {
		panic(err)
	}

	ctx := context.Background()

	tc, err := trace.NewClient(ctx, stackdriverProject)
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
	tcs := NewTweetCompositeKeyStore(tc, sc)
	ths := NewTweetHashKeyStore(tc, sc)
	tus := NewTweetUniqueIndexStore(tc, sc)
	tbs := NewTweetBenchmarkStore(tc, sc, benchmarkTableName)

	endCh := make(chan error)

	if wm.isRunWork("InsertBenchmarkTweet") && benchmarkCount > 0 {
		goInsertBenchmarkTweet(tbs, benchmarkCount, endCh)
	}
	if wm.isRunWork("InsertTweet") {
		goInsertTweet(ts, endCh)
	}
	if wm.isRunWork("InsertTweetCompositeKey") {
		goInsertTweetCompositeKey(tcs, endCh)
	}
	if wm.isRunWork("InsertTweetHashKey") {
		goInsertTweetHashKey(ths, endCh)
	}
	if wm.isRunWork("InsertTweetUniqueIndex") {
		goInsertTweetUniqueIndex(tus, endCh)
	}
	if wm.isRunWork("ListTweet") {
		goListTweet(ts, endCh)
	}
	if wm.isRunWork("ListTweetResultStruct") {
		goListTweetResultStruct(ts, endCh)
	}
	if wm.isRunWork("InsertBenchmarkJoinData") {
		sc, err := createClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProject, spannerInstance, benchmarkDatabaseName), o)
		if err != nil {
			panic(err)
		}

		sac, err := createDatabaseAdminClient(ctx)
		if err != nil {
			panic(err)
		}
		jbac := NewJoinBenchmarkAdminClient(sac)

		jbs := NewJoinBenchmarkStore(sc, benchmarkItemCount, benchmarkUserCount, benchmarkOrderCount)
		if err := jbac.CreateJoinBenchmarkTables(ctx, spannerProject, spannerInstance, benchmarkDatabaseName, jbs.ItemTableName(), jbs.UserTableName(), jbs.OrderTableName(), jbs.OrderTableDetailTableName()); err != nil {
			panic(err)
		}

		go func() {
			GoInsertBenchmarkJoinData(jbs, benchmarkItemCount, benchmarkUserCount, benchmarkOrderCount, endCh)
		}()
	}

	err = <-endCh
	fmt.Printf("%+v", err)
}

func createClient(ctx context.Context, db string, o ...option.ClientOption) (*spanner.Client, error) {
	dataClient, err := spanner.NewClient(ctx, db, o...)
	if err != nil {
		return nil, err
	}

	return dataClient, nil
}

func createDatabaseAdminClient(ctx context.Context, o ...option.ClientOption) (*sadmin.DatabaseAdminClient, error) {
	c, err := sadmin.NewDatabaseAdminClient(ctx, o...)
	if err != nil {
		return nil, err
	}

	return c, nil
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

type workManager struct {
	works []string
}

func newWorkManager(runWorks string) *workManager {
	if runWorks == "" {
		return &workManager{
			works: []string{},
		}
	}
	works := strings.Split(runWorks, ",")
	return &workManager{
		works: works,
	}
}

func (m *workManager) isRunWork(work string) bool {
	if len(m.works) == 0 {
		return true
	}
	for _, w := range m.works {
		if w == work {
			return true
		}
	}
	return false
}

func goInsertBenchmarkTweet(tbs TweetBenchmarkStore, count int, endCh chan<- error) {
	go func() {
		ts := []*TweetBenchmark{}
		for i := 0; i < count; i++ {
			now := time.Now()
			shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10
			ctx := context.Background()
			id := uuid.New().String()
			t := &TweetBenchmark{
				ID:             id,
				Author:         getAuthor(),
				Content:        uuid.New().String(),
				Favos:          getAuthors(),
				Sort:           rand.Int(),
				CreatedAt:      now,
				UpdatedAt:      now,
				CommitedAt:     spanner.CommitTimestamp,
				ShardCreatedAt: int(shardId),
			}
			ts = append(ts, t)
			if len(ts) >= 1000 {
				if err := tbs.Insert(ctx, ts); err != nil {
					endCh <- err
				}
				ts = []*TweetBenchmark{}
			}

			if i%1000 == 0 {
				fmt.Printf("TWEET_BENCHMARK_INSERT INDEX = %d, ID = %s\n", i, id)
			}
		}
		endCh <- errors.New("DONE")
	}()
}

func goInsertTweet(ts TweetStore, endCh chan<- error) {
	go func() {
		for {
			ctx := context.Background()
			id := uuid.New().String()
			if err := ts.Insert(ctx, &Tweet{
				ID:         id,
				Author:     getAuthor(),
				Content:    uuid.New().String(),
				Favos:      getAuthors(),
				Sort:       rand.Int(),
				CreatedAt:  time.Now(),
				UpdatedAt:  time.Now(),
				CommitedAt: spanner.CommitTimestamp,
			}); err != nil {
				endCh <- err
			}
			fmt.Printf("TWEET_INSERT ID = %s\n", id)
		}
	}()
}

func goInsertTweetCompositeKey(tcs TweetCompositeKeyStore, endCh chan<- error) {
	go func() {
		for {
			ctx := context.Background()
			id := uuid.New().String()
			tweet := &TweetCompositeKey{
				ID:         id,
				Author:     getAuthor(),
				Content:    uuid.New().String(),
				Favos:      getAuthors(),
				Sort:       rand.Int(),
				CreatedAt:  time.Now(),
				UpdatedAt:  time.Now(),
				CommitedAt: spanner.CommitTimestamp,
			}
			if err := tcs.Insert(ctx, tweet); err != nil {
				endCh <- err
			}
			fmt.Printf("TWEET_COMPOSITEKEY_INSERT ID = %s, Author = %s\n", id, tweet.Author)
		}
	}()
}

func goInsertTweetHashKey(ths TweetHashKeyStore, endCh chan<- error) {
	go func() {
		for {
			ctx := context.Background()
			author := getAuthor()
			id := ths.NewKey(uuid.New().String(), author)
			tweet := &TweetHashKey{
				ID:         id,
				Author:     getAuthor(),
				Content:    uuid.New().String(),
				Favos:      getAuthors(),
				Sort:       rand.Int(),
				CreatedAt:  time.Now(),
				UpdatedAt:  time.Now(),
				CommitedAt: spanner.CommitTimestamp,
			}
			if err := ths.Insert(ctx, tweet); err != nil {
				endCh <- err
			}
			fmt.Printf("TWEET_HASHKEY_INSERT ID = %s\n", id)
		}
	}()
}

func goInsertTweetUniqueIndex(tus TweetUniqueIndexStore, endCh chan<- error) {
	go func() {
		for {
			ctx := context.Background()
			id := uuid.New().String()
			tweet := &TweetUniqueIndex{
				ID:         id,
				TweetID:    uuid.New().String(),
				Author:     getAuthor(),
				Content:    uuid.New().String(),
				Favos:      getAuthors(),
				Sort:       rand.Int(),
				CreatedAt:  time.Now(),
				UpdatedAt:  time.Now(),
				CommitedAt: spanner.CommitTimestamp,
			}
			if err := tus.Insert(ctx, tweet); err != nil {
				endCh <- err
			}
			fmt.Printf("TWEET_UNIQUEINDEX_INSERT ID = %s\n", id)
		}
	}()
}

func goListTweet(ts TweetStore, endCh chan<- error) {
	go func() {
		for {
			ctx := context.Background()
			tl, err := ts.Query(ctx, 50)
			if err != nil {
				endCh <- err
			}
			fmt.Printf("TWEET_LIST Length %d\n", len(tl))

			for _, v := range tl {
				_, err := ts.Get(ctx, spanner.Key{v.ID})
				if err != nil {
					endCh <- err
				}
				fmt.Printf("TWEET_GET ID = %s\n", v.ID)
			}
		}
	}()
}

func goListTweetResultStruct(ts TweetStore, endCh chan<- error) {
	go func() {
		for {
			ctx := context.Background()
			l, err := ts.QueryResultStruct(ctx)
			if err != nil {
				endCh <- err
			}
			for _, v := range l {
				fmt.Printf("TWEET_ID_AUTHOR = %+v\n", v)
			}
		}
	}()
}
