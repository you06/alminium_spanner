package main

import (
	"context"
	"time"
	
	"github.com/pkg/errors"
	"go.opencensus.io/trace"

	"github.com/sinmetal/alminium_spanner/driver/driver"
)

// TweetBenchmarkStore is TweetTable Functions
type TweetBenchmarkStore interface {
	TableName() string
	Insert(ctx context.Context, tweets []*TweetBenchmark) error
}

var tweetBenchmarkStore TweetBenchmarkStore

// NewTweetBenchmarkStore is New TweetBenchmarkStore
func NewTweetBenchmarkStore(client driver.Driver, tableName string) TweetBenchmarkStore {
	if tweetBenchmarkStore == nil {
		tweetBenchmarkStore = &defaultTweetBenchmarkStore{
			client:    client,
			tableName: tableName,
		}
	}
	return tweetBenchmarkStore
}

// TweetBenchmark is TweetTable Row
// Table名はTweet1kのように件数が入ったものになる
type TweetBenchmark struct {
	ID             string `spanner:"Id"`
	Author         string
	Content        string
	Favos          []string
	Sort           int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CommitedAt     time.Time
	ShardCreatedAt int
}

type defaultTweetBenchmarkStore struct {
	client    driver.Driver
	tableName string
}

// TableName is return Table Name for Spanner
func (s *defaultTweetBenchmarkStore) TableName() string {
	return s.tableName
}

// Insert is Insert to Tweet
func (s *defaultTweetBenchmarkStore) Insert(ctx context.Context, tweets []*TweetBenchmark) error {
	ctx, span := trace.StartSpan(ctx, "/tweetbenchmark/store/insert")
	defer span.End()

	ms := []driver.Mutation{}

	for _, tweet := range tweets {
		m, err := s.client.InsertStruct(s.TableName(), tweet)
		if err != nil {
			return errors.WithStack(err)
		}
		ms = append(ms, m)
	}

	_, err := s.client.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
