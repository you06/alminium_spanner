package main

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
)

// TweetBenchmarkStore is TweetTable Functions
type TweetBenchmarkStore interface {
	TableName() string
	Insert(ctx context.Context, tweets []*TweetBenchmark) error
}

var tweetBenchmarkStore TweetBenchmarkStore

// NewTweetBenchmarkStore is New TweetBenchmarkStore
func NewTweetBenchmarkStore(sc *spanner.Client, tableName string) TweetBenchmarkStore {
	if tweetBenchmarkStore == nil {
		tweetBenchmarkStore = &defaultTweetBenchmarkStore{
			sc:        sc,
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
	sc        *spanner.Client
	tableName string
}

// TableName is return Table Name for Spanner
func (s *defaultTweetBenchmarkStore) TableName() string {
	return s.tableName
}

// Insert is Insert to Tweet
func (s *defaultTweetBenchmarkStore) Insert(ctx context.Context, tweets []*TweetBenchmark) error {
	ms := []*spanner.Mutation{}

	for _, tweet := range tweets {
		m, err := spanner.InsertStruct(s.TableName(), tweet)
		if err != nil {
			return errors.WithStack(err)
		}
		ms = append(ms, m)
	}

	_, err := s.sc.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
