package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

// TweetDummyStore is TweetTable Functions
type TweetDummyStore interface {
	TableName(num int) string
	Insert(ctx context.Context, num int, tweet *TweetDummy) error
}

var tweetDummyStore TweetDummyStore

// NewTweetDummyStore is New TweetDummyStore
func NewTweetDummyStore(sc *spanner.Client) TweetDummyStore {
	if tweetDummyStore == nil {
		tweetDummyStore = &defaultTweetDummyStore{
			sc: sc,
		}
	}
	return tweetDummyStore
}

// TweetDummy is TweetTable Row
type TweetDummy struct {
	ID         string `spanner:"Id"`
	Author     string
	Content    string
	Count      int
	Favos      []string
	Sort       int
	CreatedAt  time.Time
	UpdatedAt  time.Time
	CommitedAt time.Time
}

type defaultTweetDummyStore struct {
	sc *spanner.Client
}

// TableName is return Table Name for Spanner
func (s *defaultTweetDummyStore) TableName(num int) string {
	return fmt.Sprintf("Tweet%d", num)
}

// Insert is Insert to Tweet
func (s *defaultTweetDummyStore) Insert(ctx context.Context, num int, tweet *TweetDummy) error {
	wn := getWorkerName(ctx)
	ctx, span := trace.StartSpan(ctx, fmt.Sprintf("/%s/tweetdummy/insert", wn))
	defer span.End()

	m, err := spanner.InsertStruct(s.TableName(num), tweet)
	if err != nil {
		return errors.WithStack(err)
	}
	ms := []*spanner.Mutation{
		m,
	}

	_, err = s.sc.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
