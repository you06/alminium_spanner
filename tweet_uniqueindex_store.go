package main

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/trace"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

// TweetUniqueIndexStore is TweetTable Functions
type TweetUniqueIndexStore interface {
	TableName() string
	Insert(ctx context.Context, tweet *TweetUniqueIndex) error
	Get(ctx context.Context, key spanner.Key) (*TweetUniqueIndex, error)
	Query(ctx context.Context, limit int) ([]*TweetUniqueIndex, error)
}

var tweetUniqueIndexStore TweetUniqueIndexStore

// NewTweetUniqueIndexStore is New TweetUniqueIndexStore
func NewTweetUniqueIndexStore(tc *trace.Client, sc *spanner.Client) TweetUniqueIndexStore {
	if tweetUniqueIndexStore == nil {
		tweetUniqueIndexStore = &defaultTweetUniqueIndexStore{
			tc: tc,
			sc: sc,
		}
	}
	return tweetUniqueIndexStore
}

// TweetUniqueIndex is TweetTable Row
type TweetUniqueIndex struct {
	ID         string `spanner:"Id"`
	TweetID    string `spanner:"TweetId"`
	Author     string
	Content    string
	Favos      []string
	Sort       int
	CreatedAt  time.Time
	UpdatedAt  time.Time
	CommitedAt time.Time
}

type defaultTweetUniqueIndexStore struct {
	tc *trace.Client
	sc *spanner.Client
}

// TableName is return Table Name for Spanner
func (s *defaultTweetUniqueIndexStore) TableName() string {
	return "TweetUniqueIndex"
}

// Insert is Insert to Tweet
func (s *defaultTweetUniqueIndexStore) Insert(ctx context.Context, tweet *TweetUniqueIndex) error {
	ts := s.tc.NewSpan("/tweetUniqueIndex/insert")
	defer ts.Finish()

	m, err := spanner.InsertStruct(s.TableName(), tweet)
	if err != nil {
		return errors.WithStack(err)
	}
	om, err := NewOperationInsertMutation(uuid.New().String(), "INSERT", tweet.ID, s.TableName(), tweet)
	if err != nil {
		return errors.WithStack(err)
	}
	ms := []*spanner.Mutation{
		m,
		om,
	}

	_, err = s.sc.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s defaultTweetUniqueIndexStore) Get(ctx context.Context, key spanner.Key) (*TweetUniqueIndex, error) {
	ts := s.tc.NewSpan("/tweetUniqueIndex/get")
	defer ts.Finish()

	row, err := s.sc.Single().ReadRow(ctx, s.TableName(), key, []string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var tweet TweetUniqueIndex
	row.ToStruct(&tweet)
	return &tweet, nil
}

// Query is Tweet を sort_ascで取得する
func (s *defaultTweetUniqueIndexStore) Query(ctx context.Context, limit int) ([]*TweetUniqueIndex, error) {
	ts := s.tc.NewSpan("/tweetUniqueIndex/query")
	defer ts.Finish()

	iter := s.sc.Single().ReadUsingIndex(ctx, s.TableName(), "sort_asc", spanner.AllKeys(), []string{"Id", "Sort"})
	defer iter.Stop()

	count := 0
	tweets := []*TweetUniqueIndex{}
	for {
		if count >= limit {
			return tweets, nil
		}
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var tweet TweetUniqueIndex
		row.ToStruct(&tweet)
		tweets = append(tweets, &tweet)
		count++
	}

	return tweets, nil
}
