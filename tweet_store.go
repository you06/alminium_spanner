package main

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/trace"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

// TweetStore is TweetTable Functions
type TweetStore interface {
	TableName() string
	Insert(ctx context.Context, tweet *Tweet) error
	Get(ctx context.Context, key spanner.Key) (*Tweet, error)
	Query(ctx context.Context, limit int) ([]*Tweet, error)
}

var tweetStore TweetStore

// NewTweetStore is New TweetStore
func NewTweetStore(tc *trace.Client, sc *spanner.Client) TweetStore {
	if tweetStore == nil {
		tweetStore = &defaultTweetStore{
			tc: tc,
			sc: sc,
		}
	}
	return tweetStore
}

// Tweet is TweetTable Row
type Tweet struct {
	ID         string `spanner:"Id"`
	Author     string
	Content    string
	Favos      []string
	Sort       int
	CreatedAt  time.Time
	UpdatedAt  time.Time
	CommitedAt time.Time
}

type defaultTweetStore struct {
	tc *trace.Client
	sc *spanner.Client
}

// TableName is return Table Name for Spanner
func (s *defaultTweetStore) TableName() string {
	return "Tweet"
}

// Insert is Insert to Tweet
func (s *defaultTweetStore) Insert(ctx context.Context, tweet *Tweet) error {
	ts := s.tc.NewSpan("/tweet/insert")
	defer ts.Finish()

	m, err := spanner.InsertStruct(s.TableName(), tweet)
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

func (s defaultTweetStore) Get(ctx context.Context, key spanner.Key) (*Tweet, error) {
	ts := s.tc.NewSpan("/tweet/get")
	defer ts.Finish()

	row, err := s.sc.Single().ReadRow(ctx, s.TableName(), key, []string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var tweet Tweet
	row.ToStruct(&tweet)
	return &tweet, nil
}

// Query is Tweet を sort_ascで取得する
func (s *defaultTweetStore) Query(ctx context.Context, limit int) ([]*Tweet, error) {
	ts := s.tc.NewSpan("/tweet/query")
	defer ts.Finish()

	iter := s.sc.Single().ReadUsingIndex(ctx, s.TableName(), "sort_asc", spanner.AllKeys(), []string{"Id", "Sort"})
	defer iter.Stop()

	count := 0
	tweets := []*Tweet{}
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

		var tweet Tweet
		row.ToStruct(&tweet)
		tweets = append(tweets, &tweet)
		count++
	}

	return tweets, nil
}
