package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
	"google.golang.org/api/iterator"
)

// TweetCompositeKeyStore is TweetTable Functions
type TweetCompositeKeyStore interface {
	TableName() string
	Insert(ctx context.Context, tweet *TweetCompositeKey) error
	Get(ctx context.Context, key spanner.Key) (*TweetCompositeKey, error)
	Query(ctx context.Context, limit int) ([]*TweetCompositeKey, error)
}

var tweetCompositeKeyStore TweetCompositeKeyStore

// NewTweetCompositeKeyStore is New TweetStore
func NewTweetCompositeKeyStore(sc *spanner.Client) TweetCompositeKeyStore {
	if tweetCompositeKeyStore == nil {
		tweetCompositeKeyStore = &defaultTweetCompositeKeyStore{
			sc: sc,
		}
	}
	return tweetCompositeKeyStore
}

// TweetCompositeKey is TweetTable Row
type TweetCompositeKey struct {
	ID         string `spanner:"Id"`
	Author     string
	Content    string
	Favos      []string
	Sort       int
	CreatedAt  time.Time
	UpdatedAt  time.Time
	CommitedAt time.Time
}

type defaultTweetCompositeKeyStore struct {
	sc *spanner.Client
}

// TableName is return Table Name for Spanner
func (s *defaultTweetCompositeKeyStore) TableName() string {
	return "TweetCompositeKey"
}

// Insert is Insert to Tweet
func (s *defaultTweetCompositeKeyStore) Insert(ctx context.Context, tweet *TweetCompositeKey) error {
	wn := getWorkerName(ctx)
	ctx, span := trace.StartSpan(ctx, fmt.Sprintf("/%s/tweetCompositeKey/insert", wn))
	defer span.End()

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

func (s defaultTweetCompositeKeyStore) Get(ctx context.Context, key spanner.Key) (*TweetCompositeKey, error) {
	wn := getWorkerName(ctx)
	ctx, span := trace.StartSpan(ctx, fmt.Sprintf("/%s/tweetCompositeKey/get", wn))
	defer span.End()

	row, err := s.sc.Single().ReadRow(ctx, s.TableName(), key, []string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var tweet TweetCompositeKey
	row.ToStruct(&tweet)
	return &tweet, nil
}

// Query is Tweet を sort_ascで取得する
func (s *defaultTweetCompositeKeyStore) Query(ctx context.Context, limit int) ([]*TweetCompositeKey, error) {
	ctx, span := trace.StartSpan(ctx, "/tweetCompositeKey/query")
	defer span.End()

	iter := s.sc.Single().ReadUsingIndex(ctx, s.TableName(), "sort_asc", spanner.AllKeys(), []string{"Id", "Sort"})
	defer iter.Stop()

	count := 0
	tweets := []*TweetCompositeKey{}
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

		var tweet TweetCompositeKey
		row.ToStruct(&tweet)
		tweets = append(tweets, &tweet)
		count++
	}

	return tweets, nil
}
