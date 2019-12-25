package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
	"google.golang.org/api/iterator"

	"github.com/sinmetal/alminium_spanner/driver/driver"
)

// TweetHashKeyStore is TweetTable Functions
type TweetHashKeyStore interface {
	TableName() string
	NewKey(id string, author string) string
	Insert(ctx context.Context, tweet *TweetHashKey) error
	Get(ctx context.Context, key interface{}) (*TweetHashKey, error)
	Query(ctx context.Context, limit int) ([]*TweetHashKey, error)
	GetIndexes() []string
}

var tweetHashKeyStore TweetHashKeyStore

// NewTweetHashKeyStore is New TweetHashKeyStore
func NewTweetHashKeyStore(client driver.Driver) TweetHashKeyStore {
	if tweetHashKeyStore == nil {
		tweetHashKeyStore = &defaultTweetHashKeyStore{
			client: client,
		}
	}
	return tweetHashKeyStore
}

// TweetHashKey is TweetTable Row
type TweetHashKey struct {
	ID         string `spanner:"Id"`
	Author     string
	Content    string
	Favos      []string
	Sort       int
	CreatedAt  time.Time
	UpdatedAt  time.Time
	CommitedAt time.Time
}

type defaultTweetHashKeyStore struct {
	client driver.Driver
}

// TableName is return Table Name for Spanner
func (s *defaultTweetHashKeyStore) TableName() string {
	return "TweetHashKey"
}

// NewKey is return Table Key
func (s *defaultTweetHashKeyStore) NewKey(id string, author string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s-_-%s", id, author))))
}

// Insert is Insert to Tweet
func (s *defaultTweetHashKeyStore) Insert(ctx context.Context, tweet *TweetHashKey) error {
	ctx, span := trace.StartSpan(ctx, "/tweetHashKey/insert")
	defer span.End()

	m, err := s.client.InsertStruct(s.TableName(), tweet)
	if err != nil {
		return errors.WithStack(err)
	}
	om, err := NewOperationInsertMutation(s.client, uuid.New().String(), "INSERT", tweet.ID, s.TableName(), tweet)
	if err != nil {
		return errors.WithStack(err)
	}
	ms := []driver.Mutation{
		m,
		om,
	}

	_, err = s.client.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s defaultTweetHashKeyStore) Get(ctx context.Context, key interface{}) (*TweetHashKey, error) {
	ctx, span := trace.StartSpan(ctx, "/tweetHashKey/get")
	defer span.End()

	row, err := s.client.Single().ReadRow(ctx, s.TableName(), key, s.GetIndexes(), []string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var tweet TweetHashKey
	row.ToStruct(&tweet)
	return &tweet, nil
}

// Query is Tweet を sort_ascで取得する
func (s *defaultTweetHashKeyStore) Query(ctx context.Context, limit int) ([]*TweetHashKey, error) {
	ctx, span := trace.StartSpan(ctx, "/tweetHashKey/query")
	defer span.End()

	iter := s.client.Single().ReadUsingIndex(ctx, s.TableName(), "TweetHashKeySortAsc", s.client.AllKeys(), s.GetIndexes(), []string{"Id", "Sort"})
	defer iter.Stop()

	count := 0
	tweets := []*TweetHashKey{}
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

		var tweet TweetHashKey
		row.ToStruct(&tweet)
		tweets = append(tweets, &tweet)
		count++
	}

	return tweets, nil
}

// GetIndexes return index string slice for mysql usage
func (s *defaultTweetHashKeyStore) GetIndexes() []string {
	return []string{"Id"}
}
