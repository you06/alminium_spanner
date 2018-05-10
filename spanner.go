package main

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
)

// TweetStore is TweetTable Functions
type TweetStore interface {
	TableName() string
	Insert(ctx context.Context, tweet *Tweet) error
}

var tweetStore TweetStore

// NewTweetStore is New TweetStore
func NewTweetStore(client *spanner.Client) TweetStore {
	if tweetStore == nil {
		tweetStore = &defaultTweetStore{
			client,
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
	*spanner.Client
}

// TableName is return Table Name for Spanner
func (s *defaultTweetStore) TableName() string {
	return "Tweet"
}

// Insert is Insert to Tweet
func (s *defaultTweetStore) Insert(ctx context.Context, tweet *Tweet) error {
	m, err := spanner.InsertStruct(s.TableName(), tweet)
	if err != nil {
		return errors.WithStack(err)
	}
	ms := []*spanner.Mutation{
		m,
	}

	_, err = s.Client.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
