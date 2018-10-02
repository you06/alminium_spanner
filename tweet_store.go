package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
	"google.golang.org/api/iterator"
)

// TweetStore is TweetTable Functions
type TweetStore interface {
	TableName() string
	Insert(ctx context.Context, tweet *Tweet) error
	Get(ctx context.Context, key spanner.Key) (*Tweet, error)
	Query(ctx context.Context, limit int) ([]*Tweet, error)
	QueryResultStruct(ctx context.Context) ([]*TweetIDAndAuthor, error)
}

var tweetStore TweetStore

// NewTweetStore is New TweetStore
func NewTweetStore(sc *spanner.Client) TweetStore {
	if tweetStore == nil {
		tweetStore = &defaultTweetStore{
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
	sc *spanner.Client
}

// TableName is return Table Name for Spanner
func (s *defaultTweetStore) TableName() string {
	return "Tweet"
}

// Insert is Insert to Tweet
func (s *defaultTweetStore) Insert(ctx context.Context, tweet *Tweet) error {
	wn := getWorkerName(ctx)
	ctx, span := trace.StartSpan(ctx, fmt.Sprintf("/%s/tweet/insert", wn))
	defer span.End()

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
	ctx, span := trace.StartSpan(ctx, "/tweet/get")
	defer span.End()

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
	ctx, span := trace.StartSpan(ctx, "/tweet/query")
	defer span.End()

	iter := s.sc.Single().ReadUsingIndex(ctx, s.TableName(), "TweetSortAsc", spanner.AllKeys(), []string{"Id", "Sort"})
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

// TweetIDAndAuthor is StructのResponseの確認用に作ったStruct
type TweetIDAndAuthor struct {
	ID     string `spanner:"Id"`
	Author string
}

// QueryResultStruct is StructをResultで返すQueryのサンプル
func (s *defaultTweetStore) QueryResultStruct(ctx context.Context) ([]*TweetIDAndAuthor, error) {
	ctx, span := trace.StartSpan(ctx, "/tweet/queryResultStruct")
	defer span.End()

	iter := s.sc.Single().Query(ctx, spanner.NewStatement("SELECT ARRAY(SELECT STRUCT(Id, Author)) As IdWithAuthor FROM Tweet LIMIT 10;"))
	defer iter.Stop()

	type Result struct {
		IDWithAuthor []*TweetIDAndAuthor `spanner:"IdWithAuthor"`
	}

	ias := []*TweetIDAndAuthor{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var result Result
		row.ToStruct(&result)
		ias = append(ias, result.IDWithAuthor[0])
	}

	return ias, nil
}
