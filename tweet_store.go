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

	"github.com/sinmetal/alminium_spanner/driver/driver"
)

// TweetStore is TweetTable Functions
type TweetStore interface {
	TableName() string
	Insert(ctx context.Context, tweet *Tweet) error
	InsertBench(ctx context.Context, id string) error
	Update(ctx context.Context, id string) error
	Get(ctx context.Context, key spanner.Key) (*Tweet, error)
	GetIndexes() []string
	Query(ctx context.Context, limit int) ([]*Tweet, error)
	QueryResultStruct(ctx context.Context) ([]*TweetIDAndAuthor, error)
}

var tweetStore TweetStore

// NewTweetStore is New TweetStore
func NewTweetStore(client driver.Driver) TweetStore {
	if tweetStore == nil {
		tweetStore = &defaultTweetStore{
			client: client,
		}
	}
	return tweetStore
}

// Tweet is TweetTable Row
type Tweet struct {
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

type defaultTweetStore struct {
	client driver.Driver
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

	m, err := s.client.InsertStruct(s.TableName(), tweet)
	if err != nil {
		return errors.WithStack(err)
	}
	ms := []driver.Mutation{
		m,
	}

	_, err = s.client.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s defaultTweetStore) Get(ctx context.Context, key spanner.Key) (*Tweet, error) {
	ctx, span := trace.StartSpan(ctx, "/tweet/get")
	defer span.End()

	row, err := s.client.Single().ReadRow(ctx, s.TableName(), key, s.GetIndexes(), []string{"Author", "CommitedAt", "Content", "CreatedAt", "Favos", "Sort", "UpdatedAt"})
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

	iter := s.client.Single().ReadUsingIndex(ctx, s.TableName(), "TweetSortAsc", s.client.AllKeys(), s.GetIndexes(), []string{"Id", "Sort"})
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

	iter := s.client.Single().Query(ctx, "SELECT ARRAY(SELECT STRUCT(Id, Author)) As IdWithAuthor FROM Tweet LIMIT 10;")
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

func (s *defaultTweetStore) Update(ctx context.Context, id string) error {
	ctx, span := trace.StartSpan(ctx, "/tweet/update")
	defer span.End()

	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn driver.Transaction) error {
		tr, err := txn.ReadRow(ctx, s.TableName(), s.client.Key(id), s.GetIndexes(), []string{"Count"})
		if err != nil {
			return err
		}

		var count int64
		if err := tr.ColumnByName("Count", &count); err != nil {
			return err
		}
		count++
		cols := []string{"Id", "Count", "UpdatedAt", "CommitedAt"}
		tr.Stop()

		r, err := txn.ReadRow(ctx, "TweetDummy2", s.client.Key(id), s.GetIndexes(), []string{"Id"})
		if err != nil {
			return err
		}
		r.Stop()

		return txn.BufferWrite([]driver.Mutation{
			s.client.Update(s.TableName(), cols, []interface{}{id, count, time.Now(), time.Now()}),
		})
	})

	return err
}

func (s *defaultTweetStore) InsertBench(ctx context.Context, id string) error {
	ctx, span := trace.StartSpan(ctx, "/tweet/insertbench")
	defer span.End()

	ml := []driver.Mutation{}
	now := time.Now()

	t := &Tweet{
		ID:         id,
		Content:    id,
		Favos:      []string{},
		CreatedAt:  now,
		UpdatedAt:  now,
		CommitedAt: spanner.CommitTimestamp,
	}
	tm, err := s.client.InsertStruct(s.TableName(), t)
	if err != nil {
		return err
	}
	ml = append(ml, tm)

	tom, err := NewOperationInsertMutation(s.client, uuid.New().String(), "INSERT", "", s.TableName(), t)
	if err != nil {
		return err
	}
	ml = append(ml, tom)

	for i := 1; i < 4; i++ {
		td := &TweetDummy{
			ID:         id,
			Content:    id,
			Favos:      []string{},
			CreatedAt:  now,
			UpdatedAt:  now,
			CommitedAt: spanner.CommitTimestamp,
		}
		tdm, err := s.client.InsertStruct(fmt.Sprintf("TweetDummy%d", i), td)
		if err != nil {
			return err
		}
		ml = append(ml, tdm)
	}
	_, err = s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn driver.Transaction) error {
		return txn.BufferWrite(ml)
	})

	return err
}

// GetIndexes return index string slice for mysql usage
func (s *defaultTweetStore) GetIndexes() []string {
	return []string{"Id"}
}
