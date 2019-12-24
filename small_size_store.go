package main

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"go.opencensus.io/trace"

	"github.com/sinmetal/alminium_spanner/driver/driver"
)

type SmallSizeStore interface {
	TableName() string
	Get(ctx context.Context, id string) (*SmallSize, error)
	GetIndexes() []string
}

var smallSizeStore SmallSizeStore

func NewSmallSizeStore(client driver.Driver) SmallSizeStore {
	if smallSizeStore == nil {
		smallSizeStore = &defaultSmallSizeStore{
			client: client,
		}
	}
	return smallSizeStore
}

type SmallSize struct {
	ID         string `spanner:"Id"`
	Content    string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	CommitedAt time.Time
}

type defaultSmallSizeStore struct {
	client driver.Driver
}

func (s *defaultSmallSizeStore) TableName() string {
	return "SmallSize"
}

func (s *defaultSmallSizeStore) Get(ctx context.Context, id string) (*SmallSize, error) {
	ctx, span := trace.StartSpan(ctx, "/smallsize/get")
	defer span.End()

	row, err := s.client.Single().ReadRow(ctx, s.TableName(), spanner.Key{id}, s.GetIndexes(), []string{"Id", "Content", "CreatedAt", "UpdatedAt", "CommitedAt"})
	if err != nil {
		return nil, err
	}
	var e SmallSize
	if err := row.ToStruct(&e); err != nil {
		return nil, err
	}
	return &e, nil
}

// GetIndexes return index string slice for mysql usage
func (s *defaultSmallSizeStore) GetIndexes() []string {
	return []string{"Id"}
}

