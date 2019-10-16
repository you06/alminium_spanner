package main

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"go.opencensus.io/trace"
)

type SmallSizeStore interface {
	TableName() string
	Get(ctx context.Context, id string) (*SmallSize, error)
}

var smallSizeStore SmallSizeStore

func NewSmallSizeStore(sc *spanner.Client) SmallSizeStore {
	if smallSizeStore == nil {
		smallSizeStore = &defaultSmallSizeStore{
			sc: sc,
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
	sc *spanner.Client
}

func (s *defaultSmallSizeStore) TableName() string {
	return "SmallSize"
}

func (s *defaultSmallSizeStore) Get(ctx context.Context, id string) (*SmallSize, error) {
	ctx, span := trace.StartSpan(ctx, "/smallsize/get")
	defer span.End()

	row, err := s.sc.Single().ReadRow(ctx, s.TableName(), spanner.Key{id}, []string{"Id", "Content", "CreatedAt", "UpdatedAt", "CommitedAt"})
	if err != nil {
		return nil, err
	}
	var e SmallSize
	if err := row.ToStruct(&e); err != nil {
		return nil, err
	}
	return &e, nil
}
