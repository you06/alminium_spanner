package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

type JoinBenchmarkStore interface {
	ItemTableName() string
	UserTableName() string
	OrderTableName() string
	OrderTableDetailTableName() string
	InsertItem(ctx context.Context, items []*Item) error
	InsertUser(ctx context.Context, users []*User) error
	InsertOrder(ctx context.Context, orders []*Order, orderDetails []*OrderDetail) error
	SelectSampleItems(ctx context.Context) ([]*Item, error)
	SelectSampleUsers(ctx context.Context) ([]*User, error)
}

var joinBenchmarkStore JoinBenchmarkStore

func NewJoinBenchmarkStore(sc *spanner.Client, itemRows int, userRows int, orderRows int) JoinBenchmarkStore {
	if joinBenchmarkStore == nil {

		joinBenchmarkStore = &defaultJoinBenchmarkStore{
			sc:                   sc,
			itemTableName:        fmt.Sprintf("Item%s", ConvertKMGText(itemRows)),
			userTableName:        fmt.Sprintf("User%s", ConvertKMGText(userRows)),
			orderTableName:       fmt.Sprintf("Order%s", ConvertKMGText(orderRows)),
			orderDetailTableName: fmt.Sprintf("OrderDetail%s", ConvertKMGText(orderRows)),
		}
	}
	return joinBenchmarkStore
}

type Item struct {
	ID             string `spanner:"Id"`
	CategoryID     string `spanner:"CategoryId"`
	Name           string
	Price          int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CommitedAt     time.Time
	ShardCreatedAt int
}

type User struct {
	ID             string `spanner:"Id"`
	Name           string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CommitedAt     time.Time
	ShardCreatedAt int
}

type Order struct {
	ID             string `spanner:"Id"`
	UserID         string `spanner:"UserId"`
	Price          int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CommitedAt     time.Time
	ShardCreatedAt int
}

type OrderDetail struct {
	ID             string `spanner:"Id"`
	OrderID        string `spanner:"OrderId"`
	ItemID         string `spanner:"ItemId"`
	Price          int
	Number         int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CommitedAt     time.Time
	ShardCreatedAt int
}

type defaultJoinBenchmarkStore struct {
	sc                   *spanner.Client
	itemTableName        string
	userTableName        string
	orderTableName       string
	orderDetailTableName string
}

func (s *defaultJoinBenchmarkStore) ItemTableName() string {
	return s.itemTableName
}

func (s *defaultJoinBenchmarkStore) UserTableName() string {
	return s.userTableName
}

func (s *defaultJoinBenchmarkStore) OrderTableName() string {
	return s.orderTableName
}

func (s *defaultJoinBenchmarkStore) OrderTableDetailTableName() string {
	return s.orderDetailTableName
}

func (s *defaultJoinBenchmarkStore) InsertItem(ctx context.Context, items []*Item) error {
	ms := []*spanner.Mutation{}

	for _, item := range items {
		m, err := spanner.InsertStruct(s.ItemTableName(), item)
		if err != nil {
			return errors.WithStack(err)
		}
		ms = append(ms, m)
	}

	_, err := s.sc.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *defaultJoinBenchmarkStore) InsertUser(ctx context.Context, users []*User) error {
	ms := []*spanner.Mutation{}

	for _, user := range users {
		m, err := spanner.InsertStruct(s.UserTableName(), user)
		if err != nil {
			return errors.WithStack(err)
		}
		ms = append(ms, m)
	}

	_, err := s.sc.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *defaultJoinBenchmarkStore) InsertOrder(ctx context.Context, orders []*Order, orderDetails []*OrderDetail) error {
	ms := []*spanner.Mutation{}

	for _, order := range orders {
		m, err := spanner.InsertStruct(s.OrderTableName(), order)
		if err != nil {
			return errors.WithStack(err)
		}
		ms = append(ms, m)
	}
	for _, detail := range orderDetails {
		m, err := spanner.InsertStruct(s.OrderTableDetailTableName(), detail)
		if err != nil {
			return errors.WithStack(err)
		}
		ms = append(ms, m)
	}

	_, err := s.sc.Apply(ctx, ms)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *defaultJoinBenchmarkStore) SelectSampleItems(ctx context.Context) ([]*Item, error) {
	q := fmt.Sprintf("SELECT * FROM %s TABLESAMPLE RESERVOIR (1000 ROWS);", s.ItemTableName())
	iter := s.sc.Single().Query(ctx, spanner.NewStatement(q))
	defer iter.Stop()

	items := []*Item{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var item Item
		row.ToStruct(&item)
		items = append(items, &item)
	}

	return items, nil
}

func (s *defaultJoinBenchmarkStore) SelectSampleUsers(ctx context.Context) ([]*User, error) {
	q := fmt.Sprintf("SELECT * FROM %s TABLESAMPLE RESERVOIR (1000 ROWS);", s.UserTableName())
	iter := s.sc.Single().Query(ctx, spanner.NewStatement(q))
	defer iter.Stop()

	users := []*User{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var user User
		row.ToStruct(&user)
		users = append(users, &user)
	}

	return users, nil
}

func ConvertKMGText(v int) string {
	const G = 1000000000
	const M = 1000000
	const K = 1000

	switch {
	case v >= G:
		return fmt.Sprintf("%dG", v/G)
	case v >= M:
		return fmt.Sprintf("%dM", v/M)
	case v >= K:
		return fmt.Sprintf("%dK", v/K)
	default:
		return fmt.Sprintf("%d", v)
	}
}
