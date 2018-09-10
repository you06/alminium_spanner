package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
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

func GoInsertBenchmarkJoinData(jbs JoinBenchmarkStore, countItems int, countUsers int, countOrders int, endCh chan<- error) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		rows := []*User{}
		for i := 0; i < countUsers; i++ {
			now := time.Now()
			shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10
			ctx := context.Background()
			id := uuid.New().String()
			user := &User{
				UserID:         id,
				Name:           id,
				CreatedAt:      now,
				UpdatedAt:      now,
				CommitedAt:     spanner.CommitTimestamp,
				ShardCreatedAt: int(shardId),
			}
			rows = append(rows, user)
			if len(rows) >= 1000 {
				if err := jbs.InsertUser(ctx, rows); err != nil {
					fmt.Printf("%+v", err)
					return
				}
				fmt.Printf("JOIN_USER_INSERT INDEX = %d, ID = %s\n", i, id)
				rows = []*User{}
			}
		}
		fmt.Printf("DONE USER_INSERT\n")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		users := []*User{}
		for {
			time.Sleep(10 * time.Second)

			ctx := context.Background()
			var err error
			users, err = jbs.SelectSampleUsers(ctx)
			if err != nil {
				fmt.Printf("%+v", err)
				return
			}
			if len(users) > 0 {
				break
			}
		}

		rows := []*Item{}
		for i := 0; i < countItems; i++ {
			favUserIds := []string{}
			favUserNumber := rand.Intn(20)
			for favUserIdCount := 0; favUserIdCount < favUserNumber; favUserIdCount++ {
				userIndex := rand.Intn(len(users) - 1)
				favUserIds = append(favUserIds, users[userIndex].UserID)
			}

			now := time.Now()
			shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10
			ctx := context.Background()
			id := uuid.New().String()
			item := &Item{
				ItemID:         id,
				CategoryID:     GetCategoryId(),
				Name:           id,
				Price:          100 + rand.Int63n(10000),
				FavUserIDs:     favUserIds,
				CreatedAt:      now,
				UpdatedAt:      now,
				CommitedAt:     spanner.CommitTimestamp,
				ShardCreatedAt: int64(shardId),
			}
			rows = append(rows, item)
			if len(rows) >= 1000 {
				if err := jbs.InsertItem(ctx, rows); err != nil {
					fmt.Printf("%+v", err)
					return
				}
				fmt.Printf("JOIN_ITEM_INSERT INDEX = %d, ID = %s\n", i, id)
				rows = []*Item{}
			}
		}
		fmt.Printf("DONE ITEM_INSERT\n")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		orders := []*Order{}
		orderDetails := []*OrderDetail{}
		users := []*User{}
		items := []*Item{}

		for {
			time.Sleep(10 * time.Second)

			ctx := context.Background()
			var err error
			users, err = jbs.SelectSampleUsers(ctx)
			if err != nil {
				fmt.Printf("%+v", err)
				return
			}
			items, err = jbs.SelectSampleItems(ctx)
			if err != nil {
				fmt.Printf("%+v", err)
				return
			}
			if len(users) > 0 && len(items) > 0 {
				break
			}
		}
		for i := 0; i < countOrders; i++ {
			now := time.Now()
			shardId := crc32.ChecksumIEEE([]byte(now.String())) % 10
			ctx := context.Background()
			oid := uuid.New().String()

			detailCount := 1 + rand.Intn(25)
			orderPrice := int64(0)
			for dc := 0; dc < detailCount; dc++ {
				favUserIds := []string{}
				favUserNumber := rand.Intn(20)
				for favUserIdCount := 0; favUserIdCount < favUserNumber; favUserIdCount++ {
					userIndex := rand.Intn(len(users) - 1)
					favUserIds = append(favUserIds, users[userIndex].UserID)
				}

				did := uuid.New().String()
				itemIndex := rand.Intn(len(items) - 1)
				orderDetail := &OrderDetail{
					OrderDetailID:  did,
					OrderID:        oid,
					ItemID:         items[itemIndex].ItemID,
					ItemCategoryID: items[itemIndex].CategoryID,
					Price:          items[itemIndex].Price,
					Number:         1 + rand.Int63n(10),
					FavUserIDs:     favUserIds,
					CreatedAt:      now,
					UpdatedAt:      now,
					CommitedAt:     spanner.CommitTimestamp,
					ShardCreatedAt: int(shardId),
				}
				orderPrice += orderDetail.Price * orderDetail.Number
				orderDetails = append(orderDetails, orderDetail)
			}

			order := &Order{
				OrderID:        oid,
				UserID:         users[rand.Intn(len(users)-1)].UserID,
				Price:          orderPrice,
				CreatedAt:      now,
				UpdatedAt:      now,
				CommitedAt:     spanner.CommitTimestamp,
				ShardCreatedAt: int(shardId),
			}
			orders = append(orders, order)

			if len(orders) >= 50 {
				fmt.Printf("order.len:%d, orderDetail.len:%d\n", len(orders), len(orderDetails))
				if err := jbs.InsertOrder(ctx, orders, orderDetails); err != nil {
					fmt.Printf("%+v", err)
					return
				}
				fmt.Printf("JOIN_ORDER_INSERT INDEX = %d\n", i)
				orders = []*Order{}
				orderDetails = []*OrderDetail{}
			}
			if i%100000 == 0 {
				var err error
				users, err = jbs.SelectSampleUsers(ctx)
				if err != nil {
					fmt.Printf("%+v", err)
					return
				}
				items, err = jbs.SelectSampleItems(ctx)
				if err != nil {
					fmt.Printf("%+v", err)
					return
				}
			}
		}
		fmt.Printf("DONE ORDER_INSERT\n")
	}()

	wg.Wait()
	fmt.Printf("DONE BENCHMARK_JOIN_INSERT\n")
	endCh <- errors.New("DONE")
}

type Item struct {
	ItemID         string `spanner:"ItemId"`
	CategoryID     string `spanner:"CategoryId"`
	Name           string
	Price          int64
	FavUserIDs     []string `spanner:"FavUserIds"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CommitedAt     time.Time
	ShardCreatedAt int64
}

type User struct {
	UserID         string `spanner:"UserId"`
	Name           string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CommitedAt     time.Time
	ShardCreatedAt int
}

type Order struct {
	OrderID        string `spanner:"OrderId"`
	UserID         string `spanner:"UserId"`
	Price          int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CommitedAt     time.Time
	ShardCreatedAt int
}

type OrderDetail struct {
	OrderDetailID  string `spanner:"OrderDetailId"`
	OrderID        string `spanner:"OrderId"`
	ItemID         string `spanner:"ItemId"`
	ItemCategoryID string `spanner:"ItemCategoryId"`
	Price          int64
	Number         int64
	FavUserIDs     []string `spanner:"FavUserIds"`
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
	ctx, span := trace.StartSpan(ctx, "/joinBenchmarkStore/user/insert")
	defer span.End()

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
	ctx, span := trace.StartSpan(ctx, "/joinBenchmarkStore/order/insert")
	defer span.End()

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
	ctx, span := trace.StartSpan(ctx, "/joinBenchmarkStore/selectSampleItems")
	defer span.End()

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
		fmt.Printf("%+v\n", item)
		items = append(items, &item)
	}

	return items, nil
}

func (s *defaultJoinBenchmarkStore) SelectSampleUsers(ctx context.Context) ([]*User, error) {
	ctx, span := trace.StartSpan(ctx, "/joinBenchmarkStore/selectSampleUsers")
	defer span.End()

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

func GetCategoryId() string {
	c := []string{"PaaS", "IaaS", "mBaaS", "FaaS", "DBaaS"}
	return c[rand.Intn(len(c))]
}
