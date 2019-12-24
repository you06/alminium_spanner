package driver

import (
	"context"
	"time"
)

// Mutation interface
type Mutation interface {}

// Snapshot interface
type Snapshot interface {
	ReadRow(ctx context.Context, table string, key interface{}, indexes, columns []string) (Row, error)
	ReadUsingIndex(ctx context.Context, table, index string, key interface{}, indexes, columns []string) Rows
	Query(ctx context.Context, stmt string, args ...interface{}) Rows
}

// Transaction interface
type Transaction interface {
	Snapshot
	BufferWrite(ms []Mutation) error
}

// Row interface
type Row interface {
	ColumnByName(name string, ptr interface{}) error
	ToStruct(p interface{}) error
}

// Rows interface
type Rows interface {
	Next() (Row, error)
	Stop()
	Done() error
}

// Driver interface
type Driver interface {
	InsertStruct(table string, in interface{}) (Mutation, error)
	Update(table string, cols []string, vals []interface{}) Mutation
	Apply(ctx context.Context, ms []Mutation) (time.Time, error)
	Snapshot() Snapshot
	Single() Snapshot
	ReadWriteTransaction(ctx context.Context, f func(context.Context, Transaction) error) (time.Time, error)
}
