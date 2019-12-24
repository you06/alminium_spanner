package spanner

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"

	"github.com/pkg/errors"
	"github.com/sinmetal/alminium_spanner/config"
	"github.com/sinmetal/alminium_spanner/driver/driver"
)

// Spanner struct
type Spanner struct {
	sc *spanner.Client
}

// Snapshot wrap snapshot
type Snapshot struct {
	snapshot *spanner.ReadOnlyTransaction
}

// Transaction wrap ReadWriteTransaction
type Transaction struct {
	txn *spanner.ReadWriteTransaction
}

// Rows wrap spanner.RowIterator
type Rows struct {
	rows *spanner.RowIterator
}

// Row wrap spanner.Row
type Row struct {
	row *spanner.Row
}

// Init create spanner struct
func Init(ctx context.Context, cfg *config.Config) (*Spanner, error) {
	config := spanner.ClientConfig{
		NumChannels: 60,
	}
	c, err := spanner.NewClientWithConfig(ctx, cfg.Database, config)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &Spanner{sc: c}, nil
}

// InsertStruct implement spanner's InsertStruct
func (s *Spanner) InsertStruct (table string, in interface{}) (driver.Mutation, error) {
	return spanner.InsertStruct(table, in)
}

// Update implement spanner's Update
func (s *Spanner) Update(table string, cols []string, vals []interface{}) driver.Mutation {
	return spanner.Update(table, cols, vals)
}

// Apply implement spanner's apply
func (s *Spanner) Apply(ctx context.Context, ms []driver.Mutation) (time.Time, error) {
	return s.sc.Apply(ctx, restoreMutation(ms))
}

// Single provide read-only snapshot
// implement spanner's Single function
func (s *Spanner) Single() driver.Snapshot {
	return &Snapshot{snapshot: s.sc.Single()}
}

// Snapshot alias Single
func (s *Spanner) Snapshot() driver.Snapshot {
	return s.Single()
}

// ReadWriteTransaction implement spanner's ReadWriteTransaction
func (s *Spanner) ReadWriteTransaction(ctx context.Context, f func(context.Context, driver.Transaction) error) (time.Time, error) {
	return s.sc.ReadWriteTransaction(ctx, func(c context.Context, txn *spanner.ReadWriteTransaction) error {
		return f(c, Transaction{txn})
	})
}

// ReadRow implement spanner's ReadRow function
func (s *Snapshot) ReadRow(ctx context.Context, table string, key interface{}, indexes, columns []string) (driver.Row, error) {
	row, err := s.snapshot.ReadRow(ctx, table, key.(spanner.Key), columns)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Row{row: row}, nil
}

// ReadUsingIndex implement spanner's ReadUsingIndex function
func (s *Snapshot) ReadUsingIndex(ctx context.Context, table, index string, key interface{}, indexes, columns []string) driver.Rows {
	return &Rows{rows: s.snapshot.ReadUsingIndex(ctx, table, index, key.(spanner.KeySet), columns)}
}

// Query implement spanner's Query function
func (s *Snapshot) Query(ctx context.Context, stmt string, args ...interface{}) driver.Rows {
	statement := spanner.NewStatement(stmt)
	return &Rows{rows: s.snapshot.Query(ctx, statement)}
}

// ReadRow implement spanner's ReadRow function
func (t Transaction) ReadRow(ctx context.Context, table string, key interface{}, indexes, columns []string) (driver.Row, error) {
	row, err := t.txn.ReadRow(ctx, table, key.(spanner.Key), columns)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Row{row: row}, nil
}

// ReadUsingIndex implement spanner's ReadUsingIndex function
func (t Transaction) ReadUsingIndex(ctx context.Context, table, index string, key interface{}, indexes, columns []string) driver.Rows {
	return &Rows{rows: t.txn.ReadUsingIndex(ctx, table, index, key.(spanner.KeySet), columns)}
}

// BufferWrite implement spanner's BufferWrite function
func (t Transaction) BufferWrite(ms []driver.Mutation) error {
	return t.txn.BufferWrite(restoreMutation(ms))
}

// Query implement spanner's Query function
func (t Transaction) Query(ctx context.Context, stmt string, args ...interface{}) driver.Rows {
	statement := spanner.NewStatement(stmt)
	return &Rows{rows: t.txn.Query(ctx, statement)}
}

// Next implement Next
func (r *Rows) Next() (driver.Row, error) {
	row, err := r.rows.Next()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Row{row: row}, nil
}

// Stop implement Stop
func (r *Rows) Stop() {
	r.rows.Stop()
}

// Done get Done error
func (r *Rows) Done() error {
	return iterator.Done
}

// ColumnByName implement spanner's ColumnByName function
func (r *Row) ColumnByName(name string, ptr interface{}) error {
	return r.row.ColumnByName(name, ptr)
}

// ToStruct implement spanner's ToStruct function
func (r *Row) ToStruct(p interface{}) error {
	return r.row.ToStruct(p)
}

func restoreMutation(mutations []driver.Mutation) []*spanner.Mutation {
	ms := make([]*spanner.Mutation, len(mutations))
	for i, m := range mutations {
		ms[i] = m.(*spanner.Mutation)
	}
	return ms
}
