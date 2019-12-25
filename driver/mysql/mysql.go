package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sinmetal/alminium_spanner/config"
	"github.com/sinmetal/alminium_spanner/driver/driver"
)
// MySQL struct
type MySQL struct {
	db *sql.DB
}

// Mutation struct
type Mutation struct {
	stmt string
	args []interface{}
}

// Transaction struct
type Transaction struct {
	tx *sql.Tx
}

// Rows struct
type Rows struct {
	rows *sql.Rows
}

// Row struct
type Row struct {
	rows *sql.Rows
}

var errDone = errors.New("no more items in iterator")

// Init create mysql struct
func Init(ctx context.Context, cfg *config.Config) (driver.Driver, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	db.SetMaxIdleConns(cfg.Concurrency)
	return &MySQL{db: db}, nil
}

// InsertStruct implement insert from given struct
func (m *MySQL) InsertStruct (table string, in interface{}) (driver.Mutation, error) {
	cols, vals, _, err := driver.StructToMutationParams(in)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Mutation{
		stmt: fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", table, strings.Join(cols, ","), makePlaceholder(vals)),
		args: filterVals(vals),
	}, nil
}

// Update insert update statement preparation
func (m *MySQL) Update (table string, cols []string, vals []interface{}) driver.Mutation {
	var (
		k = cols[0]
		v = vals[0]
	)
	cols = cols[1:]
	vals = vals[1:]
	patch := make([]string, len(cols))
	for i, col := range cols {
		patch[i] = fmt.Sprintf("%s=?", col)
	}
	return &Mutation{
		stmt: fmt.Sprintf("UPDATE %s SET %s WHERE %s=?", table, strings.Join(patch, ", "), k),
		args: append(vals, v),
	}
}

// Apply implement the origin commit
func (m *MySQL) Apply(ctx context.Context, mutations []driver.Mutation) (time.Time, error) {
	txn, err := m.db.Begin()
	if err != nil {
		return time.Time{}, errors.WithStack(err)
	}
	for _, m := range mutations {
		mu := m.(*Mutation)
		if _, err := txn.Exec(mu.stmt, mu.args...); err != nil {
			fmt.Printf("%+v\n", mu)
			return time.Time{}, errors.WithStack(err)
		}
	}
	if err := txn.Commit(); err != nil {
		return time.Time{}, errors.WithStack(err)
	}
	return time.Now(), nil
}

// Snapshot implement snapshot by start a transaction
func (m *MySQL) Snapshot() driver.Snapshot {
	tx, err := m.db.Begin()
	// spanner will not lead to error when start a transaction
	// panic here to achive the same hehavior with spanner
	// TODO: find a way to deal with this error
	if err != nil {
		panic(err)
	}
	return &Transaction{tx: tx}
}

// Single alias Snapshot
func (m *MySQL) Single() driver.Snapshot {
	return m.Snapshot()
}

// ReadWriteTransaction start a transaction before function and submit after it
func (m *MySQL) ReadWriteTransaction(ctx context.Context, f func(context.Context, driver.Transaction) error) (time.Time, error) {
	e := driver.RunRetryableNoWrap(ctx, func (c context.Context) error {
		tx, err := m.db.Begin()
		if err != nil {
			return errors.WithStack(err)
		}
		if err := f(ctx, &Transaction{tx: tx}); err != nil {
			return errors.WithStack(err)
		}
		// return nil
		// maybe already commit in function f
		return tx.Commit()
	})
	return time.Time{}, e
}

// Key wrap args into key
func (m *MySQL) Key(args ...interface{}) interface{} {
	var res []interface{}
	for _, arg := range args {
		res = append(res, arg)
	}
	return res
}

// AllKeys implement spanner.AllKeys
func (m *MySQL) AllKeys() interface{} {
	return nil
}

func (t *Transaction) read(ctx context.Context, query string, args []interface{}) (*sql.Rows, error) {
	rows, err := t.tx.Query(query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

// ReadRow implement ReadRow
func (t *Transaction) ReadRow(ctx context.Context, table string, key interface{}, indexes, columns []string) (driver.Row, error) {
	var k []interface{}
	switch key := key.(type) {
	case []interface{}:
		k = key
	case nil:
	default:
		k = []interface{}{key}
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(columns, ","), table, makeCondition(indexes))
	rows, err := t.read(ctx, query, k)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Row{rows: rows}, nil
}

// ReadUsingIndex implement ReadUsingIndex
func (t *Transaction) ReadUsingIndex(ctx context.Context, table, index string, key interface{}, indexes, columns []string) driver.Rows {
	var k []interface{}
	switch key := key.(type) {
	case []interface{}:
		k = key
	default:
		k = []interface{}{key}
	}
	query := fmt.Sprintf("SELECT %s FROM %s USE INDEX(%s) WHERE %s", strings.Join(columns, ","), table, index, makeCondition(indexes))
	rows, err := t.read(ctx, query, k)
	if err != nil {
		panic(err)
	}
	return &Rows{rows: rows}
}

// Query implement Query
func (t *Transaction) Query(ctx context.Context, stmt string, args ...interface{}) driver.Rows {
	rows, err := t.read(ctx, stmt, args)
	if err != nil {
		panic(err)
	}
	return &Rows{rows: rows}
}

// BufferWrite implement BufferWrite
func (t *Transaction) BufferWrite(ms []driver.Mutation) error {
	for _, m := range ms {
		mu := m.(*Mutation)
		if _, err := t.tx.Exec(mu.stmt, mu.args...); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// Next implement Next
func (r *Rows) Next() (driver.Row, error) {
	if !r.rows.Next() {
		return nil, r.Done()
	}
	return &Row{rows: r.rows}, nil
}

// Stop implement Stop
func (r *Rows) Stop() {
	_ = r.rows.Close()
}

// Done get Done error
func (r *Rows) Done() error {
	return errDone
}

// ColumnByName get column by name
func (r *Row) ColumnByName(name string, ptr interface{}) error {
	cols, err := r.rows.Columns()
	if err != nil {
		return errors.WithStack(err)
	}
	if r.rows.Next() {
		acceptor := make([]interface{}, len(cols))
		for i, col := range cols {
			if col == name {
				acceptor[i] = ptr
			} else {
				acceptor[i] = new(interface{})
			}
		}
		r.rows.Scan(acceptor...)
	}
	return nil
}

// ToStruct get query value
func (r *Row) ToStruct(p interface{}) error {
	_, _, ptrs, err := driver.StructToMutationParams(p)
	if err != nil {
		return errors.WithStack(err)
	}
	if r.rows.Next() {
		r.rows.Scan(ptrs...)
	}
	return nil
}

// Stop implement Stop
func (r *Row) Stop() {
	_ = r.rows.Close()
}

func makePlaceholder(vals interface{}) string {
	switch v := vals.(type) {
	case []interface{}:
		var p []string
		for range v {
			p = append(p, "?")
		}
		return strings.Join(p, ",")
	default:
		return "?"
	}
}

func makeCondition(indexes []string) string {
	var cond []string
	for _, index := range indexes {
		cond = append(cond, fmt.Sprintf("%s=?", index))
	}
	return strings.Join(cond, " AND ")
}

func filterVals(vals []interface{}) []interface{} {
	for i, v := range vals {
		switch v := v.(type) {
		case []string, []int:
			vals[i] = driver.Slice2Str(v)
		// case time.Time:
		// 	vals[i] = driver.FormatMySQLTime(v)
		}
	}
	return vals
}
