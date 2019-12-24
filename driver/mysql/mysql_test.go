package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/sinmetal/alminium_spanner/driver/driver"
)

func TestInsertStruct(t *testing.T) {
	m := MySQL{}
	s := struct {
		ID   int `common:"iD"`
		Name string
		Date time.Time
		Age  int
	}{
		ID: 114514,
		Name: "senpai",
		Date: driver.MustParse("2019-01-09 11:45:14"),
		Age: 24,
	}
	mu, err := m.InsertStruct("shimokitazawa", &s)

	assert.Empty(t, err)
	assert.Equal(t, mu.(*Mutation).stmt, `INSERT INTO shimokitazawa(iD,name,date,age) VALUES(?,?,?,?)`)
}
