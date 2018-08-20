package main

import (
	"testing"
)

func TestNewJoinBenchmarkStore(t *testing.T) {
	candidates := []struct {
		tableNames           string
		itemRows             int
		userRows             int
		orderRows            int
		itemTableName        string
		userTableName        string
		orderTableName       string
		orderDetailTableName string
	}{
		{
			tableNames:           "BenchA",
			itemRows:             1000,
			userRows:             10000,
			orderRows:            100000,
			itemTableName:        "BenchA-Item-1K",
			userTableName:        "BenchA-User-10K",
			orderTableName:       "BenchA-Order-100K",
			orderDetailTableName: "BenchA-OrderDetail-100K",
		},
		{
			tableNames:           "BenchB",
			itemRows:             1000000,
			userRows:             1000000000,
			orderRows:            10000000000,
			itemTableName:        "BenchB-Item-1M",
			userTableName:        "BenchB-User-1G",
			orderTableName:       "BenchB-Order-10G",
			orderDetailTableName: "BenchB-OrderDetail-10G",
		},
	}

	for i, v := range candidates {
		s := NewJoinBenchmarkStore(nil, v.itemRows, v.userRows, v.orderRows)
		if e, g := v.itemTableName, s.ItemTableName(); e != g {
			t.Fatalf("%d : expected ItemTableName is %s; got %s", i, e, g)
		}
		if e, g := v.userTableName, s.UserTableName(); e != g {
			t.Fatalf("%d : expected UserTableName is %s; got %s", i, e, g)
		}
		if e, g := v.orderTableName, s.OrderTableName(); e != g {
			t.Fatalf("%d : expected OrderTableName is %s; got %s", i, e, g)
		}
		if e, g := v.orderDetailTableName, s.OrderTableDetailTableName(); e != g {
			t.Fatalf("%d : expected OrderTableDetailTableName is %s; got %s", i, e, g)
		}
	}
}
