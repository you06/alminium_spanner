package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
)

// RunBenchmarkDataCreator is Benchmark用のDataを作成する
// 指定した件数を作成するので、1回だけ動くようになってる
func RunBenchmarkDataCreator(endCh chan<- error) {
	var err error

	spannerProject := os.Getenv("SPANNER_PROJECT")
	fmt.Printf("Env SPANNER_PROJECT:%s\n", spannerProject)

	spannerInstance := os.Getenv("SPANNER_INSTANCE")
	fmt.Printf("Env SPANNER_INSTANCE:%s\n", spannerInstance)

	benchmarkDatabaseName := os.Getenv("BENCHMARK_DATABASE_NAME")
	fmt.Printf("Env BENCHMARK_DATABASE_NAME:%s\n", benchmarkDatabaseName)

	benchmarkItemCountParam := os.Getenv("BENCHMARK_ITEM_COUNT")
	fmt.Printf("Env BENCHMARK_ITEM_COUNT:%s\n", benchmarkItemCountParam)
	var benchmarkItemCount int
	if benchmarkItemCountParam != "" {
		benchmarkItemCount, err = strconv.Atoi(benchmarkItemCountParam)
		if err != nil {
			panic(err)
		}
	}

	benchmarkUserCountParam := os.Getenv("BENCHMARK_USER_COUNT")
	fmt.Printf("Env BENCHMARK_USER_COUNT:%s\n", benchmarkUserCountParam)
	var benchmarkUserCount int
	if benchmarkUserCountParam != "" {
		benchmarkUserCount, err = strconv.Atoi(benchmarkUserCountParam)
		if err != nil {
			panic(err)
		}
	}

	benchmarkOrderCountParam := os.Getenv("BENCHMARK_ORDER_COUNT")
	fmt.Printf("Env BENCHMARK_ORDER_COUNT:%s\n", benchmarkOrderCountParam)
	var benchmarkOrderCount int
	if benchmarkOrderCountParam != "" {
		benchmarkOrderCount, err = strconv.Atoi(benchmarkOrderCountParam)
		if err != nil {
			panic(err)
		}
	}

	ctx := context.Background()
	sc, err := createClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", spannerProject, spannerInstance, benchmarkDatabaseName))
	if err != nil {
		panic(err)
	}

	sac, err := createDatabaseAdminClient(ctx)
	if err != nil {
		panic(err)
	}
	jbac := NewJoinBenchmarkAdminClient(sac)

	jbs := NewJoinBenchmarkStore(sc, benchmarkItemCount, benchmarkUserCount, benchmarkOrderCount)
	if err := jbac.CreateJoinBenchmarkTables(ctx, spannerProject, spannerInstance, benchmarkDatabaseName, jbs.ItemTableName(), jbs.UserTableName(), jbs.OrderTableName(), jbs.OrderTableDetailTableName()); err != nil {
		panic(err)
	}

	go func() {
		GoInsertBenchmarkJoinData(jbs, benchmarkItemCount, benchmarkUserCount, benchmarkOrderCount, endCh)
	}()
}
