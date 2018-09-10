package main

import (
	"context"
	"fmt"

	sadmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type JoinBenchmarkAdminClient struct {
	C *sadmin.DatabaseAdminClient
}

func NewJoinBenchmarkAdminClient(client *sadmin.DatabaseAdminClient) *JoinBenchmarkAdminClient {
	return &JoinBenchmarkAdminClient{
		C: client,
	}
}

func (c *JoinBenchmarkAdminClient) CreateJoinBenchmarkTables(ctx context.Context, project string, instance string, databaseName string, itemTableName string, userTableName string, orderTableName string, orderDetailTableName string) error {

	itemTable := `
		CREATE TABLE %s (
			ItemId STRING(MAX) NOT NULL,
			CategoryId STRING(MAX) NOT NULL,
			Name STRING(MAX) NOT NULL,
			Price INT64 NOT NULL,
            FavUserIds ARRAY<STRING(MAX)> NOT NULL,
			CreatedAt TIMESTAMP NOT NULL,
			UpdatedAt TIMESTAMP NOT NULL,
			CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
			ShardCreatedAt INT64 NOT NULL,
		) PRIMARY KEY (ItemId)`

	itemCategoryId := `
		CREATE INDEX %sCategoryId
		ON %s (
			CategoryId
		)`

	itemShardCreatedAtDesc := `
		CREATE INDEX %sShardCreatedAtDesc
		ON %s (
			ShardCreatedAt,
			CreatedAt DESC
		)`

	itemCreatedAtDesc := `
		CREATE INDEX %sCreatedAtDesc
		ON %s (
			CreatedAt DESC
		)`

	userTable := `
		CREATE TABLE %s (
			UserId STRING(MAX) NOT NULL,
			Name STRING(MAX) NOT NULL,
			CreatedAt TIMESTAMP NOT NULL,
			UpdatedAt TIMESTAMP NOT NULL,
			CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
			ShardCreatedAt INT64 NOT NULL,
		) PRIMARY KEY (UserId)`

	userShardCreatedAtDesc := `
		CREATE INDEX %sShardCreatedAtDesc
		ON %s (
			ShardCreatedAt,
			CreatedAt DESC
		)`

	userCreatedAtDesc := `
		CREATE INDEX %sCreatedAtDesc
		ON %s (
			CreatedAt DESC
		)`

	orderTable := `
		CREATE TABLE %s (
			OrderId STRING(MAX) NOT NULL,
			UserId STRING(MAX) NOT NULL,
			Price INT64 NOT NULL,
			CreatedAt TIMESTAMP NOT NULL,
			UpdatedAt TIMESTAMP NOT NULL,
			CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
			ShardCreatedAt INT64 NOT NULL,
		) PRIMARY KEY (OrderId)`

	orderPriceAsc := `
		CREATE INDEX %sPriceAsc
		ON %s (
			Price
		)`

	orderPriceDesc := `
		CREATE INDEX %sPriceDesc
		ON %s (
			Price DESC
		)`

	orderShardCreatedAtDesc := `
		CREATE INDEX %sShardCreatedAtDesc
		ON %s (
			ShardCreatedAt,
			CreatedAt DESC
		)`

	orderCreatedAtDesc := `
		CREATE INDEX %sCreatedAtDesc
		ON %s (
			CreatedAt DESC
		)`

	orderDetailTable := `
		CREATE TABLE %s (
			OrderDetailId STRING(MAX) NOT NULL,
			OrderId STRING(MAX) NOT NULL,
			CreatedAt TIMESTAMP NOT NULL,
			UpdatedAt TIMESTAMP NOT NULL,
			CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
			ShardCreatedAt INT64 NOT NULL,
			Price INT64 NOT NULL,
			ItemId STRING(MAX) NOT NULL,
            ItemCategoryId STRING(MAX) NOT NULL,
			Number INT64 NOT NULL,
            FavUserIds ARRAY<STRING(MAX)> NOT NULL,
		) PRIMARY KEY (OrderId, OrderDetailId),
		  INTERLEAVE IN PARENT %s ON DELETE CASCADE`

	orderDetailItemId := `
		CREATE INDEX %sItemIdAsc
		ON %s (
			ItemId
		)`

	orderDetailShardCreatedAtDesc := `
		CREATE INDEX %sShardCreatedAtDesc
		ON %s (
			ShardCreatedAt,
			CreatedAt DESC
		)`

	orderDetailCreatedAtDesc := `
		CREATE INDEX %sCreatedAtDesc
		ON %s (
			CreatedAt DESC
		)`

	fmt.Printf("%s/%s/%s\n", project, instance, databaseName)

	op, err := c.C.CreateDatabase(ctx, &database.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE %s", databaseName),
		ExtraStatements: []string{
			fmt.Sprintf(itemTable, itemTableName),
			fmt.Sprintf(itemCategoryId, itemTableName, itemTableName),
			fmt.Sprintf(itemShardCreatedAtDesc, itemTableName, itemTableName),
			fmt.Sprintf(itemCreatedAtDesc, itemTableName, itemTableName),
			fmt.Sprintf(userTable, userTableName),
			fmt.Sprintf(userShardCreatedAtDesc, userTableName, userTableName),
			fmt.Sprintf(userCreatedAtDesc, userTableName, userTableName),
			fmt.Sprintf(orderTable, orderTableName),
			fmt.Sprintf(orderPriceAsc, orderTableName, orderTableName),
			fmt.Sprintf(orderPriceDesc, orderTableName, orderTableName),
			fmt.Sprintf(orderShardCreatedAtDesc, orderTableName, orderTableName),
			fmt.Sprintf(orderCreatedAtDesc, orderTableName, orderTableName),
			fmt.Sprintf(orderDetailTable, orderDetailTableName, orderTableName),
			fmt.Sprintf(orderDetailItemId, orderDetailTableName, orderDetailTableName),
			fmt.Sprintf(orderDetailShardCreatedAtDesc, orderDetailTableName, orderDetailTableName),
			fmt.Sprintf(orderDetailCreatedAtDesc, orderDetailTableName, orderDetailTableName),
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}
	return nil
}
