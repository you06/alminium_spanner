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
			Id STRING(MAX) NOT NULL,
			CategoryId STRING(MAX) NOT NULL,
			Name STRING(MAX) NOT NULL,
			Price INT64 NOT NULL,
			CreatedAt TIMESTAMP NOT NULL,
			UpdatedAt TIMESTAMP NOT NULL,
			CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
			ShardCreatedAt INT64 NOT NULL,
		) PRIMARY KEY (Id)`

	userTable := `
		CREATE TABLE %s (
			Id STRING(MAX) NOT NULL,
			Name STRING(MAX) NOT NULL,
			CreatedAt TIMESTAMP NOT NULL,
			UpdatedAt TIMESTAMP NOT NULL,
			CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
			ShardCreatedAt INT64 NOT NULL,
		) PRIMARY KEY (Id)`

	orderTable := `
		CREATE TABLE %s (
			Id STRING(MAX) NOT NULL,
			UserId STRING(MAX) NOT NULL,
			Price INT64 NOT NULL,
			CreatedAt TIMESTAMP NOT NULL,
			UpdatedAt TIMESTAMP NOT NULL,
			CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
			ShardCreatedAt INT64 NOT NULL,
		) PRIMARY KEY (Id)`

	orderDetailTable := `
		CREATE TABLE %s (
			Id STRING(MAX) NOT NULL,
			OrderId STRING(MAX) NOT NULL,
			CreatedAt TIMESTAMP NOT NULL,
			UpdatedAt TIMESTAMP NOT NULL,
			CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
			ShardCreatedAt INT64 NOT NULL,
			Price INT64 NOT NULL,
			ItemId STRING(MAX) NOT NULL,
			Number INT64 NOT NULL,
		) PRIMARY KEY (Id)`

	fmt.Printf("%s/%s/%s\n", project, instance, databaseName)
	fmt.Printf(itemTable, itemTableName)
	fmt.Printf(userTable, userTableName)
	fmt.Printf(orderTable, orderTableName)
	fmt.Printf(orderDetailTable, orderDetailTableName)

	op, err := c.C.CreateDatabase(ctx, &database.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE %s", databaseName),
		ExtraStatements: []string{
			fmt.Sprintf(itemTable, itemTableName),
			fmt.Sprintf(userTable, userTableName),
			fmt.Sprintf(orderTable, orderTableName),
			fmt.Sprintf(orderDetailTable, orderDetailTableName),
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
