package main

import (
	"encoding/json"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/pkg/errors"
	"github.com/sinmetal/alminium_spanner/driver/driver"
)

// OperationTableName is Operation Table Name
const OperationTableName = "Operation"

// Operation is Spanner Operation
type Operation struct {
	ID          string `spanner:"Id"`
	CommitedAt  time.Time
	VERB        string
	TargetKey   string `spanner:"TargetKey"`
	TargetTable string
	Body        []byte
}

// NewOperationInsertMutation is OperationをInsertするMutex
func NewOperationInsertMutation(client driver.Driver, id string, verb string, targetKey string, targetTable string, body interface{}) (driver.Mutation, error) {
	jt, err := json.Marshal(body)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	om, err := client.InsertStruct(OperationTableName, &Operation{
		ID:          id,
		CommitedAt:  spanner.CommitTimestamp,
		VERB:        verb,
		TargetKey:   targetKey,
		TargetTable: targetTable,
		Body:        jt,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return om, nil
}
