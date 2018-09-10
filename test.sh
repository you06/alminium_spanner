#!/bin/sh

DATE=$(date +%s)
DB="sinmetal-${DATE}"
DDL=$(cat ddl/tweet_hashkey.sql)
gcloud spanner databases create $DB --instance=merpay-sponsored-instance --project=gcpug-public-spanner --ddl="$DDL"
export SPANNER_DATABASE="projects/gcpug-public-spanner/instances/merpay-sponsored-instance/databases/${DB}"
echo $SPANNER_DATABASE
go test -v ./...
yes | gcloud spanner databases delete $DB --instance=merpay-sponsored-instance --project=gcpug-public-spanner