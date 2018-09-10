#!/bin/sh

DATE=$(date +%s)
DB="sinmetal-${DATE}"
gcloud spanner databases create $DB --instance=merpay-sponsored-instance --project=gcpug-public-spanner

DDL=$(cat ddl/tweet_hashkey.sql)
gcloud spanner databases ddl update $DB --instance=merpay-sponsored-instance --project=gcpug-public-spanner --ddl="$DDL"
export SPANNER_DATABASE="projects/gcpug-public-spanner/instances/merpay-sponsored-instance/databases/${DB}"
echo $SPANNER_DATABASE
go test ./...
yes | gcloud spanner databases delete $DB --instance=merpay-sponsored-instance --project=gcpug-public-spanner