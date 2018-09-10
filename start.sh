#!/bin/bash

GOOS=linux GOARCH=amd64 go build -o alminium.bin
gsutil mv alminium.bin $BIN_PATH
gcloud compute instances create "alminium1" --zone "us-central1-b" --machine-type "n1-standard-1" --scopes "https://www.googleapis.com/auth/cloud-platform" --metadata binpath=$BIN_PATH --metadata-from-file startup-script=bench_startup_script.sh