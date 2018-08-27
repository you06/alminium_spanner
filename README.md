# alminium_spanner

Google Cloud Spanner Playground.Spannerに適当にデータを入れたりするアプリケーション。

## Usage

### 環境変数

#### SPANNER_DATABASE

required.
example `projects/gcpug/instances/gcpug-shared-instance/databases/sinmetal`

#### STACKDRIVER_PROJECT

required.
example `gcpug`

#### RUN_WORKS

optional.
中で動かす処理を指定する。
以下の処理がそれぞれgoroutineで実行される。
複数指定する場合は `,` で区切って記述する。
example `InsertTweet,ListTweet`

* InsertBenchmarkTweet
* InsertTweet
* InsertTweetCompositeKey
* InsertTweetHashKey
* InsertTweetUniqueIndex
* ListTweet
* ListTweetResultStruct

#### BENCHMARK_TABLE_NAME

`InsertBenchmarkTweet` の時に利用する。
InsertするTableName

#### BENCHMARK_COUNT

`InsertBenchmarkTweet` の時に利用する。
Insertする行数

### InsertBenchmarkTweet で大量のデータをTableにInsertする

10億件とか自分のPCで動かしてるつらいので、Compute Engineに適当にやってもらおう。

#### 準備

Cloud Storage上にビルドしたバイナリを置いておく。
以下の例では `gs://bin-sinmetal/alminium.bin` を置いている。

#### startup script sample

Startup Scriptで勝手に動いて終わったら、自分自身を削除するようにしておく。
リトライなどは入っておらず、Preemptible VMを使うことは考慮していない。

```
#!/bin/bash
gsutil cp gs://bin-gcpug/alminium.bin .
sudo chmod +x alminium.bin
export SPANNER_PROJECT=gcpug-spanner
export STACKDRIVER_PROJECT=gcpug-stackdriver
export SPANNER_INSTANCE=gcpug-shared-instance
export RUN_WORKS=InsertBenchmarkJoinData
export BENCHMARK_DATABASE_NAME=sinmetal_benchmark_a
export BENCHMARK_ITEM_COUNT=1000
export BENCHMARK_USER_COUNT=1000
export BENCHMARK_ORDER_COUNT=1000
./alminium.bin

# Delete Me
INSTANCE_NAME=$(curl http://metadata/computeMetadata/v1/instance/name -H "Metadata-Flavor: Google")
INSTANCE_ZONE=$(curl http://metadata/computeMetadata/v1/instance/zone -H "Metadata-Flavor: Google")

IFS='/'
set -- $INSTANCE_ZONE
INSTANCE_ZONE=$4
echo $INSTANCE_ZONE
yes | gcloud compute instances delete $INSTANCE_NAME --zone $INSTANCE_ZONE
```