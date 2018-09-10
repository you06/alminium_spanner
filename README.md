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
* InsertBenchmarkJoinData

#### BENCHMARK_TABLE_NAME

`InsertBenchmarkTweet` の時に利用する。
InsertするTableName

#### BENCHMARK_COUNT

`InsertBenchmarkTweet` の時に利用する。
Insertする行数

### InsertBenchmarkJoinData で大量のデータをTableにInsertする

10億件とか自分のPCで動かしてるつらいので、Compute Engineに適当にやってもらおう。

#### 準備

Cloud Storage上にビルドしたバイナリを置いておく。
以下の例では `gs://bin-sinmetal/alminium.bin` を置いている。

#### DataをぶっこむCompute Engineを作成する

Startup Scriptで勝手に動いて終わったら、自分自身を削除するようにしておく。
リトライなどは入っておらず、Preemptible VMを使うことは考慮していない。

`bench_startup_script.sh` の項目を修正する

```
export STORAGE_PATH=gs://hoge/alminium.bin
./start.sh
```