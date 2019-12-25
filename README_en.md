# alminium

Alminium benchmark for Google Spanner and MySQL.

## Usage

### Running

Create your configuration file from [config.example.toml](./config/config.example.toml).

Build & Run

```sh
go build
./alminium_spanner -config config/config.toml
```

### Configuration

`run-works` field can be one of:

* InsertBenchmarkTweet

* UpdateTweet

* InsertTweet

* InsertTweetCompositeKey

* InsertTweetHashKey

* InsertTweetUniqueIndex
