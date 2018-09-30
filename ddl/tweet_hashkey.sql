CREATE TABLE TweetHashKey (
    Id STRING(MAX) NOT NULL,
    Author STRING(MAX) NOT NULL,
    CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    Content STRING(MAX) NOT NULL,
    CreatedAt TIMESTAMP NOT NULL,
    Favos ARRAY<STRING(MAX)> NOT NULL,
    Sort INT64 NOT NULL,
    UpdatedAt TIMESTAMP NOT NULL,
) PRIMARY KEY (Id);

CREATE TABLE Operation (
	Id STRING(MAX) NOT NULL,
	VERB STRING(MAX) NOT NULL,
	TargetKey STRING(MAX) NOT NULL,
	TargetTable STRING(MAX) NOT NULL,
	Body BYTES(MAX),
	CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Id);

CREATE INDEX TweetHashKeySortAsc
ON TweetHashKey (
	Sort
);