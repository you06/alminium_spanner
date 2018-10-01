CREATE TABLE Tweet (
    Id STRING(MAX) NOT NULL,
    Author STRING(MAX) NOT NULL,
    CommitedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    Content STRING(MAX) NOT NULL,
    CreatedAt TIMESTAMP NOT NULL,
    Favos ARRAY<STRING(MAX)> NOT NULL,
    Sort INT64 NOT NULL,
    UpdatedAt TIMESTAMP NOT NULL,
) PRIMARY KEY (Id);

CREATE INDEX TweetSortAsc
ON Tweet (
	Sort
);