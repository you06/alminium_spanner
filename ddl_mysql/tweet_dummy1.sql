DROP TABLE IF EXISTS TweetDummy1;

CREATE TABLE TweetDummy1 (
    Id VARCHAR(1023) NOT NULL,
    Author VARCHAR(1023) NOT NULL,
    CommitedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    Content VARCHAR(1023) NOT NULL,
    Count INT(64) NOT NULL,
    CreatedAt TIMESTAMP NOT NULL,
    Favos VARCHAR(1023) NOT NULL,
    Sort INT(64) NOT NULL,
    UpdatedAt TIMESTAMP NOT NULL,
    PRIMARY KEY (Id)
);

CREATE INDEX TweetDummy1SortAsc
ON TweetDummy1 (
	Sort
);

CREATE UNIQUE INDEX TweetDummy1Content ON TweetDummy1(Content);