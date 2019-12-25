DROP TABLE IF EXISTS TweetDummy2;

CREATE TABLE TweetDummy2 (
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

CREATE INDEX TweetDummy2SortAsc
ON TweetDummy2 (
	Sort
);

CREATE UNIQUE INDEX TweetDummy2Content ON TweetDummy2(Content);
