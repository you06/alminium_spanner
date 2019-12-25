DROP TABLE IF EXISTS TweetHashKey;

CREATE TABLE TweetHashKey (
    Id VARCHAR(1023) NOT NULL,
    Author VARCHAR(1023) NOT NULL,
    CommitedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    Content VARCHAR(1023) NOT NULL,
    CreatedAt TIMESTAMP NOT NULL,
    Favos VARCHAR(1023) NOT NULL,
    Sort INT(64) NOT NULL,
    UpdatedAt TIMESTAMP NOT NULL,
    PRIMARY KEY (Id)
);

CREATE INDEX TweetHashKeySortAsc
ON TweetHashKey (
	Sort
);

DROP TABLE IF EXISTS TweetCompositeKey;
CREATE TABLE TweetCompositeKey (
    Id VARCHAR(1023) NOT NULL,
    Author VARCHAR(1023) NOT NULL,
    CommitedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    Content VARCHAR(1023) NOT NULL,
    CreatedAt TIMESTAMP NOT NULL,
    Favos VARCHAR(1023) NOT NULL,
    Sort INT(64) NOT NULL,
    UpdatedAt TIMESTAMP NOT NULL,
    PRIMARY KEY (Id,Author)
);

DROP TABLE IF EXISTS TweetUniqueIndex;
CREATE TABLE TweetUniqueIndex (
	Id VARCHAR(1023) NOT NULL,
	TweetId VARCHAR(1023) NOT NULL,
	Author VARCHAR(1023) NOT NULL,
	CommitedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	Content VARCHAR(1023) NOT NULL,
	CreatedAt TIMESTAMP NOT NULL,
	Favos VARCHAR(1023) NOT NULL,
	Sort INT(64) NOT NULL,
	UpdatedAt TIMESTAMP NOT NULL,
    PRIMARY KEY (Id)
);

CREATE UNIQUE INDEX natural_key 
ON TweetUniqueIndex (
	TweetId,
	Author
);