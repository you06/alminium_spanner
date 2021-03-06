DROP TABLE IF EXISTS Operation;

CREATE TABLE Operation (
	Id VARCHAR(11) NOT NULL,
	VERB VARCHAR(1023) NOT NULL,
	TargetKey VARCHAR(1023) NOT NULL,
	TargetTable VARCHAR(1023) NOT NULL,
	Body BINARY(255),
	CommitedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (Id)
);

CREATE INDEX OperationTargetKey
ON Operation (
TargetKey
);

CREATE INDEX OperationTargetTable
ON Operation (
TargetTable
);