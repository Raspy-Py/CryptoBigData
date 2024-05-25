CREATE KEYSPACE IF NOT EXISTS crypto_space
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE crypto_space;

CREATE TABLE IF NOT EXISTS raw_transactions (
    timestamp TEXT,
    symbol TEXT,
    side TEXT,
    size INT,
    price FLOAT,
    tickdirection TEXT,
    grossvalue INT,
    homenotional FLOAT,
    foreignnotional FLOAT,
    trdtype TEXT,
    PRIMARY KEY (timestamp)
);