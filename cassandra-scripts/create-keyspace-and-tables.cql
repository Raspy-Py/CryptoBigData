CREATE KEYSPACE IF NOT EXISTS crypto_space
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE crypto_space;

CREATE TABLE IF NOT EXISTS raw_transactions (
    table_name TEXT,
    action TEXT,
    timestamp TEXT,
    symbol TEXT,
    side TEXT,
    size INT,
    price FLOAT,
    tickDirection TEXT,
    trdMatchID TEXT,
    grossValue INT,
    homeNotional FLOAT,
    foreignNotional FLOAT,
    trdType TEXT,
    PRIMARY KEY (table_name, action, timestamp, trdMatchID)
);
