-- SET GLOBAL local_infile=true;

LOAD DATA INFILE '/store_files_mysql/clean_store_transactions.csv' INTO TABLE clean_store_transactions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- LOAD DATA INFILE '/store_files_mysql/raw_store_transactions.csv' INTO TABLE clean_store_transactions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- LOAD DATA LOCAL INFILE '/store_files_mysql/raw_store_transactions.csv' INTO TABLE clean_store_transactions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- LOAD DATA LOCAL INFILE '/store_files_mysql/clean_store_transactions.csv' INTO TABLE clean_store_transactions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;