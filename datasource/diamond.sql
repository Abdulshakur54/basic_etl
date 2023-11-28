CREATE DATABASE etl;
USE etl;
CREATE TABLE diamonds (
	id INT(11) AUTO_INCREMENT PRIMARY KEY,
	carat FLOAT,
	cut VARCHAR(20),
	color CHAR,
	clarity VARCHAR(20),
	depth FLOAT,
    tab INT,
    price FLOAT,
    x FLOAT,
    y FLOAT,
    z FLOAT
);

ALTER TABLE diamonds ADD COLUMN created DATETIME DEFAULT CURRENT_TIMESTAMP;

LOAD DATA LOCAL INFILE  '/home/abdulshakur/learning/python/simple_etl/datasource/diamond.csv' 
INTO TABLE diamonds 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
SET created = CURRENT_TIMESTAMP;