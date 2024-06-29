DROP TABLE IF EXISTS `factStocks`;
DROP TABLE IF EXISTS `factCoins`;
DROP TABLE IF EXISTS `dimTime`;
DROP TABLE IF EXISTS `dimCoin`; 
DROP TABLE IF EXISTS `dimCompany`;

CREATE TABLE `dimCompany` (
	`keyCompany` INT NOT NULL,
	`stockCodeCompany` VARCHAR(32) NOT NULL,
	`nameCompany` VARCHAR(64) NOT NULL,
	`sectorCodeCompany` VARCHAR(32) NOT NULL,
	`sectorCompany` VARCHAR(256) NOT NULL,
	`segmentCompany` VARCHAR(256) NOT NULL,
	CONSTRAINT companyPK PRIMARY KEY (keyCompany)
);

CREATE TABLE `dimCoin` (
	`keyCoin` INT NOT NULL,
	`abbrevCoin` VARCHAR(32) NOT NULL,
	`nameCoin` VARCHAR(32) NOT NULL,
	`symbolCoin` VARCHAR(8) NOT NULL,
	CONSTRAINT coinPK PRIMARY KEY (keyCoin)
);

CREATE TABLE `dimTime` (
	`keyTime` INT NOT NULL,
	`datetime` VARCHAR(32) NOT NULL,
	`dayTime` SMALLINT NOT NULL,
	`dayWeekTime` SMALLINT NOT NULL,
	`dayWeekAbbrevTime` VARCHAR(32) NOT NULL,
	`dayWeekCompleteTime` VARCHAR(32) NOT NULL,
	`monthTime` SMALLINT NOT NULL,
	`monthAbbrevTime` VARCHAR(32) NOT NULL,
	`monthCompleteTime` VARCHAR(32) NOT NULL,
	`bimonthTime` SMALLINT NOT NULL,
	`quarterTime` SMALLINT NOT NULL,
	`semesterTime` SMALLINT NOT NULL,
	`yearTime` INT NOT NULL,
	CONSTRAINT timePK PRIMARY KEY (keyTime)
);

CREATE TABLE `factCoins` (
	`keyTime` INT NOT NULL,
	`keyCoin` INT NOT NULL,
	`valueCoin` FLOAT NOT NULL,
    FOREIGN KEY (keyTime) REFERENCES dimTime(keyTime),
    FOREIGN KEY (keyCoin) REFERENCES dimCoin(keyCoin),
    CONSTRAINT coinsPK PRIMARY KEY(keyTime, keyCoin)
);

CREATE TABLE `factStocks` (
	`keyTime` INT NOT NULL,
	`keyCompany` INT NOT NULL,
	`openValueStock` FLOAT NOT NULL,
	`closeValueStock` FLOAT NOT NULL,
	`highValueStock` FLOAT NOT NULL,
	`lowValueStock` FLOAT NOT NULL,
	`quantityStock` FLOAT NOT NULL,
    FOREIGN KEY (keyTime) REFERENCES dimTime(keyTime),
    FOREIGN KEY (keyCompany) REFERENCES dimCompany(keyCompany),
    CONSTRAINT stocksPK PRIMARY KEY(keyTime, keyCompany)
);
