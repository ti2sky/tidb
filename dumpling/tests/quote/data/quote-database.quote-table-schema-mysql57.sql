/*!40101 SET NAMES binary*/;
/*T![placement] SET PLACEMENT_CHECKS = 0*/;
CREATE TABLE `quo``te/table` (`quo``te/col` INT(11) NOT NULL,`a` INT(11) DEFAULT NULL,`gen``id` INT(11) GENERATED ALWAYS AS(`quo``te/col`) VIRTUAL,PRIMARY KEY(`quo``te/col`)) ENGINE = InnoDB DEFAULT CHARACTER SET = LATIN1 DEFAULT COLLATE = LATIN1_SWEDISH_CI;
