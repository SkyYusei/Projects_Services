mysqlimport -u zhaoru -p123456 --fields-terminated-by="[cmucchackersseperator]" --lines-terminated-by="[cmucchackersterminator]\n" --local twitter q4

CREATE TABLE `q4` ( `q4key` varchar(250) COLLATE utf8mb4_bin DEFAULT NULL, `q4value` text COLLATE utf8mb4_bin DEFAULT NULL, PRIMARY KEY (`q4key`) ) ENGINE = MYISAM  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
