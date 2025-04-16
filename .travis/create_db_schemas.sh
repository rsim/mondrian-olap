#!/bin/bash

set -ev

mysql -e 'CREATE DATABASE IF NOT EXISTS mondrian_test DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;'
mysql -e "CREATE USER IF NOT EXISTS 'mondrian_test'@'localhost' IDENTIFIED WITH mysql_native_password BY 'mondrian_test';"
mysql -e "GRANT ALL PRIVILEGES ON mondrian_test.* TO 'mondrian_test'@'localhost';"

psql -c "CREATE ROLE mondrian_test PASSWORD 'mondrian_test' LOGIN CREATEDB;"
psql -c 'CREATE DATABASE mondrian_test;'
psql -c 'GRANT ALL PRIVILEGES ON DATABASE mondrian_test TO mondrian_test;'

"$ORACLE_HOME/bin/sqlplus" -L -S / AS SYSDBA <<SQL
CREATE USER mondrian_test IDENTIFIED BY mondrian_test DEFAULT TABLESPACE users;
GRANT CONNECT, RESOURCE TO mondrian_test;
exit
SQL

for adapter in mysql postgresql oracle; do
  bin/rake db:create_data MONDRIAN_DRIVER=$adapter
done
