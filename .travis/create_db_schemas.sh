#!/bin/bash

set -ev

mysql -e 'CREATE DATABASE IF NOT EXISTS mondrian_test;'
mysql -e "GRANT ALL PRIVILEGES ON mondrian_test.* TO 'mondrian_test'@'localhost' IDENTIFIED BY 'mondrian_test';"

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
