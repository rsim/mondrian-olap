#!/bin/bash

set -ev

mysql -h 127.0.0.1 -u root -p$MYSQL_DATABASE_PASSWORD -e "CREATE DATABASE IF NOT EXISTS mondrian_test default character set utf8 default collate utf8_general_ci;"
mysql -h 127.0.0.1 -u root -p$MYSQL_DATABASE_PASSWORD -e "CREATE USER 'mondrian_test'@'%' IDENTIFIED BY 'mondrian_test';"
mysql -h 127.0.0.1 -u root -p$MYSQL_DATABASE_PASSWORD -e "GRANT ALL PRIVILEGES ON mondrian_test.* TO 'mondrian_test'@'%';"
export MYSQL_DATABASE_PASSWORD=mondrian_test

PGPASSWORD=$POSTGRES_DATABASE_PASSWORD psql -c "CREATE ROLE mondrian_test PASSWORD 'mondrian_test' LOGIN CREATEDB;" -U postgres
PGPASSWORD=$POSTGRES_DATABASE_PASSWORD psql -c "CREATE DATABASE mondrian_test;" -U postgres
PGPASSWORD=$POSTGRES_DATABASE_PASSWORD psql -c 'ALTER DATABASE mondrian_test OWNER TO mondrian_test;' -U postgres
export POSTGRES_DATABASE_PASSWORD=mondrian_test

$ORACLE_HOME/bin/sqlplus -L -S / AS SYSDBA <<SQL
CREATE USER mondrian_test IDENTIFIED BY mondrian_test DEFAULT TABLESPACE users;
GRANT CONNECT, RESOURCE TO mondrian_test;
exit
SQL
export ORACLE_DATABASE_PASSWORD=mondrian_test

sqlcmd -S "(local)" -U "sa" -P "Password12!" -C -i .github/workflows/sqlserver/create_user.sql
sqlcmd -S "(local)" -U "mondrian_test" -P "mondrian_test" -C -Q "CREATE DATABASE mondrian_test"
export SQLSERVER_DATABASE_PASSWORD=mondrian_test

for driver in mysql postgresql oracle sqlserver; do
  MONDRIAN_DRIVER=$driver bundle exec rake db:create_data
done
