-- init.sql
GRANT ALL PRIVILEGES ON *.* TO 'airflow'@'%' IDENTIFIED BY 'airflow' WITH GRANT OPTION;
FLUSH PRIVILEGES;