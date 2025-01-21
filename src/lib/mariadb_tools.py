from __future__ import annotations
import os
import logging
from typing import List
from subprocess import Popen, PIPE
from pathlib import Path

import mariadb
from tqdm.auto import tqdm
import pandas as pd

logger = logging.getLogger(__name__)

class MariaDBClient:
    TABLE_PARAMETER = "{TABLE_PARAMETER}"
    DROP_TABLE_SQL = f"DROP TABLE {TABLE_PARAMETER};"
    GET_TABLES_SQL = "SHOW TABLES;"
    GET_COLUMNS_SQL = f'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N"{TABLE_PARAMETER}"'

    def __init__(self, user="root", password="password", host="localhost", port=3306):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
    
    def create_database(self, database: str):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database};")
        cur.close()
        conn.close()

    def _get_conn(self, database=None):
        conn = mariadb.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        if database is not None:
            self.create_database(database)
            conn = mariadb.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=database
            )
        return conn

    def delete_all_tables(self):
        tables = self.get_tables()
        self.delete_tables(tables)

    def get_tables(self, database: str):
        conn = self._get_conn(database)
        cur = conn.cursor()
        cur.execute(self.GET_TABLES_SQL)
        tables = cur.fetchall()
        cur.close()
        return [table[0] for table in tables]

    def delete_tables(self, tables):
        conn = self._get_conn()
        cur = conn.cursor()
        for table in tables:
            sql = self.DROP_TABLE_SQL.replace(self.TABLE_PARAMETER, table)
            cur.execute(sql)
        cur.close()
        conn.close()

    def get_table_columns(self, table: str):
        conn = self._get_conn()
        cur = conn.cursor()
        sql = f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table}'"
        cur.execute(sql)
        columns = cur.fetchall()
        cur.close()
        conn.close()
        return columns

    def source_sql(self, sql_path, db, logfile=None, encoding="utf8"):
        logger.info(f"Sourceing {sql_path} to {db}")
        process = Popen([
            'mariadb',
            f'--user={self.user}',
            f'--password={self.password}',
            f'--host={self.host}',
            f'--port={self.port}',
            f'--default-character-set={encoding}',
            '--max_allowed_packet=1073741824',
            '--ssl=0',
            db,
        ], stdout=PIPE, stdin=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate(('source ' + str(sql_path) + ";").encode("utf-8"))
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")
        if stdout:
            logger.info(stdout)
        if stderr:
            logger.error(stderr)
        if logfile is not None:
            with open(logfile, "a") as f:
                if stdout is not None:
                    f.write(stdout.decode("utf-8"))
                if stderr is not None:
                    f.write(stderr.decode("utf-8"))

    def dump_mariadb_db(self, database: str, output_path: str):
        output_path = Path(output_path)
        tables = self.get_tables(database)
        total = 0
        errors = 0
        for table in tables:
            os.makedirs(output_path / database, exist_ok=True)
            table_output_path = output_path / database / (table + ".csv")
            if not os.path.exists(table_output_path):
                try:
                    df = self.table_to_df(database=database, table=table)
                    if len(df) > 0:
                        logger.info(f"Dumping {table} with {len(df)} entries...")
                        df.to_csv(table_output_path, sep="|", escapechar='\\', index=False)
                        total += 1
                except Exception as e:
                    errors += 1
                    logger.error(f"Tryied dumping {table}, error: {str(e)}")
        logger.info(f"Dumped {total} tables with {errors} errors")
        return total, errors

    def get_databases(self):
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SHOW DATABASES;")
        databases = [db[0] for db in cur.fetchall()]
        cur.close()
        conn.close()
        return databases

    def dump_all_dbs(self, output_path: str):
        output_path = Path(output_path)
        
        # Filter out system databases
        system_dbs = {'mysql', 'information_schema', 'performance_schema', 'sys', 'test', 'airflow'}
        databases = [db for db in self.get_databases() if db not in system_dbs]
        
        logger.info(f"Found {len(databases)} databases to dump")
        for database in tqdm(databases, desc="Dumping databases"):
            try:
                logger.info(f"\nDumping database: {database}")
                self.dump_mariadb_db(database, output_path)
            except Exception as e:
                logger.error(f"Error dumping database {database}: {str(e)}")
        

    def table_to_df(self, database: str, table: str, limit: int | None = None):
        conn = self._get_conn(database)
        cur = conn.cursor()
        if limit is None:
            cur.execute(f"select * from {table}")
        else:
            cur.execute(f"select * from {table} limit {limit}")
        data = cur.fetchall()
        columns = self.get_table_columns(table)
        columns = [column[3] for column in columns]
        logger.info(f"Columns: {columns}")
        if len(data) > 0 and len(columns) != len(data[0]):
            logger.warning("Columns mismatch!")
            logger.warning(f"First row: {data[0]}")
            columns = columns[:len(data[0])]
        cur.close()
        conn.close()
        return pd.DataFrame(data, columns=columns)

    def delete_database(self, database: str):
        """Deletes the specified database."""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute(f"DROP DATABASE IF EXISTS {database};")
            logger.info(f"Database {database} deleted successfully.")
        except mariadb.Error as e:
            logger.error(f"Error deleting database {database}: {str(e)}")
        finally:
            cur.close()
            conn.close()
