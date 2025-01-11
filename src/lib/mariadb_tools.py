import os
import logging
from typing import List
from subprocess import Popen, PIPE
from pathlib import Path

import mariadb
from tqdm.auto import tqdm
import pandas as pd


logger = logging.getLogger(__name__)

TABLE_PARAMETER = "{TABLE_PARAMETER}"
DROP_TABLE_SQL = f"DROP TABLE {TABLE_PARAMETER};"
GET_TABLES_SQL = "SELECT name FROM sqlite_schema WHERE type='table';"
GET_TABLES_SQL = "SHOW TABLES;"
GET_COLUMNS_SQL = f'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N"{TABLE_PARAMETER}"'

def get_conn(database, user="root", password="password", host="localhost", port=3306):
    conn = mariadb.connect(
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database};")
    cur.close()
    conn.close()
    conn = mariadb.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )
    return conn

def delete_all_tables(con):
    tables = get_tables(con)
    delete_tables(con, tables)

def get_tables(con):
    cur = con.cursor()
    cur.execute(GET_TABLES_SQL)
    tables = cur.fetchall()
    cur.close()
    return [table[0] for table in tables]

def delete_tables(con, tables):
    cur = con.cursor()
    for table in tables:
        sql = DROP_TABLE_SQL.replace(TABLE_PARAMETER, table)
        cur.execute(sql)
    cur.close()

def get_table_columns(con, table):
    cur = con.cursor()
    sql = f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table}'"
    cur.execute(sql)
    columns = cur.fetchall()
    cur.close()
    return columns

def source_sql(sql_path, db, logfile=None, encoding="utf-8", user="root", password="password", host="localhost", port=3306):
    process = Popen([
        'mariadb',
        f'--user={user}',
        f'--password={password}',
        f'--host={host}',
        f'--port={port}',
        f'--default-character-set={encoding}',
        '--max_allowed_packet=1073741824',
        db,
    ], stdout=PIPE, stdin=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate(('source ' + str(sql_path)).encode("utf-8"))
    if logfile is not None:
        with open(logfile, "a") as f:
            if stdout is not None:
                print(stdout.decode("utf-8"))
                for line in stdout.decode("utf-8"):
                    f.write(line)
            if stderr is not None:
                for line in stderr.decode("utf-8"):
                    f.write(line)


def dump_mariadb_db(database: str, output_path: str):
    output_path = Path(output_path)
    conn = get_conn(database)
    tables = get_tables(conn)
    for table in tables:
        os.makedirs(output_path / database, exist_ok=True)
        table_output_path = output_path / database / (table + ".csv")
        if not os.path.exists(table_output_path):
            try:
                df = table_to_df(conn=conn, table=table, log=logging)
                if len(df) > 0:
                    logger.info(f"Dumping {table} with {len(df)} entries...")
                    df.to_csv(table_output_path, sep="|", escapechar='\\', index=False)
            except Exception as e:
                logger.error(f"Tryied dumping {table}, error: {str(e)}")


def dump_all_dbs(output_path: str):
    output_path = Path(output_path)
    # Connect to get list of databases
    conn = get_conn("test")
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES;")
    databases = [db[0] for db in cursor.fetchall()]
    
    # Filter out system databases
    system_dbs = {'mysql', 'information_schema', 'performance_schema', 'sys', 'test', 'airflow'}
    databases = [db for db in databases if db not in system_dbs]
    
    logger.info(f"Found {len(databases)} databases to dump")
    for database in tqdm(databases, desc="Dumping databases"):
        try:
            logger.info(f"\nDumping database: {database}")
            dump_mariadb_db(database, output_path)
        except Exception as e:
            logger.error(f"Error dumping database {database}: {str(e)}")
    
    conn.close()


def table_to_df(conn, table, log, limit=None):
    cur = conn.cursor()
    if limit is None:
        cur.execute(f"select * from {table}")
    else:
        cur.execute(f"select * from {table} limit {limit}")
    data = cur.fetchall()
    columns = get_table_columns(conn, table)
    columns = [column[3] for column in columns]
    log.info(f"Columns for table {table}: {columns}")
    if len(data) > 0 and len(columns) != len(data[0]):
        log.info("Columns misimatch")
        columns = columns[:len(data[0])]
    log.info(f"Columns for table {table}: {columns}")
    return pd.DataFrame(data, columns=columns)

