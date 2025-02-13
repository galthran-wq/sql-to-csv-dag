from __future__ import annotations
import shutil
import logging
import os
from collections import defaultdict
import os
from typing import List, Dict
import gzip

import pandas as pd
from tqdm.auto import tqdm

from src.common.utils import split_ignoring_strings
from src.lib.mariadb_tools import MariaDBClient

logger = logging.getLogger(__name__)


def get_files_to_convert(root_folder: str):
    """
    Returns a list of files to convert (to .csv).
    Supports the following file formats: 
    - .sql
    - .csv
    - .sql.gz
    """
    files_to_convert = []
    total = 0
    skipped = 0
    for root, _, files in os.walk(root_folder):
        for file in files:
            total += 1
            if file.endswith(".sql") or file.endswith(".csv") or file.endswith(".sql.gz"):
                files_to_convert.append(os.path.join(root, file))
            else:
                skipped += 1
                logging.warning(f"Skipping file {file} because it is not a valid file to convert")
                logging.warning(f"File path: {os.path.join(root, file)}")
    logging.info(f"Total files: {total}, skipped: {skipped}")
    return files_to_convert


def extract_tables_from_sql(file, print_on_error=False, output_path=None, encoding="utf8", errors="ignore"):
    lines_it = open(file, encoding=encoding, errors=errors)
    dfs = defaultdict(list)

    # current df data
    header = None
    table_name = None
    columns = None
    values = None

    invalid_lines = 0

    for row in tqdm(lines_it):
        if "insert into" in row.lower() and "values" in row.lower():
            new_table_name = row[row.lower().find("insert into")+len("insert into")+1:row.lower().find("(")].strip()
            if table_name is None or new_table_name != table_name:
                if new_table_name != table_name:
                    dfs[table_name].append(pd.DataFrame(values, columns=columns))
                values = []
                logger.info(f"New table: {new_table_name}")
                header = row
                columns = [ col.strip()[1:-1] for col in split_ignoring_strings(header[header.find("(")+1:header.find(")")]) ]
            # first_sep = row.find("`")
            # table_name = row[first_sep+1:first_sep+1+row[first_sep+1:].find("`")]
            row = row[row.lower().find("values"):]
            table_name = new_table_name
        if table_name is not None:
            clean_row = []
            for raw_entry in split_ignoring_strings(row[row.find("(")+1:row.rfind(")")]):
                raw_entry = raw_entry.strip()
                try:
                    entry = int(raw_entry)
                except:
                    if raw_entry == "NULL":
                        entry = None
                    else:
                        # assume string type
                        for str_char in ["'", '"']:
                            if raw_entry.startswith(str_char):
                                raw_entry = raw_entry[1:]
                            if raw_entry.endswith(str_char):
                                raw_entry = raw_entry[:-1]
                        entry = str(raw_entry)
                clean_row.append(entry)
                del entry
            if len(clean_row) == len(columns):
                values.append(clean_row)
            else:
                if print_on_error:
                    logger.warning(f"Invalid entry: {row}")
                invalid_lines += 1
                # if invalid_lines % 10_000 == 0:
                #     print(f"Invalid_line: {row}")
            if output_path is not None and len(values) > 500_000:
                table_output_path = output_path + f"/{table_name}.csv"
                header = not os.path.exists(table_output_path)
                logger.info(f"Intermediate dump to {table_output_path}...")
                pd.DataFrame(values, columns=columns).to_csv(
                    table_output_path,
                    sep="|",
                    index=False,
                    header=header,
                    mode="a"
                )
                values = []
    if values is not None and len(values) > 0:
        dfs[table_name].append(pd.DataFrame(values, columns=columns))
    logger.info(f"Total number of invalid lines: {invalid_lines}")
    dfs = {
        table: pd.concat(table_dfs, axis=0)
        for table, table_dfs in dfs.items()
    }
    if output_path is not None:
        for table_name, df in dfs.items():
            table_output_path = output_path + f"/{table_name}.csv"
            header = not os.path.exists(table_output_path)
            logger.info(f"dump to {table_output_path}...")
            df.to_csv(
                table_output_path,
                sep="|",
                index=False,
                header=header,
                mode="a"
            )
    else:
        return dfs


def convert_file(
    root_folder: str,
    relative_file_path: str, 
    output_root_folder: str,
    output_file_path: str | None = None,
    mariadb_client: MariaDBClient | None = None,
    sep: str = "|",
):
    """
    Converts a file (containing some number of tables) to a list of .csv files.
    If .csv is passed, it is simply moved to the same relative location in output_root_folder.

    Supports the following file formats: 
    - .sql
    - .csv
    - .sql.gz
    """
    file_path = os.path.join(root_folder, relative_file_path)

    # convert .sql.gz file to .sql
    if file_path.endswith(".sql.gz"):
        with gzip.open(file_path, 'rb') as f:
            sql_content = f.read().decode('utf-8')
        with open(file_path.replace(".sql.gz", ".sql"), 'w') as f:
            f.write(sql_content)
        os.remove(file_path)
        file_path = file_path.replace(".sql.gz", ".sql")
    
    file_type = file_path.split(".")[-1]
    filename = ".".join(os.path.basename(file_path).split(".")[:-1])

    if output_file_path is None:
        if file_type == "sql":
            output_file_path = os.path.join(output_root_folder, filename)
            database_name = ''.join(e for e in filename if e.isalnum() or e == '_')
        elif file_type == "csv":
            output_file_path = os.path.join(output_root_folder, os.path.basename(file_path))
        else:
            logger.warning(f"Unsupported file type: {file_type}")

    if file_type == "sql":
        has_table_definitions = False
        with open(file_path, 'r', errors="ignore") as f:
            for line in f:
                if "CREATE TABLE" in line:
                    has_table_definitions = True
                    break
        if not has_table_definitions:
            # We assume that the file looks like:
            # INSERT INTO table_name (column1, column2, column3) VALUES (value1, value2, value3);
            # INSERT INTO table_name (column1, column2, column3) VALUES (value1, value2, value3);
            # ...
            database_to_df: Dict[str, pd.DataFrame] = extract_tables_from_sql(
                file_path
            )
            os.makedirs(output_file_path, exist_ok=True)
            for database_name, df in database_to_df.items():
                if len(df) > 0:
                    df.to_csv(os.path.join(output_file_path, f"{database_name}.csv"), index=False, sep=sep)
        else:
            logger.info(f"Converting {file_path} to {output_file_path} with mariadb_client")
            if mariadb_client is None:
                raise ValueError("mariadb_client is required to convert .sql files with table definitions")
            # We assume that it is a proper mysql dump
            databases_before = mariadb_client.get_databases()
            while database_name in databases_before:
                database_name = database_name + "_"
            mariadb_client.create_database(database_name)
            mariadb_client.source_sql(
                file_path, 
                db=database_name, 
            )
            databases_after = mariadb_client.get_databases()
            # 1. Try dumping `database_name`
            total, errors = mariadb_client.dump_mariadb_db(database=database_name, output_path=output_file_path)
            mariadb_client.delete_database(database_name)
            # 2. Try dumping all other databases, if `database_name` is empty
            # Motivation: some dumps might populate a different database than the one we use when sourcing the sql file
            if total == 0:
                logger.info(f"Dumping all other new databases: {databases_after}; since {database_name} is empty..")
                for database in databases_after:
                    if database not in databases_before and database != database_name:
                        total, errors = mariadb_client.dump_mariadb_db(database=database, output_path=output_file_path)
                        mariadb_client.delete_database(database_name)
    elif file_type == "csv":
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        shutil.move(file_path, output_file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    return output_file_path


def convert_all_files(
    root_folder: str, 
    files_to_convert: list[str],
    mariadb_client: MariaDBClient | None = None,
):
    output_root_folder = root_folder + "_output"
    total = 0
    skipped = 0
    for file in files_to_convert:
        total += 1
        relative_file_path = os.path.relpath(file, root_folder)
        try:
            output_file_path = convert_file(
                root_folder=root_folder,
                relative_file_path=relative_file_path,
                output_root_folder=output_root_folder,
                mariadb_client=mariadb_client,
            )
            logger.info(f"Successfully converted file {file} to {output_file_path}")
        except Exception as e:
            skipped += 1
            logger.error(f"Failed to convert file {file}: {e}")
    logger.info(f"Total files converted: {total}, skipped: {skipped}")
    if total == skipped:
        raise ValueError("No files were converted!")
    return output_root_folder


def cleanup_success_converted_file(success_file_path: str, mariadb_client: MariaDBClient | None = None):
    """
    Cleans up the successfully converted file by removing it from the filesystem.
    
    Args:
        success_file_path (str): The path to the successfully converted file.
    """
    if success_file_path.endswith(".csv"):
        return
    elif os.path.isdir(success_file_path):
        dir_name = os.path.basename(success_file_path)
        if mariadb_client is None:
            raise ValueError("mariadb_client is required to cleanup successfully converted files")
        databases = mariadb_client.get_databases()
        if dir_name in databases:
            mariadb_client.drop_database(dir_name)
        else:
            logging.warning(f"Database {dir_name} does not exist and cannot be dropped")
    else:
        raise ValueError(f"Unsupported file type: {success_file_path}")


def cleanup_all_success_converted_files(
    success_files: list[str], 
    mariadb_client: MariaDBClient | None = None,
):
    total = 0
    skipped = 0
    for success_file in success_files:
        total += 1
        try:
            cleanup_success_converted_file(success_file, mariadb_client)
        except Exception as e:
            skipped += 1
            logger.error(f"Failed to cleanup file {success_file}: {e}")
    logger.info(f"Total files cleaned up: {total}, skipped: {skipped}")
