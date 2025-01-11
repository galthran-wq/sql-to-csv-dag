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


def extract_tables_from_sql(file, print_on_error=True, output_path=None):
    lines_it = open(file)
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
                print(f"New table: {new_table_name}")
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
                    print(f"Invalid entry: {row}")
                invalid_lines += 1
                # if invalid_lines % 10_000 == 0:
                #     print(f"Invalid_line: {row}")
            if output_path is not None and len(values) > 500_000:
                table_output_path = output_path + f"/{table_name}.csv"
                header = not os.path.exists(table_output_path)
                print(f"Intermediate dump to {table_output_path}...")
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
    print(f"Total number of invalid lines: {invalid_lines}")
    dfs = {
        table: pd.concat(table_dfs, axis=0)
        for table, table_dfs in dfs.items()
    }
    if output_path is not None:
        for table_name, df in dfs.items():
            table_output_path = output_path + f"/{table_name}.csv"
            header = not os.path.exists(table_output_path)
            print(f"dump to {table_output_path}...")
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
            sql_content = f.read()
        with open(file_path.replace(".sql.gz", ".sql"), 'w') as f:
            f.write(sql_content)
        os.remove(file_path)
        file_path = file_path.replace(".sql.gz", ".sql")
    
    file_type = file_path.split(".")[-1]

    if output_file_path is None and file_type != "csv":
        filename = os.path.basename(file_path)
        output_file_path = os.path.join(output_root_folder, filename)

    if file_type == "sql":
        has_table_definitions = False
        with open(file_path, 'r') as f:
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
                file_path, output_path=output_file_path
            )
            for database_name, df in database_to_df.items():
                df.to_csv(os.path.join(output_file_path, f"{database_name}.csv"), index=False)
        else:
            if mariadb_client is None:
                raise ValueError("mariadb_client is required to convert .sql files with table definitions")
            # We assume that it is a proper mysql dump
            databases = mariadb_client.get_databases()
            while filename in databases:
                filename = filename + "_"
            mariadb_client.source_sql(
                file_path, 
                db=filename, 
                logfile=None, 
                encoding="utf-8", 
            )
            mariadb_client.dump_mariadb_db(database=filename, output_path=output_file_path)
    elif file_type == "csv":
        pass
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    return output_file_path
