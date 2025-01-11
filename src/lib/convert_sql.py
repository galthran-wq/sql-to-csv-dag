import os
from collections import defaultdict

import pandas as pd
from tqdm.auto import tqdm


from src.common.utils import split_ignoring_strings


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