import os
import glob
from typing import Optional
import logging

from tqdm.auto import tqdm
import pandas as pd

logger = logging.getLogger(__name__)


def get_entries_df(
    df: pd.DataFrame,
    id_col="ORDER_ID", 
    name_col="NAME",
    value_col="VALUE",
):
    # Drop duplicates before pivoting to avoid ValueError
    df = df.drop_duplicates([id_col, name_col])
    # Pivot the dataframe
    pivoted = df.pivot(index=id_col, columns=name_col, values=value_col)
    # Convert values to lists to match original format
    # pivoted = pivoted.apply(lambda x: [x] if pd.notna(x) else [None])
    return pivoted.reset_index()


def aggregate_files(
    path_pattern,
    id_col="ORDER_ID",
    name_col="NAME",
    value_col="VALUE",
    save_intermediate=False,
    sep="|",
    chunksize: Optional[int] = None
):
    if chunksize is None:
        import sys
        chunksize = sys.maxsize
    for path in glob.glob(path_pattern):
        filename = ".".join(path.split("/")[-1].split(".")[:-1])
        out_filename = filename + "_mapped.csv"
        out_file = "/".join(path.split("/")[:-1]) + "/" + out_filename
        out_dir = "/".join(path.split("/")[:-1]) + "/" + filename + "_mapped"
        if save_intermediate and not os.path.exists(out_dir):
            os.makedirs(out_dir)
        print(path)
        print(out_file)
        if not os.path.exists(out_file):
            first_chunk_columns = None
            for i, chunk in tqdm(enumerate(pd.read_csv(path, sep=sep, chunksize=chunksize, on_bad_lines="warn"))):
                if save_intermediate and os.path.exists(out_dir + f"/{i}.csv"):
                    print(out_dir + f"/{i}.csv" + " is already present")
                    if first_chunk_columns is None:
                        first_chunk_columns = pd.read_csv(out_dir + f"/{i}.csv", sep=sep, nrows=1).columns
                else:   
                    mapped = get_entries_df(
                        chunk, 
                        id_col=id_col, 
                        name_col=name_col, 
                        value_col=value_col
                    )
                    if first_chunk_columns is None:
                        first_chunk_columns = mapped.columns
                    else:
                        if set(mapped.columns) != set(first_chunk_columns):
                            logger.error(f"Chunk {i} has different columns than the first chunk.")
                            logger.error(f"First chunk: {list(first_chunk_columns)}")
                            logger.error(f"Current chunk: {list(mapped.columns)}")
                            logger.error(f"Missing in current: {set(first_chunk_columns) - set(mapped.columns)}")
                            logger.error(f"Extra in current: {set(mapped.columns) - set(first_chunk_columns)}")
                            os.remove(out_file)
                            raise ValueError("Column mismatch between chunks")
                    
                    logger.info(f"Dumping {i * chunksize}..")
                    if save_intermediate:
                        mapped.to_csv(out_dir + f"/{i}.csv", sep=sep, index=False)
                    else:
                        mapped.to_csv(out_file, header=i == 0, sep=sep, index=False, mode="a")
            if save_intermediate:
                dfs = []
                for file in glob.glob(out_dir + "/*.csv"):
                    df = pd.read_csv(file, sep="|")
                    assert set(df.columns) == set(first_chunk_columns), \
                        f"File {file} has different columns than the first chunk. " \
                        f"First chunk: {sorted(first_chunk_columns)}, " \
                        f"Current file: {sorted(df.columns)}"
                    dfs.append(df)
                pd.concat(dfs, axis=0).to_csv(out_file, sep=sep, index=False)


def _aggregate_table(
    file: str,
    id_col: str,
    name_col: str,
    value_col: str,
    chunksize: Optional[int] = None,
    sep: str = "|",
    retry: bool = False,
):
    try:
        aggregate_files(
            file, 
            id_col=id_col, 
            name_col=name_col, 
            value_col=value_col, 
            chunksize=chunksize, 
            sep=sep
        )
    except ValueError as e:
        logger.error(f"Failed to aggregate \'{file}\': {e}")
        logger.error(f"Trying to aggregate with no chunksize")
        if not retry:
            _aggregate_table(
                file, 
                id_col=id_col, 
                name_col=name_col, 
                value_col=value_col, 
                chunksize=None, 
                sep=sep,
                retry=True
            )
    except Exception as e:
        logger.error(f"Failed to aggregate \'{file}\': {e}")
    else:
        logger.info(f"Aggregated \'{file}\' successfully")


def postprocess_tables(root_folder: str, chunksize: Optional[int] = None, sep: str = "|"):
    for file in glob.glob(os.path.join(root_folder, "**", "*.csv"), recursive=True):
        if "b_sale_order_props_values" in file:
            _aggregate_table(
                file, 
                id_col="ORDER_ID", 
                name_col="NAME", 
                value_col="VALUE", 
                chunksize=chunksize, 
                sep=sep
            )
        elif "b_sale_user_props_values" in file:
            _aggregate_table(
                file,
                id_col="USER_ID",
                name_col="NAME",
                value_col="VALUE",
                chunksize=chunksize,
                sep=sep
            )


    