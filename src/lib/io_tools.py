from __future__ import annotations
import os
import shutil
import gzip
import logging

import zipfile
import rarfile
import tarfile
import py7zr
import zstandard as zstd

logger = logging.getLogger(__name__)


def extract_archive(
    archive_path: str,
    extract_to: str | None = None,
    password: str | None = None,
    archive_type: str | None = None,
    include_trivial_types: bool = True,
):
    """
    Extracts .../archive_name.zip to extract_to (or .../archive_name) folder. 
    Supports the following archive formats: 
    - .zip
    - .rar
    - .tar.gz
    - .7z
    - .zst
    - .gz

    If `include_trivial_types` is True, then we are going to "extract" .csv, .sql to the extract_to folder.
    """
    if password == "":
        password = None
    archive_type = os.path.basename(archive_path).split(".")[-1]
    if extract_to is None:
        extract_to = os.path.join(os.path.dirname(archive_path), os.path.basename(archive_path).replace(f".{archive_type}", ""))
    logger.info(f"Extracting archive {archive_path} to {extract_to}...")
    if archive_type == "zip":
        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive.extractall(extract_to, pwd=password.encode() if password else None)
    elif archive_type == "rar":
        with rarfile.RarFile(archive_path, 'r') as archive:
            archive.extractall(extract_to, pwd=password)
    elif archive_type == "7z":
        with py7zr.SevenZipFile(archive_path, 'r', password=password) as archive:
            archive.extractall(extract_to)
    elif archive_type == "tar":
        with tarfile.open(archive_path, 'r:') as archive:
            archive.extractall(extract_to)
    elif archive_type == "tar.gz":
        with tarfile.open(archive_path, 'r:gz') as archive:
            archive.extractall(extract_to)
    elif archive_type == "zst":
        zst_file_name = os.path.basename(archive_path).replace('.zst', '')
        zst_file_path = os.path.join(extract_to, zst_file_name)
        os.makedirs(extract_to, exist_ok=True)
        with open(archive_path, 'rb') as f_in, open(zst_file_path, 'wb') as f_out:
            dctx = zstd.ZstdDecompressor()
            dctx.copy_stream(f_in, f_out)
    elif archive_type == "gz":
        gz_file_name = os.path.basename(archive_path).replace('.gz', '')
        gz_file_path = os.path.join(extract_to, gz_file_name)
        os.makedirs(extract_to, exist_ok=True)
        with gzip.open(archive_path, 'rb') as f_in, open(gz_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if include_trivial_types:
        if archive_type in ["sql", "csv"]:
            os.makedirs(extract_to, exist_ok=True)
            shutil.copy(archive_path, os.path.join(extract_to, os.path.basename(archive_path)))
    return extract_to


def extract_archive_recursive(
    archive_path: str,
    extract_to: str | None = None,
    password: str | None = None,
    archive_type: str | None = None,
    include_trivial_types: bool = True,
    delete_nested_archives: bool = True,
):
    """
    Extracts archive_path to extract_to (or .../archive_name) folder. 
    If archive contains other archives, they are extracted recursively and deleted if `delete_nested_archives` is True.
    """
    extract_to = extract_archive(
        archive_path=archive_path, 
        extract_to=extract_to, 
        password=password, 
        archive_type=archive_type, 
        include_trivial_types=include_trivial_types
    )
    for root, dirs, files in os.walk(extract_to):
        for file in files:
            if (
                file.endswith(".zip") or file.endswith(".rar") or file.endswith(".7z") or file.endswith(".tar") or 
                file.endswith(".tar.gz") or file.endswith(".zst") or file.endswith(".gz")
            ):
                logger.info(f"Found nested archive {os.path.join(root, file)}")
                try:
                    extract_archive_recursive(
                        archive_path=os.path.join(root, file), 
                        extract_to=None, 
                        password=None, 
                        archive_type=None, 
                        include_trivial_types=include_trivial_types,
                        delete_nested_archives=delete_nested_archives
                    )
                    if delete_nested_archives:
                        os.remove(os.path.join(root, file))
                except Exception as e:
                    logger.error(f"Error extracting nested archive {os.path.join(root, file)}: {e}")
    return extract_to


def archive(root_folder: str, archive_to: str | None = None, delete_if_exists: bool = True):
    """
    Archives all files in root_folder to archive_to. 
    Uses zip format.
    """
    if archive_to is None:
        archive_to = os.path.join(os.path.dirname(root_folder), os.path.basename(root_folder) + ".zip")
    
    if os.path.exists(archive_to):
        if delete_if_exists:
            os.remove(archive_to)
        else:
            raise ValueError(f"Archive {archive_to} already exists")

    with zipfile.ZipFile(archive_to, 'w') as archive:
        for root, _, files in os.walk(root_folder):
            for file in files:
                archive.write(os.path.join(root, file), arcname=os.path.relpath(os.path.join(root, file), root_folder))

    return archive_to
