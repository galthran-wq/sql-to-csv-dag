from __future__ import annotations
import os
import shutil
import gzip

import zipfile
import rarfile
import tarfile
import py7zr
import zstandard as zstd


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

    If `include_trivial_types` is True, then we are going to "extract" .csv, .sql, .gz to the extract_to folder.
    """
    if password == "":
        password = None
    archive_type = os.path.basename(archive_path).split(".")[-1]
    if extract_to is None:
        extract_to = os.path.join(os.path.dirname(archive_path), os.path.basename(archive_path).replace(f".{archive_type}", ""))
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
    if include_trivial_types:
        if archive_type in ["sql", "csv"] or archive_type.endswith(".gz"):
            os.makedirs(extract_to, exist_ok=True)
            shutil.copy(archive_path, os.path.join(extract_to, os.path.basename(archive_path)))
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
