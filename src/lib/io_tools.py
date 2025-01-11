import os

import zipfile
import rarfile
import tarfile
import py7zr


def extract_archive(archive_path: str, extract_to: str | None = None):
    """
    Extracts .../archive_name.zip to extract_to (or .../archive_name) folder. 
    Supports the following archive formats: 
    - .zip
    - .rar
    - .tar.gz
    - .7z
    """
    archive_type = os.path.basename(archive_path).split(".")[-1]
    if extract_to is None:
        extract_to = os.path.join(os.path.dirname(archive_path), os.path.basename(archive_path).replace(f".{archive_type}", ""))
    if archive_type == "zip":
        with zipfile.ZipFile(archive_path, 'r') as archive:
            archive.extractall(extract_to)
    elif archive_type == "rar":
        with rarfile.RarFile(archive_path, 'r') as archive:
            archive.extractall(extract_to)
    elif archive_type == "7z":
        with py7zr.SevenZipFile(archive_path, 'r') as archive:
            archive.extractall(extract_to)
    elif archive_type == "tar":
        with tarfile.open(archive_path, 'r:') as archive:
            archive.extractall(extract_to)
    elif archive_type == "gz":
        with tarfile.open(archive_path, 'r:gz') as archive:
            archive.extractall(extract_to)
    return extract_to


def archive(root_folder: str, archive_to: str | None = None):
    """
    Archives all files in root_folder to archive_to. 
    Uses zip format.
    """
    if archive_to is None:
        archive_to = os.path.join(os.path.dirname(root_folder), os.path.basename(root_folder) + ".zip")

    if os.path.exists(archive_to):
        raise ValueError(f"Archive {archive_to} already exists")

    with zipfile.ZipFile(archive_to, 'w') as archive:
        for root, _, files in os.walk(root_folder):
            for file in files:
                archive.write(os.path.join(root, file), arcname=os.path.relpath(os.path.join(root, file), root_folder))

    return archive_to
