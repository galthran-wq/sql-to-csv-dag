from mega import Mega
import os

def download_file_from_mega(mega_url: str, artifacts_folder: str):
    mega = Mega()
    mega.login_anonymous()
    path = mega.download_url(mega_url, dest_path=artifacts_folder)
    # clean after download
    # remove all temp files with "megapy" prefix
    for file in os.listdir("/tmp"):
        if file.startswith("megapy"):
            os.remove(os.path.join("/tmp", file))
    return path

def upload_file_to_mega(
    file_path: str,
    dest: str = None,
    dest_filename: str = None,
    mega_login: str = None,
    mega_password: str = None,
):
    mega = Mega()
    mega.login(mega_login, mega_password)
    file = mega.upload(file_path, dest, dest_filename)
    return mega.get_upload_link(file)

