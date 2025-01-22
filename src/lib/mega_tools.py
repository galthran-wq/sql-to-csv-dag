import subprocess
import os

def mega_login(mega_email: str, mega_password: str):
    # Construct the MEGAcmd command for logging in
    command = ["mega-login", mega_email, mega_password]
    
    try:
        # Execute the login command
        subprocess.run(command, check=True)
        print("Login successful.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during login: {e}")
        return False
    return True

def download_file_from_mega(mega_url: str, artifacts_folder: str, mega_email: str = None, mega_password: str = None):
    if mega_email is not None and mega_password is not None:
        mega_login(mega_email, mega_password)

    # Construct the MEGAcmd command for downloading
    command = ["mega-get", mega_url, artifacts_folder]
    
    try:
        # Execute the command
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        output_lines = result.stdout.splitlines()
        for line in output_lines:
            if line.startswith("Download finished:"):
                downloaded_path = line.split("Download finished:")[1].strip()
                if downloaded_path.startswith(artifacts_folder):
                    downloaded_path = os.path.join(artifacts_folder, os.path.relpath(downloaded_path, artifacts_folder))
                break
        else:
            downloaded_path = None
        return downloaded_path
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during download: {e.stderr}")
        raise

def upload_file_to_mega(
    file_path: str,
    mega_email: str,
    mega_password: str,
    dest: str = None,
):
    mega_login(mega_email, mega_password)

    # Construct the MEGAcmd command for uploading
    command = ["mega-put", file_path]
    
    if dest:
        command.append(dest)
    
    try:
        # Execute the upload command
        subprocess.run(command, check=True)
        
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during upload or export: {e}")
        return None
    
    try:
        # Construct the MEGAcmd command for exporting the file to get a link
        export_command = ["mega-export", "-a", os.path.join(dest or "", os.path.basename(file_path))]
       
        # Execute the export command
        result = subprocess.run(export_command, capture_output=True, text=True, check=True)
        
        # Extract the export link from the output
        output_line = result.stdout.strip()
        export_link = output_line.split(": ")[1]
        return export_link
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during export: {e}")
        return None

