# SQL Converter Project

## Overview
This project is designed to convert SQL dump files into CSV format, leveraging Docker and Apache Airflow for orchestration and automation. It supports various SQL dump formats and provides a Telegram bot interface for initiating and monitoring conversion tasks.

## Features
- **SQL to CSV Conversion**: Converts SQL dump files into CSV format for easier data manipulation and analysis.
- **Dockerized Environment**: Utilizes Docker for consistent and isolated environments.
- **Airflow Integration**: Automates the conversion process using Apache Airflow.
- **Telegram Bot**: Provides a Telegram bot interface for initiating and monitoring conversion tasks.
- **Support for Multiple SQL Formats**: Handles various SQL dump formats including MySQL and PostgreSQL.

## Project Structure

Here's a tree-like view of the project with descriptions for some source files:

```
sql-converter/
├── README.md                   # Project overview and setup instructions
├── docker-compose.yaml         # Docker Compose configuration for services
├── config.yaml                 # Configuration file for credentials and settings
├── src/
│   ├── bot/
│   │   └── telegram_bot.py     # Telegram bot interface for conversion tasks
│   │   └── ...
│   ├── dags/
│   │   ├── load_sql.py         # DAG for converting SQL to CSV
│   │   ├── hello_world.py      # Simple DAG for printing a greeting
│   │   └── show_databases.py   # DAG for showing databases in MariaDB
│   ├── lib/
│   │   ├── mega_tools.py       # Tools for interacting with Mega storage
│   │   ├── mariadb_tools.py    # Tools for interacting with MariaDB
│   │   ├── convert_sql.py      # Functions for SQL to CSV conversion
│   │   ├── io_tools.py         # Input/output utility functions
│   │   └── sql_to_csv.py       # SQL to CSV conversion logic
└── .gitignore                  # Git ignore file
```


## Airflow DAGs Overview

**`sql_to_csv` DAG**:
- **Purpose**: Converts SQL dump files to CSV format.
- **Tasks**:
   - `download_file_from_mega`: Downloads a file from a Mega URL.
   - `extract_archive`: Extracts the downloaded archive.
   - `get_files_to_convert`: Identifies files to be converted.
   - `convert_all_files`: Converts the identified files to CSV.
   - `archive_files`: Archives the converted files.
   - `upload_files_to_mega`: Uploads the archived files back to Mega.


## Deployment Instructions

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Modify `config.yaml` with your credentials.

3. **Build Docker Images**:
   - Use the provided `docker-compose.yaml` to build the Docker images.
   ```bash
   docker-compose build
   ```

4. **Start the Services**:
   - Launch the services using Docker Compose.
   ```bash
   docker-compose up -d
   ```

5. **Initiazlie Airflow**:
   - Initialize Airflow by running the following command.
   ```bash
   docker-compose up airflow-init
   ```

6. **Access Airflow**:
   - Open your web browser and navigate to `http://localhost:8765` to access the Airflow web interface.

7. **Use the Telegram Bot**:
   - Start the Telegram bot by sending the `/start` command to initiate the conversion process.
   - Forward or directly send the archive containing the SQL dump file to the bot.

8. **Monitor and Manage**:
   - Use the Airflow web interface to monitor DAG runs and task statuses.
   - Use the Telegram bot to check the status of your conversion tasks.

## Requirements
- Docker and Docker Compose installed on your machine.
- Python 3.8 environment for local development and testing.

