FROM apache/airflow:2.7.0

# Install necessary packages as root
USER root
RUN apt-get update && \
    apt-get install -y python3.8-dev build-essential software-properties-common wget curl gnupg && \
    # Download and run the MariaDB repository setup script
    wget https://r.mariadb.com/downloads/mariadb_repo_setup && \
    chmod +x mariadb_repo_setup && \
    ./mariadb_repo_setup && \
    apt-get update && \
    apt-get install -y libmariadb3 libmariadb-dev && \
    # Add MEGAcmd repository and install MEGAcmd
    curl -sL https://mega.nz/linux/MEGAsync/Debian_11/Release.key | apt-key add - && \
    echo "deb https://mega.nz/linux/MEGAsync/Debian_11/ ./" > /etc/apt/sources.list.d/megacmd.list && \
    apt-get update && \
    apt-get install -y megacmd

# Switch back to the user specified by AIRFLOW_UID
ARG AIRFLOW_UID=50000
USER ${AIRFLOW_UID}:0

# Copy the requirements file and install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r /requirements.txt

# Use the entrypoint script
# ENTRYPOINT ["/entrypoint.sh"]
# CMD ["airflow", "webserver"]
