FROM apache/airflow:2.7.0

# Copy the entrypoint script and change permissions
# COPY docker/entrypoint.sh /entrypoint.sh
# RUN chmod +x /entrypoint.sh

# Adjust the path to the requirements.txt file
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r /requirements.txt

# Use the entrypoint script
# ENTRYPOINT ["/entrypoint.sh"]
# CMD ["airflow", "webserver"]
