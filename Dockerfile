# Use the official Airflow image as the base image
FROM apache/airflow:2.9.2

# Install openpyxl and any other packages you need
RUN pip install openpyxl xlrd numpy

# Ensure the Airflow user has the correct UID
ARG AIRFLOW_UID=50000
USER root
RUN chown -R ${AIRFLOW_UID}:0 /opt/airflow
USER ${AIRFLOW_UID}

# Copy the rest of the files needed for Airflow
COPY dags /opt/airflow/dags/
COPY plugins /opt/airflow/plugins/
COPY config /opt/airflow/config/

# Set the entrypoint and command if necessary
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]
