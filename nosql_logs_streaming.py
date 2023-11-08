import csv
import os.path
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'nosql_log_stream',
    default_args=default_args,
    schedule=timedelta(minutes=1),
)

logs_file = "/home/sami/airflow/mongod.log"  # Define the log file path
csv_file = "/home/sami/airflow/output/"  # Define the CSV output file path


def stream_logs_to_csv(logs_file, csv_file):
    with open('stream_logs.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        print("stream log csv opened")

        # Write the header row to the CSV file (modify as needed)
        header = ["Log files with the details..."]  # Modify this based on your log format
        csv_writer.writerow(header)

        with open(logs_file, 'r') as log_file:
            for line in log_file:
                # Parse your log lines and extract the relevant data
                # Example parsing: timestamp and log message
                timestamp, log_message = line.split(' ',1)

                # Write the data to the CSV file
                csv_writer.writerow([timestamp, log_message])
            print("log file opened..")
if __name__ == "__main__":
    stream_logs_to_csv(logs_file, csv_file)


# Define the PythonOperator to execute the stream_logs_to_csv function
stream_logs_task = PythonOperator(
    task_id='stream_logs_to_csv',
    python_callable=stream_logs_to_csv,
    op_args=[logs_file, csv_file],  # Pass your log and CSV paths as arguments
    dag=dag,
)



