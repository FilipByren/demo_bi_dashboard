from google.cloud import bigquery
import datetime
import random
import os
import logging
from flask import Flask
app = Flask(__name__)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/credential.json"

bigquery_client = bigquery.Client()

@app.route('/')
def update_table():
    df = aggregation_on_table()
    load_on_dashboard_table(df)

    return 'Success'

@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    logging.exception(e)
    return 'An internal error occurred.', 500

def aggregation_on_table():
    # Table is from: https://www.w3schools.com/sql/
    query = """
    SELECT SUM(CustomerID) as random_value , MAX(CustomerID) as max
    FROM
    (SELECT CustomerID
    FROM dashboard_db.customer 
    ORDER BY RAND()
    LIMIT 10)
    """
    query_job = bigquery_client.query(query)
    df = query_job.to_dataframe()
    random_day = random.randint(1, 28)
    random_month = random.randint(1, 12)
    df["date"] = datetime.datetime.now().replace(day=random_day,month=random_month)

    return df


def load_on_dashboard_table(data_frame):
    destination_table = "dashboard_db.dashboard_table"
    project_id = "bi-dashboard-automated-example"
    data_frame.to_gbq(destination_table, project_id, if_exists='append')
