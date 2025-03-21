import string
from itertools import count
import boto3
from DBConnectionSystem import connect

connection, cursor = connect.connect_to_db()
def get_cloudwatch_metrics():
    client = boto3.client('cloudwatch')
    response = client.list_metrics()
    return response
def show_cloudwatch_metrics():
    response = get_cloudwatch_metrics()
    for metric in response['Metrics']:
        print(metric)
def read_one_entry():
    try:
        cursor.execute("SELECT * FROM ")
        result = cursor.fetchone()
        if result:
            print("Fetched row:", result)
        else:
            print("No data found.")
    except Exception as e:
        print("Error reading from the database:", e)





