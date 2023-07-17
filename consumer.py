from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
import os 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/azril/finproj/google_credentials.json"

dataset_name = 'bank_marketing'
table_name = 'marketing_data_table'

client = bigquery.Client()
client.create_dataset(dataset_name, exists_ok=True)
dataset = client.dataset(dataset_name)

schema = [
    bigquery.SchemaField('ID', 'INT64'),
    bigquery.SchemaField('Date', 'DATE'),
    bigquery.SchemaField('Age', 'INT64'),
    bigquery.SchemaField('Job', 'STRING'),
    bigquery.SchemaField('Marital', 'STRING'),
    bigquery.SchemaField('Education', 'STRING'),
    bigquery.SchemaField('Default', 'STRING'),
    bigquery.SchemaField('Housing', 'STRING'),
    bigquery.SchemaField('Loan', 'STRING'),
    bigquery.SchemaField('Contact', 'STRING'),
    bigquery.SchemaField('Month', 'STRING'),
    bigquery.SchemaField('Day_of_week', 'STRING'),
    bigquery.SchemaField('Duration', 'INT64'),
    bigquery.SchemaField('Campaign', 'INT64'),
    bigquery.SchemaField('pdays', 'INT64'),
    bigquery.SchemaField('Previous', 'INT64'),
    bigquery.SchemaField('Poutcome', 'STRING'),
    bigquery.SchemaField('Cons_conf_idx', 'FLOAT64'),
    bigquery.SchemaField('Emp_var_rate', 'FLOAT64'),
    bigquery.SchemaField('Cons_price_idx', 'FLOAT64'),
    bigquery.SchemaField('Euribor3m', 'FLOAT64'),
    bigquery.SchemaField('Nr_employed', 'INT64'),
    bigquery.SchemaField('y', 'STRING')
]


table_ref = bigquery.TableReference(dataset, table_name)
table = bigquery.Table(table_ref, schema=schema)
client.create_table(table, exists_ok=True)

def read_messages():
    consumer_config = {"bootstrap.servers": "localhost:9092",
                       "schema.registry.url": "http://localhost:8081",
                       "group.id": "marketingData.avro.consumer.1",
                       "auto.offset.reset": "earliest"}

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["marketing_data"])

    while True:
        try:
            client.insert_rows(table, [message.value()])
        except Exception as e:
            print(f"Exception while inserting into BigQuery: {e}")

        else:
            if message is not None:
                print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
                # INSERT STREAM TO BIGQUERY
                client.insert_rows(table, [message.value()])
            else:
                print("No new messages at this point. Try again later.")

        consumer.close()


if __name__ == "__main__":
    read_messages()
