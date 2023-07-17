from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
import glob

def load_avro_schema_from_file():
    key_schema = avro.load("marketing_data_key.avsc")
    value_schema = avro.load("marketing_data_value.avsc")
    return key_schema, value_schema

def send_record():
    key_schema, value_schema = load_avro_schema_from_file()
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
    file = open('/home/azril/finproj/datasets/bank_2022-10-01.csv')
    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
        try:
            emp_var_rate = float(row[17])
            cons_price_idx = float(row[18].replace(' ', '.'))
            euribor3m = float(row[20].replace(' ', '.'))
            cons_conf_idx = float(row[19])
        except ValueError as e:
            print(f"Failed to convert float field: {e}")
            continue
                
        key = {"ID": int(row[0])}
        value = {
            "ID": int(row[0]),
            "Date": str(row[1]),
            "Age": int(row[2]),
            "Job": str(row[3]),
            "Marital": str(row[4]),
            "Education": str(row[5]),
            "Default": str(row[6]),
            "Housing": str(row[7]),
            "Loan": str(row[8]),
            "Contact": str(row[9]),
            "Month": str(row[10]),
            "Day_of_week": str(row[11]),
            "Duration": int(row[12]),
            "Campaign": int(row[13]),
            "pdays": int(row[14]),
            "Previous": int(row[15]),
            "Poutcome": str(row[16]),
            "Emp_var_rate": emp_var_rate,
            "Cons_conf_idx": cons_conf_idx,
            "Cons_price_idx": cons_price_idx,
            "Euribor3m": euribor3m,
            "Nr_employed": int(row[21]),
            "y": str(row[22])
        }
        try:
            producer.produce(topic='marketing_data', key=key, value=value)
            print(f"Successfully producing record value - {value}")
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")

        producer.flush()
        sleep(1)

def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

if __name__ == "__main__":
    send_record()
