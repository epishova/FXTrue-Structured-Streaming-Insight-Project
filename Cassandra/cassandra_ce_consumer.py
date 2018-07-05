import datetime
import json
from kafka import KafkaConsumer
import cassandra
from cassandra.cluster import Cluster

# Define brokers for Kafca and cluster parameters for Cassandra:
broker = ['ec2-18-209-75-68.compute-1.amazonaws.com:9092', 'ec2-18-205-142-57.compute-1.amazonaws.com:9092', 'ec2-50-17-32-144.compute-1.amazonaws.com:9092']
cassandra_host_names = ['ec2-52-23-103-178.compute-1.amazonaws.com', 'ec2-52-2-16-225.compute-1.amazonaws.com', 'ec2-34-192-194-39.compute-1.amazonaws.com']
topic = 'currency_exchange'
keyspace = 'fx'

consumer = KafkaConsumer(topic, bootstrap_servers = broker)
print('Consumer created')

cluster = Cluster(cassandra_host_names)
session = cluster.connect(keyspace)
print('Connected to Cassandra')
insert_prep = session.prepare("""
    insert into fx_rates (
    record_id,
    fx_marker,
    timestamp_ms,
    timestamp_d,
    bid_big,
    bid_points,
    offer_big,
    offer_points,
    hight,
    low,
    open)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")

# While there are some msgs in Kafca insert them to Cassandra:
for msg in consumer:
    try:
        jsons = msg.value.decode()
        for line in jsons.split('\n'):
            parsed = json.loads(line)
            values = []
            values.append(parsed["fx_marker"])
            values.append(int(parsed["timestamp_ms"]))
            values.append(datetime.datetime.fromtimestamp(values[1] / 1000.0).strftime('%Y-%m-%d'))
            values.append(float(parsed["bid_big"]))
            values.append(int(parsed["bid_points"]))
            values.append(float(parsed["offer_big"]))
            values.append(int(parsed["offer_points"]))
            values.append(float(parsed["hight"]))
            values.append(float(parsed["low"]))
            values.append(float(parsed["open"]))
            values.insert(0, cassandra.util.uuid_from_time(values[1] / 1000.0))
            session.execute(insert_prep, values)
            print('Message inserted into Cassandra')
    except :
        #log.exception()
        print('Insert into Cassandra FAILED!')
        # Keep inserting new data as it comming from the stream
        pass
