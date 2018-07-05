import datetime
import time
import dateutil.parser
import json
from kafka import KafkaConsumer
import cassandra
from cassandra.cluster import Cluster

# Define Kafka brokers and Cassandra cluster to connect to:
broker = ['ec2-18-209-75-68.compute-1.amazonaws.com:9092', 'ec2-18-205-142-57.compute-1.amazonaws.com:9092', 'ec2-50-17-32-144.compute-1.amazonaws.com:9092']
topic = 'fx_avg'
keyspace = 'fx'
cassandra_host_names = ['ec2-52-23-103-178.compute-1.amazonaws.com', 'ec2-52-2-16-225.compute-1.amazonaws.com', 'ec2-34-192-194-39.compute-1.amazonaws.com']

consumer = KafkaConsumer(topic, bootstrap_servers = broker)
print('Consumer created')

cluster = Cluster(cassandra_host_names)
session = cluster.connect(keyspace)
print('Connected to Cassandra')
insert_prep = session.prepare("""
    insert into fx_rates_avg (
    record_id,
    timestamp_d,
    window_start,
    window_end,
    fx_marker,
    bid_big_avg,
    bid_points_avg)
    VALUES (?, ?, ?, ?, ?, ?, ?)""")

# While there are some msgs in Kafca insert them to Cassandra:
for msg in consumer:
    try:
        parsed = msg.value.decode()
        line = json.loads(parsed)
        
        values = []
        values.append(cassandra.util.uuid_from_time(time.time()))
        values.append(dateutil.parser.parse(line['window']['start']).date())
        values.append(dateutil.parser.parse(line['window']['start']))
        values.append(dateutil.parser.parse(line['window']['end']))
        values.append(line['fx_marker'])
        values.append(float(line['avg(bid_big)']))
        values.append(float(line['avg(bid_points)']))
        session.execute(insert_prep, values)
        print('Message inserted into Cassandra')
    except :
        #log.exception()
        print('Insert into Cassandra FAILED!')
        # Keep inserting new data as it comming from the stream
        pass
