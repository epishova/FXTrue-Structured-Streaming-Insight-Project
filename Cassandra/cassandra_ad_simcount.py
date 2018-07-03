import datetime
import time
import dateutil.parser
import json
from kafka import KafkaConsumer
import cassandra
from cassandra.cluster import Cluster

# TODO: specify cassandra remote cluster here
broker = ['ec2-18-209-75-68.compute-1.amazonaws.com:9092', 'ec2-18-205-142-57.compute-1.amazonaws.com:9092', 'ec2-50-17-32-144.compute-1.amazonaws.com:9092']
topic = 'ad_simple_count'
keyspace = 'fx'

consumer = KafkaConsumer(topic, bootstrap_servers = broker)
print('Consumer created')

cluster = Cluster()
session = cluster.connect(keyspace)
print('Connected to Cassandra')

for msg in consumer:
    try:
        parsed = msg.value.decode()
        line = json.loads(parsed)
        #print(line)        
        values = []
        values.append(cassandra.util.uuid_from_time(time.time()))
        values.append(dateutil.parser.parse(line['window']['start']).date())
        values.append(dateutil.parser.parse(line['window']['start']))
        values.append(dateutil.parser.parse(line['window']['end']))
        values.append(line['fx_marker'])
        values.append(int(line['count']))
        #print(values)
        session.execute(
                """
            insert into anomaly_by_simple_count (
            record_id,
            timestamp_d,
            window_start,
            window_end,
            fx_marker,
            anomaly_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            values
        )
        print('Message inserted into Cassandra')
    except :
        #log.exception()
        print('Insert into Cassandra FAILED!')
        pass
    
# for msg in consumer:
#     print(msg)
