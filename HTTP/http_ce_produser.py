import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# Specify your credentials to connect to TrueFX:
YOURUSERNAME = 
YOURPASSWORD = 

topic = 'currency_exchange'
broker = 'ec2-18-209-75-68.compute-1.amazonaws.com:9092, ec2-18-205-142-57.compute-1.amazonaws.com:9092, ec2-50-17-32-144.compute-1.amazonaws.com:9092'
#broker = 'localhost:9092'

r = requests.get("http://webrates.truefx.com/rates/connect.html?u="+YOURUSERNAME+"&p="+YOURPASSWORD"&q=ozrates&f=csv")
#print(r.status_code)
if r.status_code == requests.codes.ok:
    print('HTTP request is OK')
    request_body = "http://webrates.truefx.com/rates/connect.html?id=" + r.text.strip()

    producer = KafkaProducer(bootstrap_servers = broker)
    print('Start sending messages')

    while True:
    #for i in range(5):
        rates = requests.get(request_body)
        if rates.status_code == requests.codes.ok:
            #if rates.text.strip() == '':
            #    print("GET returned NO msgs. Nothing to send to pipeline")
            #    continue
            #print("here {}".format(rates.text.strip()))
            jsons = ''
            for rate in rates.text.strip().split("\n"):
                values = rate.split(",")
                #print("here {}".format(values))
                if len(values) < 9:
                    print("GET returned unexpected msg. Nothing to send to pipeline")
                    continue
                msg = {'fx_marker': values[0], 'timestamp_ms': values[1], 'bid_big': values[2], 'bid_points': values[3], 'offer_big': values[4], 'offer_points': values[5], 'hight': values[6], 'low': values[7], 'open': values[8]}
                jsons += json.dumps(msg) + "\n"

            # It is more efficient to send one big msg then many small msgs:
            jsons = jsons.strip()
            future = producer.send(topic, str.encode(jsons))
            try:
                record_metadata = future.get(timeout = 60) #ms
            except KafkaError:
                #log.exception()
                print('Message failed')
                pass
        #print('Message sent')
        #time.sleep(5)
