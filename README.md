# FXTrue - Insight Project

Real-time analysis and manipulation detection for currency exchange

### Introduction

This project was developed as part of Insight Data Engineering Fellow Program.

The goal was to build a distributed, streaming pipeline for real-time analysis and manipulation detection for currency exchange.

First, I ingest real-time streaming data from FX provider. Then, I analyze and monitor incoming data to identify current FX trends and detect anomalies in the stream (Kafka, Spark, Cassandra, Python, Scala). Finally, I create analytical dashboards to display reactive real-time metrics (Dash)

### Pipeline

This code was run on Amazon AWS servers. 

I get FX rates from [TrueFX Market Data](https://www.truefx.com/?page=frontpage) by using their Web API. The HTTP client is implemented in http_ce_producer.py. This script produces data to Kafka topic. 
