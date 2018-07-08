// This script creates data schema in Cassandra

CREATE KEYSPACE fx WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};

use fx;

// Table for real-time FX rates
CREATE TABLE fx_rates (
record_id uuid,
fx_marker text,
timestamp_ms timestamp,
timestamp_d date,
bid_big double,
bid_points int,
offer_big double,
offer_points int,
hight double,
low double,
open double,
primary key ((fx_marker, timestamp_d), timestamp_ms)
) WITH CLUSTERING ORDER BY (timestamp_ms DESC);

// Table stores computed averages:
CREATE TABLE fx_rates_avg (
record_id uuid,
timestamp_d date,
window_start timestamp,
window_end timestamp,
fx_marker text,
bid_big_avg double,
bid_points_avg double,
primary key ((fx_marker, timestamp_d), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

// Table for storing anomalyes detected my SimpleCount detectore
CREATE TABLE anomaly_by_simple_count (
record_id uuid,
timestamp_d date,
window_start timestamp,
window_end timestamp,
fx_marker text,
anomaly_count double,
primary key ((fx_marker, timestamp_d), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

// Table which stores statistics about FX stream computed on historical data
CREATE TABLE anomaly_count_statistics (
fx_marker text,
stat_gathered_date timestamp,
avg_10sec double,
sbddev_10sec double,
primary key (fx_marker, stat_gathered_date)
) WITH CLUSTERING ORDER BY (stat_gathered_date DESC);
