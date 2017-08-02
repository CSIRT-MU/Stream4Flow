## Host profiling

### Description
An application for collecting aggregated characteristics of hosts in a longer period of time, typically 24 hours.
The application produces the temporal summary of activity of every host observed over the longer period of time.

The aggregation and observation intervals can be set by `-lw` (long window), and `-sw` (short window) params, 
defining the duration of **aggregations** (-**short window**) and a duration of **observation** (=**long window**) in seconds.
The long window must be a **multiple** of short window.

By default, the durations are set to `-lw 86400 -sw 3600` to observe and deliver in a daily interval aggregated in hourly intervals.

The application outputs the data of host logs with temporal identification as keys. The logs contain the aggregated data delivered
from [host_stats](https://github.com/CSIRT-MU/Stream4Flow/blob/master/applications/statistics/hosts_statistics/spark/host_stats.py) 
application:
- **Packets**: for each host a sum of packets transferred in each of small windows
- **Bytes**: for each host a sum of bytes transferred in each of small aggregation windows
- **Flows**: for each host a sum of flows transferred in each of small aggregation windows

Note that the application requires a running **Kafka** and Spark's **host_stats** application with output zookeeper 
and output topic **matching** the input zookeeper and input topic of **this** application. 

In addition, the **host_stats** app is supposed to deliver data in **time interval dividing** the **``-sw``**, otherwise the results
 of **host_daily_profile** will be **biased**.

### Usage:
After setting up the stream data producer with matching ``-oz `` and `` -ot ``, start the application as follows:

- General:

```commandline
host_daily_profile.py -iz <input-zookeeper-hostname>:<input-zookeeper-port> -it <input-topic> -oz <output-hostname>:<output-port> -ot <output-topic> -sw <small window in secs> -lw <long window in secs>
```                       

- Stream4Flow example:

```commandline
/home/spark/applications/run-application.sh /home/spark/applications/hosts_profiling/host_daily_profile.py -iz producer:2181 -it host.stats -oz producer:9092 -ot results.daily -sw 3600 -lw 86400
```

...starts the application with **24h** delivery interval of **1h** statistics aggregations.