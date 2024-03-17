# 06 Streaming Homework solutions

## Question 1: Redpanda version

### creating alias for repanda-1 container since i have existing one

```bash
alias rpk1="sudo docker exec -ti repanda-1 rpk"
```

### checking for repanda version

```bash
rpk1 version

# response
v22.3.5 (rev 28b2443)
```

## Question 2: Creating a topic

```bash
rpk1 topic create test-topic

# response
TOPIC       STATUS
test-topic  OK

```

## Question 3: Connecting to the Kafka server

```py
import json
import time 

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()

"""
output:

True
"""
```

## Question 4: Sending data to the stream

```py
t0 = time.time()

topic_name = 'test-topic'

for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')

"""
output:

Sent: {'number': 0}
Sent: {'number': 1}
Sent: {'number': 2}
Sent: {'number': 3}
Sent: {'number': 4}
Sent: {'number': 5}
Sent: {'number': 6}
Sent: {'number': 7}
Sent: {'number': 8}
Sent: {'number': 9}
took 0.50 seconds
"""
```

### creating topic test-topic

```bash
rpk1 topic consume test-topic
# output
{
  "topic": "test-topic",
  "value": "{\"number\": 0}",
  "timestamp": 1710675107720,
  "partition": 0,
  "offset": 0
}
{
  "topic": "test-topic",
  "value": "{\"number\": 1}",
  "timestamp": 1710675107771,
  "partition": 0,
  "offset": 1
}
{
  "topic": "test-topic",
  "value": "{\"number\": 2}",
  "timestamp": 1710675107821,
  "partition": 0,
  "offset": 2
}
{
  "topic": "test-topic",
  "value": "{\"number\": 3}",
  "timestamp": 1710675107871,
  "partition": 0,
  "offset": 3
}
{
  "topic": "test-topic",
  "value": "{\"number\": 4}",
  "timestamp": 1710675107922,
  "partition": 0,
  "offset": 4
}
{
  "topic": "test-topic",
  "value": "{\"number\": 5}",
  "timestamp": 1710675107972,
  "partition": 0,
  "offset": 5
}
{
  "topic": "test-topic",
  "value": "{\"number\": 6}",
  "timestamp": 1710675108022,
  "partition": 0,
  "offset": 6
}
{
  "topic": "test-topic",
  "value": "{\"number\": 7}",
  "timestamp": 1710675108073,
  "partition": 0,
  "offset": 7
}
{
  "topic": "test-topic",
  "value": "{\"number\": 8}",
  "timestamp": 1710675108123,
  "partition": 0,
  "offset": 8
}
{
  "topic": "test-topic",
  "value": "{\"number\": 9}",
  "timestamp": 1710675108174,
  "partition": 0,
  "offset": 9
}
{
  "topic": "test-topic",
  "value": "{\"number\": 0}",
  "timestamp": 1710675355890,
  "partition": 0,
  "offset": 10
}
{
  "topic": "test-topic",
  "value": "{\"number\": 1}",
  "timestamp": 1710675355941,
  "partition": 0,
  "offset": 11
}
{
  "topic": "test-topic",
  "value": "{\"number\": 2}",
  "timestamp": 1710675355991,
  "partition": 0,
  "offset": 12
}
{
  "topic": "test-topic",
  "value": "{\"number\": 3}",
  "timestamp": 1710675356041,
  "partition": 0,
  "offset": 13
}
{
  "topic": "test-topic",
  "value": "{\"number\": 4}",
  "timestamp": 1710675356092,
  "partition": 0,
  "offset": 14
}
{
  "topic": "test-topic",
  "value": "{\"number\": 5}",
  "timestamp": 1710675356142,
  "partition": 0,
  "offset": 15
}
{
  "topic": "test-topic",
  "value": "{\"number\": 6}",
  "timestamp": 1710675356192,
  "partition": 0,
  "offset": 16
}
{
  "topic": "test-topic",
  "value": "{\"number\": 7}",
  "timestamp": 1710675356243,
  "partition": 0,
  "offset": 17
}
{
  "topic": "test-topic",
  "value": "{\"number\": 8}",
  "timestamp": 1710675356293,
  "partition": 0,
  "offset": 18
}
{
  "topic": "test-topic",
  "value": "{\"number\": 9}",
  "timestamp": 1710675356344,
  "partition": 0,
  "offset": 19
}

```

### Sending the taxi data

```py
t0 = time.time()

for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    # print(row_dict)
    producer.send(topic_name, value=row_dict)
    
producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')

"""
output:

took 28.88 seconds
"""
```

## Question 5: Sending the Trip Data

```bash
rpk1 topic create green-trips

#output

TOPIC        STATUS
green-trips  OK
```

### Sending the Trip Data to green_trips topic

```py

t0 = time.time()

for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send("green_trips", value=row_dict)
    
producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')

"""
output:

took 24.81 seconds
"""
```
