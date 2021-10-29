# KafkaStreamProcessor

**Configuration**

All Kafka configurations are in kafka.properties file. 



**How to start the project?**

Run KafkaConsumerUtil class main method.
It starts the kafka consumer to read the incoming messages from kafka consumer broker topic.
Upto 1000 messages are polled and processed one at a time by a message processor.

MessageProcessor aggregates the uid's for each minute and send to MessagePublisher

MessagePublisher creates outbound message with unique uid's per minute and sends to KafkaProducerUtil

KafkaProducerUtil publishes the messages to producer kafka proker topic



**Bonus questions / challenges:**

How can you scale it to improve throughput?
Ans: We can scale the kafka poll from 1000 to higher value to read more messages at once.
Also MessagePublisher threads can be scaled from 10 to higher value to process and publish faster

You may want count things for different time frames but only do json parsing once.
Ans: Parse the json once and extract all the required attributes using a gson and a bean class and perform the required aggregation

Explain how you would cope with failure if the app crashes mid day / mid year.
Ans: Consumer.commitAsync doesn't commit the offset immediately being read. So, if the app crashes, unprocessed messages can be read again and process. Also, we can store the processed message offsets in db, so that using last processed offset can be reset if app crashes.

When creating e.g. per minute statistics, how do you handle frames that arrive late or frames with a random timestamp (e.g. hit by a bitflip), describe a strategy?
Ans: This will require a trade off on latency and processing time, as the messages are out of sequence, we can set a waiting threshold of a minute or two to wait for all out of sequence messages to arrive and process later.

Make it accept data also from std-in (instead of Kafka) for rapid prototyping. (this might be helpful to have for testing anyways)
Ans: Run IbMessageProcessor main method.

Measure also the performance impact / overhead of the json parser
Ans: Json is being parsed only once. Larger the json takes more time for parsing.


**Advanced solution**


Benchmark:
Ans: At 1000 messages polled at once, it takes < 5sec to process and publish the message to kafka topic. Higher the messages / minute increase latency of the result

Output to a new Kafka Topic instead of stdout
Ans: Done

Try to measure performance and optimize
Ans: Tried with local input from KafkaProducer main method and the given sample data stream.jsonl.gz and published custom messages to test the performance. Hgiher the number of messages/minute greate the latency of the output.

Write about how you could scale
Ans: Using Threadpool for processing and publishing the messages.

Only now think about the edge cases, options and other things
Ans: Ignore the messages that doesn't have ts field.
If ts is in millis but not in seconds
if uid is not present in the message
if messages are out of sequence



