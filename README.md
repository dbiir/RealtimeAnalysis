# RealTimeAnalysis
Real time analysis toolkits.
Four modules are included:
+ Loader: Pull messages from Kafka, package them into column-oriented file format(Parquet, ORC), and spill out to HDFS.
+ LoaderClient: Push messages to Kafka.
+ RealTimeBenchmark: Generate TPC-H(`order` and `lineitem`) based dataset and call LoaderClient to push messages.

### Get started
1. `mvn package -DskipTests`.
2. Generated jar files are located in `dist/bin` directory.
3. Run `java -jar xxx.jar` for help.
4. Modify `nodes` file, add hostnames or ip addresses. Run `sbin/deploy.sh` to deploy toolkits to nodes specified in `nodes`.

### Configuration

id | name        | default value      | comment
-- | ----------- | ------------------ | -------
1  | log.dir     | ./logs             | The directory in which the log data is kept
2  | logs        | system.log, user.log | Enabled loggers
3  | acks        | 1                    | Kafka producer config: the number of acknowledgments the producer requires the leader to have received before considering a request complete
4  | retries     | 0                    | Kafka producer config: setting a value greater than zero will cause the client to resend any record whose send fails
5  | batch.size  | 112000               | Kafka producer config: the producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition.
6  | buffer.memory | 33554432           | Kafka producer config: the total bytes of memory the producer can use to buffer records waiting to be sent to the server
7  | bootstrap.servers | 127.0.0.1:9092 | Kafka producer/consumer config: a list of host/port pairs to use for establishing the initial connection to the Kafka cluster
8  | linger.ms   | 1                    | Kafka producer config: the producer groups together any records that arrive in between requests transmissions into a single batch request
9  | key.serializer | org.apache.kafka.common.serialization.LongSerialize | Kafka producer config: serializer class for key
10 | value.serializer | cn.edu.ruc.realtime.utils.MessageSer | Kafka producer config: serializer class for value
11 | partitioner.class | cn.edu.ruc.realtime.partition.LoaderClientPartitionKafka | Kafka producer config: partitioner class
12 | thread.pool.size | 10000 | `Loader` thread pool size. Default is `Runtime.getRuntime().availableProcessors() * 2`, if this num is larger than default, set as this num, or else as default.
13 | blocking.pool.size | 50000 | `Loader` blocking pool size. The blocking pool is used as buffer between consumer threads and writer threads in `Loader`
14 | producer.thread.num | 1 | `LoaderClient` producer thread num
15 | loader.topic | realtime | Kafka topic name. `Loader` pulls messages from this topic.
16 | loader.partition.begin | 0 | `Loader` pulls messages from some partitions of the topic specified in `loader.topic`, and partitions are ranged from this value and `loader.partition.end`
17 | loader.partition.end | 79 | `Loader` pulls messages from some partitions of the topic specified in `loader.topic`, and partitions are ranged from `loader.partition.begin` and this value
18 | consumer.group.id | test | Kafka consumer group id. Consumers in the same group shares offsets of the topic
19 | consumer.auto.commit | true | Kafka consumer automatically commit offset or manually commit
20 | consumer.auto.commit.interval.ms | 1000 | Kafka consumer config: the frequency in milliseconds that the consumer offsets are auto-committed to Kafka if `consumer.auto.commit` is set to `true`.
21 | consumer.session.timeout | 30000 | Kafka consumer config: the timeout used to detect consumer failures when using Kafka's group management facility
22 | consumer.key.deserializer | org.apache.kafka.common.serialization.LongDeserializer | Kafka consumer config: deserializer class for key
23 | consumer.value.deserializer | cn.edu.ruc.realtime.utils.MessageDer | Kafka consumer config: deserializer class for value
24 | writer.batch.size | 10000 | `Loader` writer batch size. Num of messages are pulled by consumer threads, batched together and pipelined to the writer thread.
25 | blocking.queue.size | 50000 | `Loader` blocking queue size. The blocking queue are pushed by consumer threads, and pulled by the writer thread.
26 | writer.block.size | 50 | Num of batches in a block. The block is spilled out.
27 | writer.full.factor | 0.98 | Deprecated.
28 | writer.db.name | test | Name of database which data is loaded into
29 | writer.table.name | realtime | Name of table which data is loaded into
30 | writer.file.path | hdfs://127.0.0.1:9000/warehouse/test/realtime/ | Path of HDFS which blocks are written into
31 | writer.thread.num | 1 | Num of `Loader` writer thread. Recommended as 1.
32 | db.connection.driver.class | org.postgresql.Driver | Meta database jdbc driver class
33 | db.connection.user | jelly | Meta database user name
34 | db.connection.pass | 123456 | Meta database user password
35 | db.connection.url | jdbc:postgresql://127.0.0.1/metabase | Meta database connection url
36 | db.connection.table | blockfiles | Name of meta table which stores block information

See [website](https://dbiir.github.io/RealtimeAnalysis/)
