# Processing transactions using Kafka, Spark and Avro

This Java example how to produce and consume data to and from Kafka. Data is serialized and deserialized using Avro. Apache Spark is used to process the data before sending it to and when reading from the Kafka data stream.
The use case in this example is the processing of transactions. These are sent to a Kafka stream using random intervals (max. 5 seconds). Another process calculates the sum per distributor over a 3 second window and stores the ongoing accumulation in another file.
This process should pick up where it left off in case it crashes and needs to restart.

# Resources

The **resources** directory contains the following files:
1. transactions.csv: the transactions
2. assignment.docx: the complete assignment
3. output.csv: an output example

This project is built and packaged using Maven 3.3.9.
The **target** directory contains a jar including all dependencies: ´demo.spark.josi-0.1-jar-with-dependencies´
To generate the jar, execute the following Maven command in the cloned directory: 
´mvn clean compile assembly:single´

The **src/main/java/myapp** directory contains four .java files:
1. App.java: contains UI-logic
2. MyAvroSparkProducer.java: implementation of the producer
3. MyAvroSparkConsumer.java: implementation of the consumer
4. Util.java: A utility class containing the used Avro schema

## Prerequisites
- Java 8 (or higher)
- For Windows users: install the hadoop winutils and [add them to your class path](https://stackoverflow.com/questions/18630019/running-apache-hadoop-2-1-0-on-windows)
- [A Kafka installation (1.1.0) ](https://kafka.apache.org/quickstart)

The **pom.xml** contains all Java dependencies. This manual assumes the user runs the examples in a Windows (10) environment. If you use another OS, I refer you to the Kafka documentations for the correct commands to setup your Kafka server.

Before running the example, make sure that Zookeeper and Kafka. In what follows, we assume that Zookeeper, Kafka and Schema Registry are started with the default settings. 
These can also be found in the **kafka_properties** directory.

The default producer and consumer properties used in this example can be found in **src\main\resources** 

---

# Start Zookeeper
$ bin/zookeeper-server-start config/zookeeper.properties

# Start Kafka
$ bin/kafka-server-start config/server.properties

# Start Schema Registry
$ bin/schema-registry-start config/schema-registry.properties
If you don't already have a schema registry, you will need to install it. Either from packages: http://www.confluent.io/developer Or from source: https://github.com/confluentinc/schema-registry

Then create a topic called clicks:

# Create page_visits topic
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
  --partitions 1 --topic clicks
Then run the producer to produce 100 clicks:

$ java -cp target/uber-ClickstreamGenerator-1.0-SNAPSHOT.jar com.shapira.examples.producer.avroclicks.AvroClicksProducer 100 http://localhost:8081
You can validate the result by using the avro console consumer (part of the schema repository):

$ bin/kafka-avro-console-consumer --zookeeper localhost:2181 --topic clicks --from-beginning


## Edit a file

You’ll start by editing this README file to learn how to edit a file in Bitbucket.

1. `> cd %KAFKA_HOME%`
2. `> bin\windows\zookeeper-server-start config\zookeeper.properties`
3. `> bin\windows\kafka-server-start.bat config\server.properties`
4. `> bin\windows\kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic **topic**`
5. java -jar target\demo.spark.josi-0.1-jar-with-dependencies.jar  --type producer
6. java -jar target\demo.spark.josi-0.1-jar-with-dependencies.jar  --type consumer
