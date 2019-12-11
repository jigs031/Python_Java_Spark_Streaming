**`Sprint 3** `


**Pre-requisites**
1. Kafka Cluster Should be setup
2. HDFS Should be setup and running
3. Station_information and station_status file should be available in HDFS 
	Station_information - in CSV
	Station_status - in JSON



**Setup Single Node Kafka Cluster**

mkdir /usr/local/confluent
cd /usr/local/confluent
wget http://packages.confluent.io/archive/5.2/confluent-5.2.1-2.12.zip
unzip confluent-5.2.1-2.12.zip

**Start ZK:**

/usr/local/confluent/confluent-5.2.1/bin/zookeeper-server-start -daemon  /usr/local/confluent/confluent-5.2.1/etc/kafka/zookeeper.properties

**Start Kafka:**

/usr/local/confluent/confluent-5.2.1/bin/kafka-server-start -daemon /usr/local/confluent/confluent-5.2.1/etc/kafka/server.properties

**Produce Message to Kafka Topic:**

/usr/local/confluent/confluent-5.2.1/bin/kafka-console-producer --broker-list localhost:9092 --topic <topic-name>

_Example:_
Below command read the data from console and write it into Kafka
/usr/local/confluent/confluent-5.2.1/bin/kafka-console-producer --broker-list localhost:9092 --topic trip


**Consumer Message from Kafka**

/usr/local/confluent/confluent-5.2.1/bin/kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic <topic-name>

Example
/usr/local/confluent/confluent-5.2.1/bin/kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic trip
/usr/local/confluent/confluent-5.2.1/bin/kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic enriched_trip
/usr/local/confluent/confluent-5.2.1/bin/kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic curated_trip




**Start the Program in below sequence:**

**1. InformationSource:**
	This program read the input CSV records from file and load into Kafka "trip" topic

	Verification:
		Run the Kafka consumer on "trip topic"
			-  /usr/local/confluent/confluent-5.2.1/bin/kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic trip


**2. Enrichment Service**
	- Enrichment Service read the Look up station information data from HDFS using Java Code and load into in-memory.
    - It consumes messages from "trip" topic, enrich it with in-memory look up and load the result into "enriched_trip" topic.

	Verification:
		Run the Kafka consumer on "enriched_trip topic"
			-  /usr/local/confluent/confluent-5.2.1/bin/kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic enriched_trip

**3. Curation Service**
	- Service read the Look up station Status data from HDFS using Spark and load it into Spark DataFrame.
	- Start Streaming from "enriched_trip" topic and join it with "Station_status" dataframe
	- result will be loaded into Kafka topic - "curated_trip"

	Verification:
		Run the Kafka consumer on "curated_trip topic"
			-  /usr/local/confluent/confluent-5.2.1/bin/kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic curated_trip


**4. ResultSink**
	- Porgram load the "curated_trip" stream into HDFS.

	Verification:
			Check underlying HDFS Location -
				- hdfs dfs -ls <location>
				- hdfs dfs -cat <location>/*