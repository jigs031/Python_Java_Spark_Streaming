# go to kafka bin directory
cd /usr/lib/kafka/bin

# create trip topic
./kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic trip

# create enriched_trip topic
./kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic enriched_trip

# create curated_trip topic
./kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 1 \
    --partitions 1 \
    --topic curated_trip

# prepare a test csv file with 100 rows to test the flow
head -100 ~/OD_2019-06.csv > ~/test

# publish records from test file to trip topic
./kafka-console-producer.sh --broker-list 34.69.50.158:9092 --topic trip < ~/test.csv

/usr/lib/kafka/bin/kafka-console-consumer.sh --from-beginning --bootstrap-server 34.69.50.158:9092 --topic enriched_trip
/usr/lib/kafka/bin/kafka-console-consumer.sh --from-beginning --bootstrap-server 34.69.50.158:9092 --topic trip

./kafka-console-consumer --from-beginning --bootstrap-server 34.69.50.158:9092 --topic curated_trip





