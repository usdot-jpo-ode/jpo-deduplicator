cd ~/kafka/
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# start kafka
bin/kafka-server-start.sh -daemon config/kraft/server.properties
# wait 2 seconds for the server to start and be able to add partitions
sleep 2s
# add topics

# MAP
bin/kafka-topics.sh --create --topic "topic.OdeMapJson" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic "topic.DeduplicatedOdeMapJson" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic "topic.ProcessedMap" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic "topic.DeduplicatedProcessedMap" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

# SPaT
bin/kafka-topics.sh --create --topic "topic.OdeSpatJson" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic "topic.DeduplicatedOdeSpatJson" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic "topic.ProcessedSpat" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic "topic.DeduplicatedProcessedSpat" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

# BSM
bin/kafka-topics.sh --create --topic "topic.OdeBsmJson" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic "topic.DeduplicatedOdeBsmJson" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

# TIM
bin/kafka-topics.sh --create --topic "topic.OdeTimJson" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic "topic.DeduplicatedOdeTimJson" --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
