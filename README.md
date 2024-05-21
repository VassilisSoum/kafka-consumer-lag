# Kafka Consumer Service

This project is a Kafka Consumer Service written in Scala. It encapsulates the logic for consuming
messages from a Kafka topic and keeps track of the lag for each partition.

## Prerequisites

- Docker
- Docker Compose
- SBT
- Scala

## Algorithm Explanation

    Single-Threaded Execution Context:
        Ensures all operations on the KafkaConsumer are sequential, preventing ConcurrentModificationException.

    Polling for Records:
        Uses consumer.poll to fetch records and process them within the single-threaded context.

    Processing Records:
        Updates partition lag and commits offsets after processing each record.
        Simulates slow processing with a 2-second delay.

    Rebalancing Handling:
        CustomRebalanceListener handles partition revocation and assignment.
        Commits offsets before rebalancing to avoid duplication.
        Resets lag history and partition lag map upon reassignment.

    Lag Monitoring:
        Tracks lag history and evaluates stability based on grace periods and non-consuming polls.

## Getting Started

1. Start the Kafka and Zookeeper services by running the following command:

```bash
docker compose -f docker-compose.yml up
```

2. Create the JAR file by running the following command:

```bash
sbt assembly
```

3. Create the docker image by running the following command:

```bash
docker build -t billsoum/example-app .
```

4. Start the Kafka Consumer Service in swarm mode by running the following command:

```bash
docker stack deploy -c docker-compose.yml example-app
```

## Considerations

By default the topic test_topic is created with only 1 partition.

To change the number of partitions you can list the docker containers and find the container id for
the kafka container. Then you can run the following command:

```bash
docker exec -it <container_id> bash
```

Then you can run the following command to update the with more partitions:

```bash
kafka-topics --bootstrap-server localhost:9092 --alter --topic test_topic --partitions 3
```

This will cause rebalancing of the partitions and the consumer will start consuming from the new
partitions.

Also, in case you want to change the number of replicas for the service you can update the
docker-compose-swarm.yml file
and modify the replicas field and run the following command:

```bash
docker stack deploy -c docker-compose-swarm.yml example-app
```

There is also a commented class called `KafkaMessageProducer` that can be used to produce messages
to the topic.

