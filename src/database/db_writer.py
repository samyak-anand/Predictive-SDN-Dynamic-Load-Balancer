import json
import logging
import os
import signal
import time

import psycopg
from psycopg import sql
from kafka import KafkaConsumer


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "traffic.validated"
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "postgres-db-writer"
)

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432"
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "sdn_traffic"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "sdn_user"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "sdn_password"
)

BATCH_SIZE = int(
    os.getenv("DB_BATCH_SIZE", "5000")
)

FLUSH_INTERVAL_SECONDS = int(
    os.getenv("DB_FLUSH_INTERVAL_SECONDS", "5")
)


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("db_writer")


# ---------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------

running = True


def shutdown_handler(signum, frame):
    global running

    logger.info(
        "Shutdown signal received. Finishing current batch..."
    )

    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ---------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------

def create_db_connection():
    connection_string = (
        f"host={POSTGRES_HOST} "
        f"port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )

    return psycopg.connect(connection_string)


# ---------------------------------------------------------
# Value conversion
# ---------------------------------------------------------

def get_value(payload, *keys):
    """
    Return the first available value from the payload.

    Allows the writer to handle slightly different
    field naming conventions.
    """

    for key in keys:
        if key in payload:
            return payload[key]

    return None


def convert_timestamp(value):
    """
    PostgreSQL TIMESTAMPTZ accepts ISO timestamp strings.
    Return None when the value is missing.
    """

    if value is None:
        return None

    return value


# ---------------------------------------------------------
# Kafka message -> PostgreSQL row
# ---------------------------------------------------------

def message_to_row(message):
    """
    Convert one validated Kafka message into a PostgreSQL row.
    """

    payload = message.value

    if not isinstance(payload, dict):
        raise ValueError(
            f"Expected JSON object but received "
            f"{type(payload).__name__}"
        )

    event_id = get_value(
        payload,
        "event_id",
        "eventId",
        "id"
    )

    if event_id is None:
        raise ValueError(
            "Validated message does not contain event_id"
        )

    row = (
        str(event_id),

        convert_timestamp(
            get_value(
                payload,
                "event_timestamp",
                "timestamp",
                "event_time",
                "eventTime"
            )
        ),

        get_value(
            payload,
            "source_node",
            "sourceNode",
            "src_node"
        ),

        get_value(
            payload,
            "destination_node",
            "destinationNode",
            "dst_node"
        ),

        get_value(
            payload,
            "source_ip",
            "sourceIp",
            "src_ip"
        ),

        get_value(
            payload,
            "destination_ip",
            "destinationIp",
            "dst_ip"
        ),

        get_value(
            payload,
            "protocol"
        ),

        get_value(
            payload,
            "packet_count",
            "packetCount"
        ),

        get_value(
            payload,
            "byte_count",
            "byteCount"
        ),

        get_value(
            payload,
            "duration_ms",
            "durationMs"
        ),

        get_value(
            payload,
            "throughput_mbps",
            "throughputMbps"
        ),

        get_value(
            payload,
            "packet_loss_percent",
            "packetLossPercent"
        ),

        get_value(
            payload,
            "latency_ms",
            "latencyMs"
        ),

        json.dumps(payload),

        message.topic,
        message.partition,
        message.offset
    )

    return row


# ---------------------------------------------------------
# Batch insert
# ---------------------------------------------------------

INSERT_QUERY = """
INSERT INTO traffic_data (
    event_id,
    event_timestamp,
    source_node,
    destination_node,
    source_ip,
    destination_ip,
    protocol,
    packet_count,
    byte_count,
    duration_ms,
    throughput_mbps,
    packet_loss_percent,
    latency_ms,
    raw_payload,
    kafka_topic,
    kafka_partition,
    kafka_offset
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s
)kafka_data:
  postgres_data:(.venv) samyak@samyak:~/PycharmProjects/Predictive-SDN-Dynamic-Load-Balancer$ docker stats --no-stream predictive-sdn-kafka
CONTAINER ID   NAME                   CPU %     MEM USAGE / LIMIT     MEM %     NET I/O           BLOCK I/O        PIDS
86f38967ad77   predictive-sdn-kafka   3.92%     693.9MiB / 11.42GiB   5.93%     2.34MB / 1.94MB   101MB / 26.6MB   105
(.venv) samyak@samyak:~/PycharmProjects/Predictive-SDN-Dynamic-Load-Balancer$ docker exec -it predictive-sdn-kafka bash -c 'cat /etc/kafka/docker/server.properties 2>/dev/null || true'
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

################## Why We Need This Separate Config ####################

# While our latest version supports dynamic voters configuration,
# we will continue using static voter configurations in our Docker Hub images.
# This decision ensures broader compatibility across different versions and
# maintains consistent behavior for existing deployments.
# By retaining static voter implementation in our Docker images, we can provide
# a more stable and predictable environment for users across various versions of the application.

############################# Server Basics #############################

# The role of this server. Setting this puts us in KRaft mode
process.roles=broker,controller

# The node id associated with this instance's roles
node.id=1

# The connect string for the controller quorum
controller.quorum.voters=1@localhost:9093

############################# Socket Server Settings #############################

# The address the socket server listens on.
# Combined nodes (i.e. those with `process.roles=broker,controller`) must list the controller listener here at a minimum.
# If the broker listener is not defined, the default listener will use a host name that is equal to the value of java.net.InetAddress.getCanonicalHostName(),
# with PLAINTEXT listener name, and port 9092.
#   FORMAT:
#     listeners = listener_name://host_name:port
#   EXAMPLE:
#     listeners = PLAINTEXT://your.host.name:9092
listeners=PLAINTEXT://:9092,CONTROLLER://:9093

# Name of listener used for communication between brokers.
inter.broker.listener.name=PLAINTEXT

# Listener name, hostname and port the broker or the controller will advertise to clients.
# If not set, it uses the value for "listeners".
advertised.listeners=PLAINTEXT://localhost:9092

# A comma-separated list of the names of the listeners used by the controller.
# If no explicit mapping set in `listener.security.protocol.map`, default will be using PLAINTEXT protocol
# This is required if running in KRaft mode.
controller.listener.names=CONTROLLER

# Maps listener names to security protocols, the default is for them to be the same. See the config documentation for more details
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL

# The number of threads that the server uses for receiving requests from the network and sending responses to the network
num.network.threads=3

# The number of threads that the server uses for processing requests, which may include disk I/O
num.io.threads=8

# The send buffer (SO_SNDBUF) used by the socket server
socket.send.buffer.bytes=102400

# The receive buffer (SO_RCVBUF) used by the socket server
socket.receive.buffer.bytes=102400

# The maximum size of a request that the socket server will accept (protection against OOM)
socket.request.max.bytes=104857600


############################# Log Basics #############################

# A comma separated list of directories under which to store log files
log.dirs=/tmp/kraft-combined-logs

# The default number of log partitions per topic. More partitions allow greater
# parallelism for consumption, but this will also result in more files across
# the brokers.
num.partitions=1

# The number of threads per data directory to be used for log recovery at startup and flushing at shutdown.
# This value is recommended to be increased for installations with data dirs located in RAID array.
num.recovery.threads.per.data.dir=1

############################# Internal Topic Settings  #############################
# The replication factor for the group metadata internal topics "__consumer_offsets", "__share_group_state" and "__transaction_state"
# For anything other than development testing, a value greater than 1 is recommended to ensure availability such as 3.
offsets.topic.replication.factor=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

############################# Log Flush Policy #############################

# Messages are immediately written to the filesystem but by default we only fsync() to sync
# the OS cache lazily. The following configurations control the flush of data to disk.
# There are a few important trade-offs here:
#    1. Durability: Unflushed data may be lost if you are not using replication.
#    2. Latency: Very large flush intervals may lead to latency spikes when the flush does occur as there will be a lot of data to flush.
#    3. Throughput: The flush is generally the most expensive operation, and a small flush interval may lead to excessive seeks.
# The settings below allow one to configure the flush policy to flush data after a period of time or
# every N messages (or both). This can be done globally and overridden on a per-topic basis.

# The number of messages to accept before forcing a flush of data to disk
#log.flush.interval.messages=10000

# The maximum amount of time a message can sit in a log before we force a flush
#log.flush.interval.ms=1000

############################# Log Retention Policy #############################

# The following configurations control the disposal of log segments. The policy can
# be set to delete segments after a period of time, or after a given size has accumulated.
# A segment will be deleted whenever *either* of these criteria are met. Deletion always happens
# from the end of the log.

# The minimum age of a log file to be eligible for deletion due to age
log.retention.hours=168

# A size-based retention policy for logs. Segments are pruned from the log unless the remaining
# segments drop below log.retention.bytes. Functions independently of log.retention.hours.
#log.retention.bytes=1073741824

# The maximum size of a log segment file. When this size is reached a new log segment will be created.
log.segment.bytes=1073741824

# The interval at which log segments are checked to see if they can be deleted according
# to the retention policies
log.retention.check.interval.ms=300000
(.venv) samyak@samyak:~/PycharmProjects/Predictive-SDN-Dynamic-Load-Balancer$ (.venv) samyak@samyak:~/PycharmProjects/Predictive-SDN-Dynamic-Load-Balancer$ docker exec -it predictive-sdn-kafka bash -c 'cat /etc/kafka/docker/server.properties 2>/dev/null || true'
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

################## Why We Need This Separate Config ####################

# While our latest version supports dynamic voters configuration,
# we will continue using static voter configurations in our Docker Hub images.
# This decision ensures broader compatibility across different versions and
# maintains consistent behavior for existing deployments.
# By retaining static voter implementation in our Docker images, we can provide
# a more stable and predictable environment for users across various versions of the application.

############################# Server Basics #############################

# The role of this server. Setting this puts us in KRaft mode
#^Che License.  You may obtain a copy of the License atcompliance with2.0d-Balancer$ docker exec -it predictive-sdn-kafka bash -c 'cat /etc/kafka/doc
(.venv) samyak@samyak:~/PycharmProjects/Predictive-SDN-Dynamic-Load-Balancer$ 
ON CONFLICT (
    kafka_topic,
    kafka_partition,
    kafka_offset
)
DO NOTHING
"""


def insert_batch(connection, batch):
    """
    Insert a complete batch inside one PostgreSQL transaction.

    The Kafka offsets are committed only AFTER this function
    successfully commits the database transaction.
    """

    if not batch:
        return

    try:

        with connection.cursor() as cursor:

            cursor.executemany(
                INSERT_QUERY,
                batch
            )

        connection.commit()

        logger.info(
            "Inserted batch of %d records into PostgreSQL",
            len(batch)
        )

    except Exception:

        connection.rollback()

        logger.exception(
            "Database insertion failed. "
            "Transaction rolled back."
        )

        raise


# ---------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------

def create_consumer():

    return KafkaConsumer(
        KAFKA_TOPIC,

        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        group_id=KAFKA_GROUP_ID,

        enable_auto_commit=False,

        auto_offset_reset="earliest",

        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),

        consumer_timeout_ms=1000,

        max_poll_records=BATCH_SIZE
    )


# ---------------------------------------------------------
# Main DB writer
# ---------------------------------------------------------

def run():

    logger.info("Starting Kafka → PostgreSQL DB Writer")

    logger.info(
        "Kafka topic: %s",
        KAFKA_TOPIC
    )

    logger.info(
        "Kafka bootstrap server: %s",
        KAFKA_BOOTSTRAP_SERVERS
    )

    logger.info(
        "PostgreSQL: %s:%s/%s",
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB
    )

    logger.info(
        "Batch size: %d",
        BATCH_SIZE
    )

    consumer = create_consumer()

    connection = create_db_connection()

    logger.info(
        "Connected to PostgreSQL successfully"
    )

    batch = []

    last_flush = time.time()

    try:

        while running:

            records = consumer.poll(
                timeout_ms=1000,
                max_records=BATCH_SIZE
            )

            for _, messages in records.items():

                for message in messages:

                    try:

                        row = message_to_row(message)

                        batch.append(row)

                    except Exception:

                        logger.exception(
                            "Failed to transform Kafka message "
                            "partition=%s offset=%s",
                            message.partition,
                            message.offset
                        )

            current_time = time.time()

            should_flush = (
                len(batch) >= BATCH_SIZE
                or (
                    batch
                    and
                    current_time - last_flush
                    >= FLUSH_INTERVAL_SECONDS
                )
            )

            if should_flush:

                insert_batch(
                    connection,
                    batch
                )

                # IMPORTANT:
                # Kafka offsets are committed ONLY AFTER
                # PostgreSQL transaction succeeds.

                consumer.commit()

                logger.info(
                    "Kafka offsets committed successfully"
                )

                batch.clear()

                last_flush = current_time

    except KeyboardInterrupt:

        logger.info(
            "Keyboard interrupt received."
        )

    finally:

        # Flush remaining messages during shutdown
        if batch:

            try:

                insert_batch(
                    connection,
                    batch
                )

                consumer.commit()

                logger.info(
                    "Final batch inserted and offsets committed"
                )

            except Exception:

                logger.exception(
                    "Failed to flush final batch"
                )

        consumer.close()

        connection.close()

        logger.info(
            "DB writer stopped."
        )


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    run()