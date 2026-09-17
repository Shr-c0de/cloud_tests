import grpc
import database_pb2
import database_pb2_grpc

import json
import time
import threading

from concurrent import futures

from kafka import KafkaProducer, KafkaConsumer

from psycopg_pool import ConnectionPool



# ---------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------

pool = ConnectionPool(
    "host=postgres port=5432 dbname=cars user=shreyas password=password",
    min_size=1,
    max_size=10,
)


# ---------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",

    # Convert Python dict -> JSON -> bytes
    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),

    # Make sure messages are acknowledged by Kafka
    acks="all",

    # Retry transient failures
    retries=5,
)


# ---------------------------------------------------------
# Kafka Consumer
# ---------------------------------------------------------

consumer = KafkaConsumer(
    "post-handler",

    bootstrap_servers="kafka:9092",

    group_id="processor",

    auto_offset_reset="earliest",

    # IMPORTANT:
    # We manually commit only after PostgreSQL succeeds.
    enable_auto_commit=False,

    # We don't want Kafka to give us enormous batches.
    max_poll_records=1000,

    value_deserializer=lambda value:
        json.loads(value.decode("utf-8")),
)


BATCH_SIZE = 1000
FLUSH_INTERVAL = 0.030  # 30 ms


# ---------------------------------------------------------
# Kafka consumer / DB worker
# ---------------------------------------------------------

def bulk_worker():

    while True:

        batch = []

        # -------------------------------------------------
        # Wait for first Kafka message
        # -------------------------------------------------

        records = consumer.poll(
            timeout_ms=1000,
            max_records=BATCH_SIZE
        )

        if not records:
            continue

        # Kafka returns:
        #
        # {
        #     TopicPartition(...): [
        #         ConsumerRecord(...),
        #         ConsumerRecord(...),
        #     ]
        # }
        #
        # Flatten it.

        for messages in records.values():
            print(messages)
            batch.extend(messages)

        # -------------------------------------------------
        # Collect more messages for up to FLUSH_INTERVAL
        # -------------------------------------------------

        deadline = time.perf_counter() + FLUSH_INTERVAL

        while len(batch) < BATCH_SIZE:

            remaining = deadline - time.perf_counter()

            if remaining <= 0:
                break

            records = consumer.poll(
                timeout_ms=max(1, int(remaining * 1000)),
                max_records=BATCH_SIZE - len(batch)
            )

            if not records:
                break

            for messages in records.values():
                batch.extend(messages)

                if len(batch) >= BATCH_SIZE:
                    break

        # -------------------------------------------------
        # Write batch to PostgreSQL
        # -------------------------------------------------

        start_db = time.perf_counter()

        try:

            with pool.connection() as conn:

                with conn.cursor() as cur:

                    values = []

                    for message in batch:

                        values.extend([
                            message.value["car_id"],
                            message.value["car_long"],
                            message.value["car_lat"],
                        ])

                    placeholders = ", ".join(
                        ["(%s, %s, %s)"] * len(batch)
                    )

                    query = f"""
                        UPDATE location AS l
                        SET
                            car_long = v.car_long,
                            car_lat = v.car_lat
                        FROM (
                            VALUES {placeholders}
                        ) AS v(
                            car_id,
                            car_long,
                            car_lat
                        )
                        WHERE l.car_id = v.car_id;
                    """

                    cur.execute(query, values)

            # -------------------------------------------------
            # DB succeeded.
            #
            # Now tell Kafka that these messages are processed.
            # -------------------------------------------------

            consumer.commit()

            db_time = time.perf_counter() - start_db

            print(
                f"[KAFKA] processed {len(batch)} messages "
                f"in {db_time * 1000:.2f} ms",
                flush=True
            )

        except Exception as e:

            print(
                f"[KAFKA] Database error: {e}",
                flush=True
            )

            # IMPORTANT:
            #
            # We DO NOT commit the Kafka offsets.
            #
            # Therefore these messages will be processed again
            # after the consumer restarts/rebalances.
            #

            time.sleep(1)


# ---------------------------------------------------------
# gRPC Service
# ---------------------------------------------------------

class CarService(database_pb2_grpc.CarServicer):

    def Update(self, request, context):

        message = {
            "car_id": request.car_id,
            "car_long": request.car_long,
            "car_lat": request.car_lat,
        }

        try:

            # Kafka key is useful because messages for the same
            # car can be routed to the same partition.
            #
            # With your current 1 partition topic this doesn't
            # make a difference yet, but it becomes useful if
            # you increase the number of partitions.

            future = producer.send(
                "post-handler",
                key=str(request.car_id).encode("utf-8"),
                value=message
            )

            # Wait until Kafka acknowledges the message.
            metadata = future.get(timeout=10)

            return database_pb2.LocationResponse(
                success=True,
                car_id=request.car_id,
                car_long=request.car_long,
                car_lat=request.car_lat,
                db_time=0
            )

        except Exception as e:

            print(
                f"[KAFKA] Producer error: {e}",
                flush=True
            )

            context.set_code(
                grpc.StatusCode.UNAVAILABLE
            )

            context.set_details(
                "Failed to write update to Kafka"
            )

            return database_pb2.LocationResponse(
                success=False,
                car_id=request.car_id,
                car_long=request.car_long,
                car_lat=request.car_lat,
                db_time=0
            )


    def Add(self, request, context):

        with pool.connection() as conn:

            with conn.cursor() as cur:

                cur.execute("""
                    INSERT INTO car_info (owner)
                    VALUES (%s)
                    RETURNING car_id, owner;
                """, (
                    request.owner,
                ))

                row = cur.fetchone()

        return database_pb2.CarResponse(
            car_id=row[0],
            owner=row[1]
        )


    def AddLocation(self, request, context):

        with pool.connection() as conn:

            with conn.cursor() as cur:

                cur.execute("""
                    INSERT INTO location (
                        car_id,
                        car_long,
                        car_lat
                    )
                    VALUES (%s, %s, %s)
                    RETURNING car_id, car_long, car_lat;
                """, (
                    request.car_id,
                    request.car_long,
                    request.car_lat
                ))

                row = cur.fetchone()

        return database_pb2.LocationResponse(
            success=True,
            car_id=row[0],
            car_long=row[1],
            car_lat=row[2]
        )


# ---------------------------------------------------------
# gRPC Server
# ---------------------------------------------------------

def serve():

    # Start Kafka -> PostgreSQL worker

    worker = threading.Thread(
        target=bulk_worker,
        daemon=True
    )

    worker.start()

    # Start gRPC

    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=50
        )
    )

    database_pb2_grpc.add_CarServicer_to_server(
        CarService(),
        server
    )

    server.add_insecure_port(
        "[::]:50051"
    )

    server.start()

    print(
        "[GRPC] server started on port 50051",
        flush=True
    )

    try:

        server.wait_for_termination()

    finally:

        producer.flush()
        producer.close()

        consumer.close()

        pool.close()


# ---------------------------------------------------------

if __name__ == "__main__":
    serve()