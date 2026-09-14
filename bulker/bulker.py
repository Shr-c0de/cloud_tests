import grpc
import database_pb2
import database_pb2_grpc

import time
import queue
import threading

from concurrent import futures
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row


pool = ConnectionPool(
    "host=postgres port=5432 dbname=cars user=shreyas password=password",
    min_size=1,
    max_size=10,
)


BATCH_SIZE = 1000
FLUSH_INTERVAL = 0.030  # 30 ms

update_queue = queue.Queue()


class UpdateRequest:
    def __init__(self, car_id, car_long, car_lat):
        self.car_id = car_id
        self.car_long = car_long
        self.car_lat = car_lat

        self.done = threading.Event()
        self.success = False
        self.db_time = 0.0


def bulk_worker():

    while True:

        batch = []

        # Wait for the first request
        request = update_queue.get()
        batch.append(request)

        # Start the batching timer
        deadline = time.perf_counter() + FLUSH_INTERVAL

        # Collect requests for up to FLUSH_INTERVAL
        while len(batch) < BATCH_SIZE:

            remaining = deadline - time.perf_counter()

            if remaining <= 0:
                break

            try:
                request = update_queue.get(
                    timeout=remaining
                )
                batch.append(request)

            except queue.Empty:
                break

        # Execute the batch
        start_db = time.perf_counter()

        try:

            with pool.connection() as conn:
                with conn.cursor() as cur:

                    # Build one bulk UPDATE statement
                    values = []

                    for request in batch:
                        values.extend([
                            request.car_id,
                            request.car_long,
                            request.car_lat,
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
                        ) AS v(car_id, car_long, car_lat)
                        WHERE l.car_id = v.car_id;
                    """

                    cur.execute(query, values)

                    # IDs that actually existed
                    updated_ids = {
                        row[0]
                        for row in cur.execute(
                            f"""
                            SELECT car_id
                            FROM location
                            WHERE car_id IN (
                                {", ".join(["%s"] * len(batch))}
                            );
                            """,
                            [request.car_id for request in batch]
                        )
                    }

                    for request in batch:
                        request.success = (
                            request.car_id in updated_ids
                        )

        except Exception as e:

            print(
                f"[BULK] Database error: {e}",
                flush=True
            )

            for request in batch:
                request.success = False

        db_time = time.perf_counter() - start_db

        # Notify waiting gRPC calls
        for request in batch:
            request.db_time = db_time
            request.done.set()


class CarService(database_pb2_grpc.CarServicer):

    def Update(self, request, context):

        update_queue.put(
            UpdateRequest(
                request.car_id,
                request.car_long,
                request.car_lat
            )
        )

        return database_pb2.LocationResponse(
            success=True,
            car_id=request.car_id,
            car_long=request.car_long,
            car_lat=request.car_lat,
            db_time=0
        )


    def Add(self, request, context):

        with pool.connection() as conn:

            with conn.cursor(
                row_factory=dict_row
            ) as cur:

                cur.execute("""
                    INSERT INTO car_info (owner)
                    VALUES (%s)
                    RETURNING car_id, owner;
                """, (
                    request.owner,
                ))

                row = cur.fetchone()

        return database_pb2.CarResponse(
            car_id=row["car_id"],
            owner=row["owner"]
        )


    def AddLocation(self, request, context):

        with pool.connection() as conn:

            with conn.cursor(
                row_factory=dict_row
            ) as cur:

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
            car_id=row["car_id"],
            car_long=row["car_long"],
            car_lat=row["car_lat"]
        )


def serve():

    # Start bulk worker
    worker = threading.Thread(
        target=bulk_worker,
        daemon=True
    )

    worker.start()

    # Start gRPC server
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
        pool.close()


if __name__ == "__main__":
    serve()