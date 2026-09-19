import asyncio
import aiohttp
import random
import time
import statistics
import json

URL = "http://127.0.0.1:8000"
# URL = "http://172.18.0.13:8001"

latencies = []
kafka_latencies = 0.00
completed = 0
errors = 0
logs = open("logs.txt", "a", buffering=1)
REQUIRED_CARS = 10000


async def ensure_cars(session, required_cars):
    async with session.get(f"{URL}/last") as response:
        if response.status != 200:
            raise RuntimeError(
                f"Failed to get last car: {response.status}"
            )

        last = await response.json()

    if last is None:
        current = 0
    else:
        current = last["car_id"]

    print(f"Current cars: {current}")

    missing = required_cars - current

    if missing <= 0:
        print(f"Already have at least {required_cars} cars")
        return

    print(f"Adding {missing} cars...")

    for _ in range(missing):
        async with session.post(
            f"{URL}/addcars",
            json={"owner": "benchmark"}
        ) as response:

            if response.status >= 400:
                text = await response.text()
                raise RuntimeError(
                    f"Failed to add car: HTTP {response.status}: {text}"
                )

            data = await response.json()

    print(f"Now have {required_cars} cars")

async def ensure_locations(session, required_cars):
    return
    for car_id in range(1, required_cars + 1):
        async with session.post(
            f"{URL}/addlocation/{car_id}",
            json={
                "car_long": random.random() * 100,
                "car_lat": random.random() * 100,
            },
        ) as response:

            if response.status >= 400:
                text = await response.text()
                raise RuntimeError(
                    f"Failed to add location for car {car_id}: "
                    f"HTTP {response.status}: {text}"
                )

    print(f"Initialized locations for {required_cars} cars")

async def worker(session, worker_id, end_time   ):
    global completed, errors, kafka_latencies
    while time.perf_counter() < end_time:
        car_id = (worker_id % 10000) + 1
        start = time.perf_counter()

        try:
            async with session.put(
                f"{URL}/location/{car_id}",
                json={
                    "car_long": random.random() * 100,
                    "car_lat": random.random() * 100,
                },
            ) as response:

                text = await response.text()

                latency = time.perf_counter() - start

                if response.status >= 400:
                    errors += 1
                    logs.write(
                        f"HTTP {response.status} "
                        f"car_id={car_id}: {text}\n"
                    )                    
                    logs.flush()
                    continue

                latencies.append(latency)
                completed += 1

                try:
                    data = json.loads(text)
                    kafka_latencies += (data["queue_time"])
                except (json.JSONDecodeError, KeyError, ValueError):
                    print("Error: Json", flush=True)
                    logs.write(f"{data}\n")
                    logs.flush()
                    pass

        except Exception as e:
            errors += 1
            logs.write(f"{type(e).__name__}: {e}\n")
            logs.flush()


async def main(CONCURRENCY):
    timeout = aiohttp.ClientTimeout(total=None)

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY,
        limit_per_host=CONCURRENCY,
        ttl_dns_cache=300,
        force_close=False,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
    ) as session:

        end_time = time.perf_counter() + DURATION

        tasks = [
            asyncio.create_task(worker(session, i, end_time))
            for i in range(CONCURRENCY)
        ]

        start = time.perf_counter()

        await asyncio.gather(*tasks)

        elapsed = time.perf_counter() - start

    print()
    print("========== RESULTS ==========")
    print(f"Concurrency:  {CONCURRENCY}")
    print(f"Requests:     {completed}")
    print(f"Errors:       {errors}")
    print(f"Duration:     {elapsed:.3f} sec")
    print(f"RPS:          {completed / elapsed:.2f}")
    print(f"Kafka que:    {kafka_latencies / completed:.2f}")

    if latencies:
        latencies.sort()

        def percentile(values, p):
            index = int(len(values) * p / 100)
            index = min(index, len(values) - 1)
            return values[index] * 1000

        print(f"Latency avg:  {statistics.mean(latencies) * 1000:.3f} ms")
        print(f"Latency p99:  {percentile(latencies, 99):.3f} ms")

    # if db_latencies:
    #     print(f"DB avg:       {statistics.mean(db_latencies) * 1000:.3f} ms")
    #     print(f"DB p99:       {percentile(db_latencies, 99):.3f} ms")


DURATION = 5

async def setup():
    timeout = aiohttp.ClientTimeout(total=None)

    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=100,
        ttl_dns_cache=300,
        force_close=False,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
    ) as session:
        await ensure_cars(session, REQUIRED_CARS)
        await ensure_locations(session, REQUIRED_CARS)


asyncio.run(setup())



for CONCURRENCY in range(50, 300, 50):
    latencies = []
    kafka_latencies = 0
    completed = 0
    errors = 0
    asyncio.run(main(CONCURRENCY))