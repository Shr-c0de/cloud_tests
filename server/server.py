import subprocess
from pathlib import Path
from fastapi import FastAPI
from psycopg_pool import ConnectionPool
import signal
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row
from pydantic import BaseModel
import anyio.to_thread
import time    
import string

BASE = Path(__file__).parent / "postgres"
# REST server starts
pool = ConnectionPool(
    "host=postgres port=5432 dbname=cars user=shreyas password=password",
    min_size=1,
    max_size=1  ,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool.wait()
    print("[API] server started")

    yield

    print("[API] shutting down")
    pool.close()


app = FastAPI(lifespan=lifespan)


class CarCreate(BaseModel):
    owner: str


class LocationCreate(BaseModel):
    car_long: float
    car_lat: float


@app.get("/")
def root():
    return {"message": "API is running"}


@app.get("/cars")
def get_cars():
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT car_id, owner
                FROM car_info
                ORDER BY car_id;
            """)

            return cur.fetchall()
            
@app.get("/last")
def get_last():
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT car_id, owner
                FROM car_info
                ORDER BY car_id desc;
            """)

            return cur.fetchone()

@app.post("/addcars")
def add_car(car: CarCreate):
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:

            # Create car and get generated car_id
            cur.execute("""
                INSERT INTO car_info (owner)
                VALUES (%s)
                RETURNING car_id;
            """, (car.owner,))

            return cur.fetchone()

@app.post("/addlocation/{car_id}")
def add_loc(car_id: int, location: LocationCreate):
    req_begin = time.clock_gettime()

    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            conn_time = time.clock_gettime()
            cur.execute("""
                INSERT INTO location (car_id, car_long, car_lat)
                VALUES (%s, %s, %s)
                RETURNING car_id, car_long, car_lat;
            """, (
                car_id,
                location.car_long,
                location.car_lat
            ))
            db_time = time.clock_gettime()
            return {"message":"success"}




@app.put("/location/{car_id}")
def update_car(car_id: int, location: LocationCreate):
    req_begin = time.clock_gettime(time.CLOCK_MONOTONIC)

    with pool.connection() as conn:
        conn_time = time.clock_gettime(time.CLOCK_MONOTONIC)

        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                UPDATE location
                SET car_long = %s,
                    car_lat = %s
                WHERE car_id = %s
            """, (
                location.car_long,
                location.car_lat,
                car_id
            ))

            db_time = time.clock_gettime(time.CLOCK_MONOTONIC)

            # print(
            #     f"connection time: {conn_time - req_begin}\n"
            #     f"db time: {db_time - conn_time}",
            # )

            if cur.rowcount <= 0:
                return {"error": "Location not found"}

    return {"message": "Update Successful", "databse time":f"{db_time-req_begin}"}

@app.get("/location")
def get_location():
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT car_id, car_long, car_lat
                FROM location
                ORDER BY car_id;
            """)

            return cur.fetchall()


@app.get("/location/{car_id}")
def get_location(car_id: int):
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT car_id, car_long, car_lat
                FROM location
                WHERE car_id = %s;
            """, (car_id,))

            result = cur.fetchone()

            if result is None:
                return {"error": "Location not found"}

            return result