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
    print("[GET] server started")

    yield

    print("[GET] shutting down")
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