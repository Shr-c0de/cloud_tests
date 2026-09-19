import grpc
import os
import database_pb2
import database_pb2_grpc
import json
import time    

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))


channel = None
car_stub = None


@asynccontextmanager
async def lifespan(app: FastAPI):

    global channel, car_stub

    print("[POST] Starting gRPC channel", flush=True)

    BULKER_HOST = os.getenv(
        "BULKER_HOST",
        "bulker-1:50051"
    )

    channel = grpc.aio.insecure_channel(BULKER_HOST)

    car_stub = database_pb2_grpc.CarStub(
        channel
    )

    yield

    print("[POST] Closing gRPC channel", flush=True)

    await channel.close()



app = FastAPI(
    lifespan=lifespan
)


class CarCreate(BaseModel):
    owner: str


class LocationCreate(BaseModel):
    car_long: float
    car_lat: float


@app.post("/addcars")
async def add_car(car: CarCreate):

    response = await car_stub.Add(
        database_pb2.CarCreate(
            owner=car.owner
        )
    )

    return {
        "car_id": response.car_id,
        "owner": response.owner
    }


@app.post("/addlocation/{car_id}")
async def add_loc(
    car_id: int,
    location: LocationCreate
):

    response = await car_stub.AddLocation(
        database_pb2.Location(
            car_id=car_id,
            car_long=location.car_long,
            car_lat=location.car_lat
        )
    )

    if not response.success:
        raise HTTPException(
            status_code=400,
            detail="Failed to add location"
        )

    return {
        "message": "success",
        "car_id": response.car_id,
        "car_long": response.car_long,
        "car_lat": response.car_lat
    }


@app.put("/location/{car_id}")
async def update_car(
    car_id: int,
    location: LocationCreate
):
    conn_time = time.clock_gettime(time.CLOCK_MONOTONIC)
    producer.send("post-handler", {
        "car_id": f"{car_id}",
        "car_long": f"{location.car_long}",
        "car_lat": f"{location.car_lat}"
    })
    producer.flush()
    kafka = time.clock_gettime(time.CLOCK_MONOTONIC)
    return {
        "message": "Successful",
        "car_id": car_id,
        "car_long": location.car_long,
        "car_lat": location.car_lat,
        "queue_time":kafka - conn_time
    }