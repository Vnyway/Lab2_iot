import json
from datetime import datetime
from typing import List, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import (
    Column, DateTime, Float, Integer, MetaData, String, Table, create_engine
)

import config

# SQLAlchemy setup
DATABASE_URL = (
    f"postgresql+psycopg2://{config.POSTGRES_USER}:{config.POSTGRES_PASSWORD}"
    f"@{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB}"
)
engine = create_engine(DATABASE_URL)
metadata = MetaData()

processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)


# Pydantic models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @field_validator("timestamp", mode="before")
    @classmethod
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI app
app = FastAPI()

# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()


@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.discard(websocket)


async def send_data_to_subscribers(data: list):
    for websocket in subscriptions.copy():
        try:
            await websocket.send_json(json.dumps(data, default=str))
        except Exception:
            subscriptions.discard(websocket)


# CRUD endpoints
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    with engine.connect() as conn:
        inserted = []
        for item in data:
            result = conn.execute(
                processed_agent_data.insert().values(
                    road_state=item.road_state,
                    x=item.agent_data.accelerometer.x,
                    y=item.agent_data.accelerometer.y,
                    z=item.agent_data.accelerometer.z,
                    latitude=item.agent_data.gps.latitude,
                    longitude=item.agent_data.gps.longitude,
                    timestamp=item.agent_data.timestamp,
                ).returning(*processed_agent_data.c)
            )
            row = result.fetchone()
            inserted.append(dict(row._mapping))
        conn.commit()

    await send_data_to_subscribers(inserted)
    return inserted


@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int):
    with engine.connect() as conn:
        result = conn.execute(
            processed_agent_data.select().where(
                processed_agent_data.c.id == processed_agent_data_id
            )
        ).fetchone()
    if result is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return dict(result._mapping)


@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data():
    with engine.connect() as conn:
        results = conn.execute(processed_agent_data.select()).fetchall()
    return [dict(row._mapping) for row in results]


@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def update_processed_agent_data(
    processed_agent_data_id: int,
    data: ProcessedAgentData,
):
    with engine.connect() as conn:
        result = conn.execute(
            processed_agent_data.update()
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .values(
                road_state=data.road_state,
                x=data.agent_data.accelerometer.x,
                y=data.agent_data.accelerometer.y,
                z=data.agent_data.accelerometer.z,
                latitude=data.agent_data.gps.latitude,
                longitude=data.agent_data.gps.longitude,
                timestamp=data.agent_data.timestamp,
            )
            .returning(*processed_agent_data.c)
        ).fetchone()
        conn.commit()
    if result is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return dict(result._mapping)


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    with engine.connect() as conn:
        result = conn.execute(
            processed_agent_data.delete()
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .returning(*processed_agent_data.c)
        ).fetchone()
        conn.commit()
    if result is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return dict(result._mapping)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
