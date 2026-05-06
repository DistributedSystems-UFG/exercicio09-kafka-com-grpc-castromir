"""
Cliente gRPC de exemplo: consulta último agregado, lista por sensor e histórico.
Requer api_server.py em execução. Execute a partir desta pasta: python grpc_client.py
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import grpc
import temperature_service_pb2
import temperature_service_pb2_grpc

from const import GRPC_CLIENT_TARGET


def run() -> None:
    logging.basicConfig(level=logging.INFO)
    sensor_id = "sensor-lab-a1"
    now = datetime.now(timezone.utc)
    from_iso = (now - timedelta(hours=6)).isoformat()

    with grpc.insecure_channel(GRPC_CLIENT_TARGET) as channel:
        stub = temperature_service_pb2_grpc.TemperatureQueryStub(channel)

        try:
            latest = stub.GetLatestAggregate(temperature_service_pb2.SensorId(sensor_id=sensor_id))
            print("Último agregado:", latest)
        except grpc.RpcError as e:
            print("GetLatestAggregate:", e.code(), e.details())

        all_latest = stub.ListLatestAggregates(temperature_service_pb2.Empty())
        print("Último por sensor (lista):", list(all_latest.items))

        hist = stub.GetHistory(
            temperature_service_pb2.HistoryQuery(
                sensor_id=sensor_id,
                from_iso=from_iso,
                to_iso="",
            )
        )
        print(f"Histórico desde {from_iso}: {len(hist.items)} registro(s)")
        for item in hist.items:
            print(" -", item)


if __name__ == "__main__":
    run()
