"""
Serviço gRPC + consumidor Kafka: persiste agregados recebidos em SQLite
e expõe consultas (último valor, histórico).
Execute a partir desta pasta: python api_server.py
"""
from __future__ import annotations

import json
import logging
import threading
from concurrent import futures

import grpc
import temperature_service_pb2
import temperature_service_pb2_grpc
from kafka import KafkaConsumer

from const import GRPC_BIND, KAFKA_BOOTSTRAP, TOPIC_TEMPERATURE_AGGREGATES
from db_store import AggregateStore


def _row_to_proto(row) -> temperature_service_pb2.TemperatureAggregate:
    return temperature_service_pb2.TemperatureAggregate(
        id=row["id"],
        sensor_id=row["sensor_id"],
        window_start_iso=row["window_start_iso"],
        window_end_iso=row["window_end_iso"],
        avg_celsius=row["avg_celsius"],
        sample_count=row["sample_count"],
        computed_at_iso=row["computed_at_iso"],
    )


class TemperatureQueryService(temperature_service_pb2_grpc.TemperatureQueryServicer):
    def __init__(self, store: AggregateStore):
        self._store = store

    def GetLatestAggregate(self, request, context):
        row = self._store.get_latest(request.sensor_id)
        if row is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"sensor_id={request.sensor_id}")
        return _row_to_proto(row)

    def ListLatestAggregates(self, request, context):
        rows = self._store.list_latest_per_sensor()
        out = temperature_service_pb2.TemperatureAggregateList()
        for r in rows:
            out.items.append(_row_to_proto(r))
        return out

    def GetHistory(self, request, context):
        rows = self._store.history(
            request.sensor_id,
            request.from_iso or None,
            request.to_iso or None,
        )
        out = temperature_service_pb2.TemperatureAggregateList()
        for r in rows:
            out.items.append(_row_to_proto(r))
        return out


def _kafka_persist_loop(store: AggregateStore, stop: threading.Event) -> None:
    consumer = KafkaConsumer(
        TOPIC_TEMPERATURE_AGGREGATES,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id="api-persist",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    logging.info("Kafka: consumindo %s", TOPIC_TEMPERATURE_AGGREGATES)
    try:
        for msg in consumer:
            if stop.is_set():
                break
            p = msg.value
            store.insert(
                sensor_id=p["sensor_id"],
                window_start_iso=p["window_start_iso"],
                window_end_iso=p["window_end_iso"],
                avg_celsius=float(p["avg_celsius"]),
                sample_count=int(p["sample_count"]),
                computed_at_iso=p["computed_at_iso"],
            )
            logging.info("Persistido agregado sensor=%s avg=%s", p["sensor_id"], p["avg_celsius"])
    finally:
        consumer.close()


def serve() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    store = AggregateStore()
    stop = threading.Event()
    t = threading.Thread(target=_kafka_persist_loop, args=(store, stop), daemon=True)
    t.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    temperature_service_pb2_grpc.add_TemperatureQueryServicer_to_server(
        TemperatureQueryService(store), server
    )
    server.add_insecure_port(GRPC_BIND)
    server.start()
    logging.info("gRPC em %s (SQLite: %s)", GRPC_BIND, store.path)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        stop.set()
        server.stop(0)


if __name__ == "__main__":
    serve()
