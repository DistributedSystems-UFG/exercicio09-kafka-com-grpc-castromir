import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SENSOR_READINGS = "sensor-readings"
TOPIC_TEMPERATURE_AGGREGATES = "temperature-aggregates"

WINDOW_HOURS = float(os.environ.get("AGG_WINDOW_HOURS", "2"))

GRPC_PORT = os.environ.get("GRPC_PORT", "50051")
GRPC_BIND = os.environ.get("GRPC_BIND", f"[::]:{GRPC_PORT}")
GRPC_CLIENT_TARGET = os.environ.get("GRPC_CLIENT_TARGET", f"localhost:{GRPC_PORT}")

DB_PATH = os.environ.get("TEMPERATURE_DB_PATH", "temperature.db")

SENSOR_PUBLISH_DELTA_C = float(os.environ.get("SENSOR_DELTA_C", "0.5"))
