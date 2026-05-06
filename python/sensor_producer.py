"""
Simula um sensor de temperatura: publica leituras no Kafka apenas quando
há variação significativa em relação à última leitura enviada.
Execute a partir desta pasta: python sensor_producer.py
"""
from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from const import KAFKA_BOOTSTRAP, SENSOR_PUBLISH_DELTA_C, TOPIC_SENSOR_READINGS


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    sensor_id = "sensor-lab-a1"
    temp_c = 22.0 + random.uniform(-0.3, 0.3)
    last_sent: float | None = None

    print(f"Sensor {sensor_id} → tópico {TOPIC_SENSOR_READINGS} @ {KAFKA_BOOTSTRAP}")
    print(f"Publica quando |Δ| >= {SENSOR_PUBLISH_DELTA_C} °C (Ctrl+C para sair)")

    try:
        while True:
            drift = random.uniform(-0.35, 0.35)
            temp_c = max(-50.0, min(60.0, temp_c + drift))

            should_send = last_sent is None or abs(temp_c - last_sent) >= SENSOR_PUBLISH_DELTA_C
            if should_send:
                payload = {
                    "sensor_id": sensor_id,
                    "celsius": round(temp_c, 2),
                    "observed_at": _now_iso(),
                }
                future = producer.send(TOPIC_SENSOR_READINGS, value=payload)
                try:
                    future.get(timeout=10)
                except KafkaError as e:
                    print(f"Erro ao enviar: {e}")
                else:
                    print(f"Enviado: {payload}")
                    last_sent = temp_c

            time.sleep(random.uniform(0.4, 1.2))
    except KeyboardInterrupt:
        print("\nEncerrando produtor.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
