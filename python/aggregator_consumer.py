"""
Consumidor/produtor: lê leituras brutas, mantém janela móvel (últimas N horas)
e publica média e contagem de amostras no tópico de agregados.
Execute a partir desta pasta: python aggregator_consumer.py
"""
from __future__ import annotations

import json
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from const import (
    KAFKA_BOOTSTRAP,
    TOPIC_SENSOR_READINGS,
    TOPIC_TEMPERATURE_AGGREGATES,
    WINDOW_HOURS,
)


def _parse_ts(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _trim_window(
    samples: deque[tuple[datetime, float]], end: datetime, window: timedelta
) -> None:
    cutoff = end - window
    while samples and samples[0][0] < cutoff:
        samples.popleft()


def main() -> None:
    window = timedelta(hours=WINDOW_HOURS)
    by_sensor: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)

    consumer = KafkaConsumer(
        TOPIC_SENSOR_READINGS,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id="temperature-aggregator",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Consumindo {TOPIC_SENSOR_READINGS} → produzindo {TOPIC_TEMPERATURE_AGGREGATES}")
    print(f"Janela: {WINDOW_HOURS} h @ {KAFKA_BOOTSTRAP}")

    try:
        for msg in consumer:
            row = msg.value
            sensor_id = row["sensor_id"]
            celsius = float(row["celsius"])
            observed = _parse_ts(row["observed_at"])

            samples = by_sensor[sensor_id]
            samples.append((observed, celsius))
            _trim_window(samples, observed, window)

            if not samples:
                continue

            window_start = samples[0][0]
            window_end = samples[-1][0]
            vals = [t for _, t in samples]
            avg = sum(vals) / len(vals)
            out = {
                "sensor_id": sensor_id,
                "window_start_iso": window_start.astimezone(timezone.utc).isoformat(),
                "window_end_iso": window_end.astimezone(timezone.utc).isoformat(),
                "avg_celsius": round(avg, 3),
                "sample_count": len(samples),
                "computed_at_iso": datetime.now(timezone.utc).isoformat(),
            }
            future = producer.send(TOPIC_TEMPERATURE_AGGREGATES, value=out)
            try:
                future.get(timeout=10)
            except KafkaError as e:
                print(f"Erro ao publicar agregado: {e}")
            else:
                print(f"Agregado: {out}")
    except KeyboardInterrupt:
        print("\nEncerrando agregador.")
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
