from __future__ import annotations

import json
import os
import time
from datetime import datetime

import paho.mqtt.client as mqtt
import psycopg


def connect_db() -> psycopg.Connection:
    while True:
        try:
            return psycopg.connect(
                host=os.getenv("DB_HOST", "db"),
                port=os.getenv("DB_PORT", "5432"),
                dbname=os.getenv("DB_NAME", "bracelet_connecte"),
                user=os.getenv("DB_USER", "bracelet"),
                password=os.getenv("DB_PASSWORD", "bracelet"),
            )
        except Exception as exc:  # pragma: no cover - defensive retry loop
            print(f"Database unavailable, retrying: {exc}")
            time.sleep(3)


def ensure_bracelet(conn: psycopg.Connection, payload: dict[str, object]) -> int:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO bracelets (device_uid, serial_number, display_name, firmware_version, status, last_seen_at)
            VALUES (%s, %s, %s, %s, 'active', NOW())
            ON CONFLICT (device_uid) DO UPDATE
            SET serial_number = EXCLUDED.serial_number,
                display_name = EXCLUDED.display_name,
                last_seen_at = NOW(),
                updated_at = NOW()
            RETURNING id
            """,
            (
                payload["device_uid"],
                payload["serial_number"],
                payload.get("display_name", "Bracelet de test"),
                payload.get("firmware_version"),
            ),
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("Failed to upsert bracelet")
        return int(row[0])


def insert_measurement(conn: psycopg.Connection, bracelet_id: int, payload: dict[str, object]) -> None:
    captured_at = payload.get("captured_at") or datetime.utcnow().isoformat()
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO biometric_measurements (
                bracelet_id,
                captured_at,
                heart_rate_bpm,
                spo2_percent,
                step_count,
                motion_level,
                signal_quality,
                raw_payload,
                source_topic,
                received_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW())
            RETURNING id
            """,
            (
                bracelet_id,
                captured_at,
                payload.get("heart_rate_bpm"),
                payload.get("spo2_percent"),
                payload.get("step_count", 0),
                payload.get("motion_level"),
                payload.get("signal_quality"),
                json.dumps(payload.get("raw_payload", {})),
                payload.get("source_topic"),
            ),
        )
        measurement_row = cursor.fetchone()
        if measurement_row is None:
            raise RuntimeError("Failed to insert measurement")
        measurement_id = int(measurement_row[0])

        samples = payload.get("samples", [])
        for sample in samples:
            cursor.execute(
                """
                INSERT INTO biometric_measurement_samples (
                    measurement_id,
                    sample_type,
                    sample_index,
                    sample_value
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (measurement_id, sample_type, sample_index) DO UPDATE
                SET sample_value = EXCLUDED.sample_value
                """,
                (
                    measurement_id,
                    sample["sample_type"],
                    sample["sample_index"],
                    sample["sample_value"],
                ),
            )


class Bridge:
    def __init__(self) -> None:
        self.topic = os.getenv("MQTT_TOPIC", "bracelets/+/measurements")
        self.mqtt_host = os.getenv("MQTT_HOST", "mqtt-broker")
        self.mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
        self.mqtt_user = os.getenv("MQTT_USER", "mqtt")
        self.mqtt_password = os.getenv("MQTT_PASSWORD", "mqtt")
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-db-bridge")
        self.client.username_pw_set(self.mqtt_user, self.mqtt_password)
        self.conn = connect_db()

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client: mqtt.Client, userdata, flags, reason_code, properties) -> None:  # noqa: ANN001
        print(f"Connected to MQTT broker with reason {reason_code}")
        client.subscribe(self.topic, qos=1)

    def on_message(self, client: mqtt.Client, userdata, message: mqtt.MQTTMessage) -> None:  # noqa: ANN001
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            with self.conn.transaction():
                bracelet_id = ensure_bracelet(self.conn, payload)
                insert_measurement(self.conn, bracelet_id, payload)
        except Exception as exc:  # pragma: no cover - defensive logging path
            print(f"Failed to persist MQTT payload: {exc}")

    def run(self) -> None:
        while True:
            try:
                self.client.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
                self.client.loop_forever()
            except Exception as exc:
                print(f"MQTT unavailable, retrying: {exc}")
                time.sleep(3)


def main() -> None:
    Bridge().run()


if __name__ == "__main__":
    main()