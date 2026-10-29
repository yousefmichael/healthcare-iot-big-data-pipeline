#!/usr/bin/env python3
"""Continuously publish synthetic healthcare IoT events to Kafka.

Data-quality injection rates are independent of --anomaly-rate. Raw Kafka payloads
never include anomaly labels or data-quality labels. Optional local evaluation truth
is written to a separate file and must not be sent to Kafka or uploaded with raw data.
"""
import argparse, json, random, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaProducer


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--vital-topic", default="vital-readings")
    p.add_argument("--device-topic", default="device-status-events")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between event pairs")
    p.add_argument("--anomaly-rate", type=float, default=0.18, help="probability of a simulated anomaly, independent of data-quality issues")
    p.add_argument("--null-rate", type=float, default=0.04, help="probability of injecting null/missing fields per event")
    p.add_argument("--invalid-rate", type=float, default=0.025, help="probability of injecting invalid values/formats per event")
    p.add_argument("--duplicate-rate", type=float, default=0.01, help="probability of sending a duplicate event")
    p.add_argument("--broken-reference-rate", type=float, default=0.01, help="probability of injecting a broken device/patient reference")
    p.add_argument("--evaluation-file", default="evaluation_only/streaming_ground_truth.jsonl", help="local-only ground truth file; never published to Kafka")
    p.add_argument("--no-evaluation-file", action="store_true", help="disable local ground-truth output")
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args()
    if args.interval < 0: p.error("--interval must be >= 0")
    for name in ("anomaly_rate", "null_rate", "invalid_rate", "duplicate_rate", "broken_reference_rate"):
        if not 0 <= getattr(args, name) <= 0.5: p.error(f"--{name.replace('_','-')} must be between 0 and 0.5")
    rng = random.Random(args.seed)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"), acks="all", retries=5,
    )
    metrics = {
        "heart_rate": {"unit":"bpm", "mean":72, "sd":10, "low":35, "high":190},
        "spo2": {"unit":"percent", "mean":97, "sd":1.5, "low":70, "high":100},
        "temperature": {"unit":"celsius", "mean":36.8, "sd":0.35, "low":30, "high":42},
        "systolic_bp": {"unit":"mmHg", "mean":120, "sd":12, "low":60, "high":240},
    }
    eval_path = None
    eval_handle = None
    if not args.no_evaluation_file:
        eval_path = Path(args.evaluation_file)
        eval_path.parent.mkdir(parents=True, exist_ok=True)
        eval_handle = eval_path.open("a", encoding="utf-8")
    i = 0
    print(f"Publishing to {args.vital_topic} and {args.device_topic} at {args.bootstrap_servers}; Ctrl+C to stop.")
    print("Ground truth is local-only; do not upload it to raw S3 or publish it to Kafka.")
    try:
        while True:
            i += 1
            device = f"D{rng.randint(1,120):04d}"
            patient = f"P{rng.randint(1,500):05d}"
            metric = rng.choice(list(metrics))
            spec = metrics[metric]
            value = round(rng.gauss(spec["mean"], spec["sd"]), 2)
            reading_id = f"SR-{uuid.uuid4().hex[:16]}"
            reading = {"reading_id":reading_id,"device_id":device,"patient_id":patient,"admission_id":"",
                       "metric_type":metric,"metric_value":value,"unit":spec["unit"],"measured_at":now_iso()}
            anomaly = rng.random() < args.anomaly_rate
            anomaly_kind = None
            if anomaly:
                anomaly_kind = rng.choice(["high_reading", "low_reading", "missing_reading", "implausible_reading"])
                if anomaly_kind == "high_reading": reading["metric_value"] = round(rng.uniform(spec["high"]+1, spec["high"]+25), 2)
                elif anomaly_kind == "low_reading": reading["metric_value"] = round(rng.uniform(max(0,spec["low"]-20), spec["low"]-1), 2)
                elif anomaly_kind == "missing_reading": reading["metric_value"] = None
                else: reading["metric_value"] = round(rng.choice([rng.uniform(-20,-1), rng.uniform(spec["high"]+30,spec["high"]+100)]), 2)
                if eval_handle:
                    eval_handle.write(json.dumps({"record_id":reading_id,"record_type":"vital_reading","ground_truth_type":"anomaly","anomaly_type":anomaly_kind,"event_time":reading["measured_at"]})+"\n")
                    eval_handle.flush()

            # Data-quality corruption is separate from the anomaly injection above.
            dq_roll = rng.random()
            if dq_roll < args.null_rate:
                field = rng.choice(["patient_id", "metric_value", "unit", "measured_at"])
                reading[field] = None
                dq_kind = "null"
            elif dq_roll < args.null_rate + args.invalid_rate:
                field = rng.choice(["metric_value", "metric_type", "measured_at", "unit"])
                reading[field] = {"metric_value":"NOT_A_NUMBER", "metric_type":"unknown_metric", "measured_at":"not-a-timestamp", "unit":"unknown_unit"}[field]
                dq_kind = "invalid"
            elif dq_roll < args.null_rate + args.invalid_rate + args.broken_reference_rate:
                field = rng.choice(["device_id", "patient_id", "admission_id"])
                reading[field] = "MISSING_REF_999999"
                dq_kind = "broken_reference"
            else:
                dq_kind = None
            producer.send(args.vital_topic, key=device, value=reading)
            if dq_kind and eval_handle:
                eval_handle.write(json.dumps({"record_id":reading_id,"record_type":"vital_reading","ground_truth_type":"data_quality_issue","issue_type":dq_kind,"field_name":field,"event_time":reading.get("measured_at")})+"\n")
                eval_handle.flush()
            if rng.random() < args.duplicate_rate:
                producer.send(args.vital_topic, key=device, value=reading)
                if eval_handle:
                    eval_handle.write(json.dumps({"record_id":reading_id,"record_type":"vital_reading","ground_truth_type":"data_quality_issue","issue_type":"duplicate","event_time":reading.get("measured_at")})+"\n")
                    eval_handle.flush()

            status = rng.choice(["disconnected", "sensor_error", "battery_low"]) if rng.random() < args.anomaly_rate else rng.choice(["online", "heartbeat", "reconnected"])
            event_id = f"SE-{uuid.uuid4().hex[:16]}"
            event = {"event_id":event_id,"device_id":device,"event_type":status,"event_time":now_iso()}
            status_is_anomaly = status in {"disconnected", "sensor_error", "battery_low"}
            if status_is_anomaly and eval_handle:
                eval_handle.write(json.dumps({"record_id":event_id,"record_type":"device_status_event","ground_truth_type":"anomaly","anomaly_type":status,"event_time":event["event_time"]})+"\n")
                eval_handle.flush()
            dq_roll = rng.random()
            if dq_roll < args.null_rate:
                field = rng.choice(["device_id", "event_type", "event_time"])
                event[field] = None
                dq_kind = "null"
            elif dq_roll < args.null_rate + args.invalid_rate:
                field = rng.choice(["event_type", "event_time"])
                event[field] = "NOT_A_VALID_VALUE"
                dq_kind = "invalid"
            elif dq_roll < args.null_rate + args.invalid_rate + args.broken_reference_rate:
                field = "device_id"
                event[field] = "D999999"
                dq_kind = "broken_reference"
            else:
                dq_kind = None
            producer.send(args.device_topic, key=device, value=event)
            if dq_kind and eval_handle:
                eval_handle.write(json.dumps({"record_id":event_id,"record_type":"device_status_event","ground_truth_type":"data_quality_issue","issue_type":dq_kind,"field_name":field,"event_time":event.get("event_time")})+"\n")
                eval_handle.flush()
            if rng.random() < args.duplicate_rate:
                producer.send(args.device_topic, key=device, value=event)
                if eval_handle:
                    eval_handle.write(json.dumps({"record_id":event_id,"record_type":"device_status_event","ground_truth_type":"data_quality_issue","issue_type":"duplicate","event_time":event.get("event_time")})+"\n")
                    eval_handle.flush()
            producer.flush(timeout=10)
            print(f"sent reading={reading_id} device_event={event_id}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        if eval_handle:
            eval_handle.close()
if __name__ == "__main__": main()
