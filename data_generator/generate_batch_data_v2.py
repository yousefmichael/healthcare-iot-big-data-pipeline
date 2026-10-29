#!/usr/bin/env python3
"""Generate synthetic healthcare IoT batch datasets with configurable data-quality issues.

Raw records contain no anomaly labels. Evaluation-only anomaly and data-quality
ground truth is written separately under evaluation_only/.
"""
from __future__ import annotations
import argparse, csv, json, random
from datetime import datetime, timedelta, timezone
from pathlib import Path

def iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def choose_issue(rng, rates):
    x = rng.random()
    cumulative = 0
    for name, rate in rates:
        cumulative += rate
        if x < cumulative:
            return name
    return None

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", default="generated_data")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--wards", type=int, default=60)
    p.add_argument("--beds", type=int, default=1600)
    p.add_argument("--patients", type=int, default=5000)
    p.add_argument("--devices", type=int, default=1000)
    p.add_argument("--admissions", type=int, default=300)
    p.add_argument("--device-assignments", type=int, default=300)
    p.add_argument("--readings", type=int, default=500000)
    p.add_argument("--device-events", type=int, default=20000)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--anomaly-rate", type=float, default=0.18,
                   help="Target fraction of abnormal vital readings and device events; separate from data-quality rates")
    p.add_argument("--null-rate", type=float, default=0.04, help="Fraction of eligible records to inject null/missing fields into")
    p.add_argument("--invalid-rate", type=float, default=0.025, help="Fraction of eligible records to inject invalid values/formats into")
    p.add_argument("--duplicate-rate", type=float, default=0.01, help="Fraction of eligible event records to duplicate")
    p.add_argument("--broken-reference-rate", type=float, default=0.01, help="Fraction of eligible records with a deliberately broken device/patient reference")
    args = p.parse_args()
    rates = [args.anomaly_rate, args.null_rate, args.invalid_rate, args.duplicate_rate, args.broken_reference_rate]
    if any(not 0 <= x <= 0.5 for x in rates): p.error("all rates must be between 0 and 0.5")
    if min(args.wards, args.beds, args.patients, args.devices, args.admissions, args.device_assignments, args.readings, args.device_events, args.days) < 1:
        p.error("counts and days must be positive")
    rng = random.Random(args.seed)
    out = Path(args.output_dir)
    eval_dir = out / "evaluation_only"
    start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=args.days)

    # 1. Wards (default: 60)
    base_wards = [
        ("North Medical", "medical"),
        ("South Medical", "medical"),
        ("Cardiology", "cardiac"),
        ("Surgical", "surgical"),
        ("Critical Care", "icu"),
        ("Pediatrics", "pediatric"),
    ]
    wards = []
    for i in range(1, args.wards + 1):
        base_name, typ = base_wards[(i - 1) % len(base_wards)]
        unit_num = (i - 1) // len(base_wards) + 1
        ward_name = base_name if args.wards <= len(base_wards) else f"{base_name} {unit_num}"
        wards.append({
            "ward_id": f"W{i:02d}",
            "ward_name": ward_name,
            "floor": (i - 1) // 6 + 1,
            "ward_type": typ
        })

    # 2. Beds (default: 1600)
    beds = []
    for i in range(1, args.beds + 1):
        ward = wards[(i - 1) % len(wards)]
        beds.append({
            "bed_id": f"B{i:04d}",
            "ward_id": ward["ward_id"],
            "bed_number": f"{(i - 1) // len(wards) + 1:02d}",
            "bed_status": rng.choices(["occupied", "available", "maintenance"], [0.68, 0.27, 0.05])[0]
        })

    # 3. Patients (default: 5000)
    patients = []
    for i in range(1, args.patients + 1):
        dob = start - timedelta(days=rng.randint(365, 365 * 90))
        patients.append({
            "patient_id": f"P{i:05d}",
            "name": f"Synthetic Patient {i:05d}",
            "date_of_birth": dob.date().isoformat(),
            "gender": rng.choice(["female", "male", "other", "unknown"])
        })

    # 4. Devices (default: 1000)
    devices = []
    device_types = ["vital_monitor", "pulse_oximeter", "thermometer", "blood_pressure_monitor"]
    for i in range(1, args.devices + 1):
        devices.append({
            "device_id": f"D{i:04d}",
            "device_type": device_types[(i - 1) % len(device_types)],
            "model": f"SIM-{(i - 1) % 12 + 1:02d}",
            "installed_at": iso(start - timedelta(days=rng.randint(10, 900)))
        })

    # 5. Admissions (default: 300)
    admissions = []
    for i in range(1, args.admissions + 1):
        admitted = start - timedelta(days=rng.randint(0, args.days + 15))
        discharged = "" if rng.random() < 0.7 else iso(admitted + timedelta(hours=rng.randint(12, 24 * 10)))
        admissions.append({
            "admission_id": f"A{i:05d}",
            "patient_id": patients[(i - 1) % len(patients)]["patient_id"],
            "bed_id": beds[(i - 1) % len(beds)]["bed_id"],
            "admitted_at": iso(admitted),
            "discharged_at": discharged,
            "status": "active" if not discharged else "discharged"
        })

    # 6. Device Assignments (default: 300)
    assignments = []
    for i in range(args.device_assignments):
        d = devices[i % len(devices)]
        patient = patients[i % len(patients)]
        admission = admissions[i % len(admissions)] if admissions else {}
        assigned = start - timedelta(days=rng.randint(0, args.days + 10))
        assignments.append({
            "assignment_id": f"AS{i + 1:05d}",
            "device_id": d["device_id"],
            "patient_id": patient["patient_id"],
            "admission_id": admission.get("admission_id", ""),
            "assigned_at": iso(assigned),
            "unassigned_at": ""
        })

    # Apply data-quality issues to source entity tables too, without changing headers.
    dq_truth = []
    entity_tables = [
        ("wards", wards, ["ward_name", "ward_type"], ["floor"]),
        ("beds", beds, ["ward_id", "bed_status"], ["bed_number"]),
        ("patients", patients, ["name", "date_of_birth", "gender"], []),
        ("admissions", admissions, ["patient_id", "bed_id", "admitted_at", "status"], ["admitted_at"]),
        ("devices", devices, ["device_type", "model", "installed_at"], ["installed_at"]),
        ("device_assignments", assignments, ["device_id", "patient_id", "assigned_at"], ["assigned_at"]),
    ]
    for table, rows, nullable_fields, invalid_fields in entity_tables:
        for idx, row in enumerate(rows):
            issue = choose_issue(rng, [("null", args.null_rate), ("invalid", args.invalid_rate), ("broken_reference", args.broken_reference_rate)])
            if not issue: continue
            field = rng.choice(nullable_fields or list(row.keys()))
            before = row.get(field, "")
            if issue == "null":
                row[field] = ""
            elif issue == "invalid":
                field = rng.choice(invalid_fields) if invalid_fields else field
                before = row.get(field, "")
                row[field] = "NOT_A_VALID_VALUE"
            else:
                if field.endswith("_id") and field not in ("ward_id",):
                    row[field] = "MISSING_REF_999999"
                elif field == "ward_id":
                    row[field] = "W999"
                else:
                    field = "patient_id" if "patient_id" in row else ("device_id" if "device_id" in row else field)
                    before = row.get(field, "")
                    row[field] = "MISSING_REF_999999"
            dq_truth.append({"record_type":table,"record_id":row.get(next((k for k in row if k.endswith("_id")), ""),""),"issue_type":issue,"field_name":field,"original_value":before,"injected_value":row.get(field,""),"description":f"Injected {issue} into {table}.{field}"})

    for name, rows in [("wards",wards),("beds",beds),("patients",patients),("admissions",admissions),("devices",devices),("device_assignments",assignments)]:
        write_csv(out/f"{name}.csv", rows, list(rows[0]))

    metric_specs={"heart_rate":("bpm",72,10,35,190),"spo2":("percent",97,1.5,70,100),"temperature":("celsius",36.8,0.35,30,42),"systolic_bp":("mmHg",120,12,60,240)}
    readings=[]; scenarios=[]; scenario_id=0
    dq_counts={"null":0,"invalid":0,"duplicate":0,"broken_reference":0}
    for i in range(args.readings):
        dev=devices[i%len(devices)]
        patient=patients[i%len(patients)]
        admission=admissions[i%len(admissions)] if admissions else {}
        metric=rng.choice(list(metric_specs))
        unit,mean,sd,low,high=metric_specs[metric]
        value=round(rng.gauss(mean,sd),2)
        ts=start+timedelta(seconds=rng.randint(0,args.days*24*3600))
        if rng.random() < args.anomaly_rate:
            scenario_id+=1
            kind=rng.choice(["high_reading","low_reading","missing_reading","implausible_reading"])
            if kind=="high_reading": value=round(rng.uniform(high+1, high+25),2)
            elif kind=="low_reading": value=round(rng.uniform(max(0,low-20), low-1),2)
            elif kind=="missing_reading": value=""
            else: value=round(rng.choice([rng.uniform(-20,-1),rng.uniform(high+30,high+100)]),2)
            scenarios.append({"scenario_id":f"SC{scenario_id:06d}","record_id":f"R{i+1:08d}","device_id":dev["device_id"],"metric_type":metric,"start_time":iso(ts),"end_time":iso(ts+timedelta(minutes=rng.randint(1,15))),"anomaly_type":kind,"severity":rng.choice(["low","medium","high"])})
        row={"reading_id":f"R{i+1:08d}","device_id":dev["device_id"],"patient_id":patient["patient_id"],"admission_id":admission.get("admission_id",""),"metric_type":metric,"metric_value":value,"unit":unit,"measured_at":iso(ts)}
        issue=choose_issue(rng,[("null",args.null_rate),("invalid",args.invalid_rate),("broken_reference",args.broken_reference_rate)])
        if issue=="null":
            field=rng.choice(["patient_id","metric_value","unit","measured_at"]); row[field]=""; dq_counts["null"]+=1
        elif issue=="invalid":
            field=rng.choice(["metric_value","metric_type","measured_at","unit"])
            row[field]={"metric_value":"NOT_A_NUMBER","metric_type":"unknown_metric","measured_at":"not-a-timestamp","unit":"unknown_unit"}[field]; dq_counts["invalid"]+=1
        elif issue=="broken_reference":
            field=rng.choice(["device_id","patient_id","admission_id"])
            row[field]="MISSING_REF_999999"; dq_counts["broken_reference"]+=1
        readings.append(row)
        if rng.random() < args.duplicate_rate:
            readings.append(dict(row)); dq_counts["duplicate"]+=1
            dq_truth.append({"record_type":"vital_readings","record_id":row["reading_id"],"issue_type":"duplicate","field_name":"reading_id","original_value":row["reading_id"],"injected_value":row["reading_id"],"description":"Repeated record with same reading_id"})
        if issue:
            dq_truth.append({"record_type":"vital_readings","record_id":row["reading_id"],"issue_type":issue,"field_name":field,"original_value":"","injected_value":row.get(field,""),"description":f"Injected {issue} into vital_readings.{field}"})
    write_csv(out/"vital_readings.csv",readings,list(readings[0]))

    event_types=["online","heartbeat","battery_low","disconnected","reconnected","sensor_error"]
    events=[]; event_scenarios=[]
    for i in range(args.device_events):
        dev=devices[i%len(devices)]
        ts=start+timedelta(seconds=rng.randint(0,args.days*24*3600))
        abnormal=rng.random()<args.anomaly_rate
        etype=rng.choice(["disconnected","sensor_error","battery_low"]) if abnormal else rng.choice(["online","heartbeat","reconnected"])
        row={"event_id":f"E{i+1:08d}","device_id":dev["device_id"],"event_type":etype,"event_time":iso(ts)}
        if abnormal:
            event_scenarios.append({"scenario_id":f"EVSC{i+1:08d}","record_id":row["event_id"],"device_id":dev["device_id"],"metric_type":"","start_time":iso(ts),"end_time":iso(ts+timedelta(minutes=rng.randint(1,30))),"anomaly_type":etype,"severity":rng.choice(["low","medium","high"])})
        issue=choose_issue(rng,[("null",args.null_rate),("invalid",args.invalid_rate),("broken_reference",args.broken_reference_rate)])
        if issue=="null":
            field=rng.choice(["device_id","event_type","event_time"]); row[field]=""; dq_counts["null"]+=1
        elif issue=="invalid":
            field=rng.choice(["event_type","event_time"]); row[field]="NOT_A_VALID_VALUE"; dq_counts["invalid"]+=1
        elif issue=="broken_reference":
            field="device_id"; row[field]="D999999"; dq_counts["broken_reference"]+=1
        events.append(row)
        if rng.random() < args.duplicate_rate:
            events.append(dict(row)); dq_counts["duplicate"]+=1
            dq_truth.append({"record_type":"device_status_events","record_id":row["event_id"],"issue_type":"duplicate","field_name":"event_id","original_value":row["event_id"],"injected_value":row["event_id"],"description":"Repeated event with same event_id"})
        if issue:
            dq_truth.append({"record_type":"device_status_events","record_id":row["event_id"],"issue_type":issue,"field_name":field,"original_value":"","injected_value":row.get(field,""),"description":f"Injected {issue} into device_status_events.{field}"})
    event_path=out/"device_status_events.jsonl"
    event_path.mkdir(parents=True,exist_ok=True) if False else event_path.parent.mkdir(parents=True,exist_ok=True)
    with event_path.open("w",encoding="utf-8") as f:
        for row in events: f.write(json.dumps(row,separators=(",",":"))+"\n")
    all_scenarios=scenarios+event_scenarios
    write_csv(eval_dir/"anomaly_scenarios.csv",all_scenarios,["scenario_id","record_id","device_id","metric_type","start_time","end_time","anomaly_type","severity"])
    write_csv(eval_dir/"data_quality_issues.csv",dq_truth,["record_type","record_id","issue_type","field_name","original_value","injected_value","description"])
    manifest={"generated_at":iso(datetime.now(timezone.utc)),"seed":args.seed,"rates":{"anomaly_rate_target":args.anomaly_rate,"null_rate":args.null_rate,"invalid_rate":args.invalid_rate,"duplicate_rate":args.duplicate_rate,"broken_reference_rate":args.broken_reference_rate},
              "counts":{"wards":len(wards),"beds":len(beds),"patients":len(patients),"admissions":len(admissions),"devices":len(devices),"device_assignments":len(assignments),"vital_readings_including_duplicates":len(readings),"device_status_events_including_duplicates":len(events),"evaluation_only_anomaly_scenarios":len(all_scenarios),"evaluation_only_data_quality_issue_records":len(dq_truth),"data_quality_injected_counts":dq_counts}}
    (out/"manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
    print(f"Generated datasets in: {out.resolve()}")
    print(json.dumps(manifest["counts"],indent=2))
    print(f"Evaluation ground truth is separate under: {eval_dir.resolve()}")

if __name__=="__main__": main()