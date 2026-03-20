"""HealthKit parser — reads per-metric JSON files from Apple Health export.

Expects a directory with per-metric JSON files (e.g. data/healthkit/history/).
Generates one record per day with natural-language health narratives.

This is a complex parser with anomaly detection, unit conversions, and
multi-source sleep data. See the original parse_healthkit.py for full docs.
"""

import json
import logging
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

log = logging.getLogger("parsers")

_SKIP_FILES = {
    "environmental_audio_exposure.json", "stair_speed_down.json",
    "stair_speed_up.json", "apple_stand_hour.json", "apple_stand_time.json",
}

_CONVERSIONS = {
    "walking_running_distance": 1.60934,
    "weight_body_mass": 0.453592,
    "lean_body_mass": 0.453592,
    "walking_speed": 1.60934,
    "walking_step_length": 2.54,
}

_TEMP_METRICS = {"apple_sleeping_wrist_temperature"}


def _load_metrics(history_dir: Path) -> dict[str, dict]:
    """Load all metric files, merge into daily records."""
    daily: dict[str, dict] = defaultdict(dict)

    for fpath in sorted(history_dir.glob("*.json")):
        if fpath.name in _SKIP_FILES or fpath.name == "workouts.json":
            continue
        if "_full_res" in fpath.name:
            continue

        with open(fpath) as f:
            raw = json.load(f)
        metric = raw["metric"]
        for item in raw.get("data", []):
            date = (item.get("date") or item.get("startDate", ""))[:10]
            if not date:
                continue

            if metric == "sleep_analysis":
                source = item.get("source", "")
                key = "sleep_watch" if "Watch" in source or "watch" in source else "sleep_oura"
                total = item.get("totalSleep", item.get("asleep", 0))
                existing = daily[date].get(key)
                if existing is None or total > existing.get("totalSleep", 0):
                    daily[date][key] = {
                        "totalSleep": total, "deep": item.get("deep", 0),
                        "rem": item.get("rem", 0), "core": item.get("core", 0),
                        "inBedStart": item.get("inBedStart", ""),
                        "inBedEnd": item.get("inBedEnd", ""),
                    }
            elif metric == "heart_rate":
                daily[date]["heart_rate"] = {
                    "avg": item.get("Avg", 0), "min": item.get("Min", 0),
                    "max": item.get("Max", 0),
                }
            elif metric in _TEMP_METRICS:
                val = item.get("qty", 0)
                daily[date][metric] = round((val - 32) * 5 / 9, 1)
            else:
                val = item.get("qty", 0)
                if metric in _CONVERSIONS:
                    val *= _CONVERSIONS[metric]
                daily[date][metric] = val

    return dict(daily)


def _load_workouts(history_dir: Path) -> dict[str, list[dict]]:
    wf = history_dir / "workouts.json"
    if not wf.exists():
        return {}
    with open(wf) as f:
        raw = json.load(f)
    by_date: dict[str, list] = defaultdict(list)
    for w in raw.get("workouts", []):
        date = (w.get("start", ""))[:10]
        if not date:
            continue
        name = w.get("name", w.get("workoutActivityType", "Unknown"))
        dur_min = w.get("duration", 0) / 60
        ae = w.get("activeEnergyBurned", {})
        kcal = ae.get("qty", 0) if isinstance(ae, dict) else ae
        by_date[date].append({"name": name, "duration_min": round(dur_min, 1), "kcal": round(kcal)})
    return dict(by_date)


def _get_sleep(rec: dict) -> tuple[dict | None, str]:
    watch = rec.get("sleep_watch")
    oura = rec.get("sleep_oura")
    if watch and watch.get("totalSleep", 0) > 2:
        return watch, "Watch"
    if oura and oura.get("totalSleep", 0) > 0:
        return oura, "Oura"
    return (watch, "Watch") if watch else (None, "")


def _fmt(val, d=1):
    if val is None:
        return ""
    return f"{int(round(val)):,}" if d == 0 else f"{val:.{d}f}"


def _narrative(date: str, rec: dict, workouts: list[dict]) -> str:
    lines = [f"Health: {date}"]
    sleep, src = _get_sleep(rec)
    if sleep and sleep["totalSleep"] > 0:
        parts = [f"{_fmt(sleep['totalSleep'])}h total"]
        for k, label in [("deep", "deep"), ("rem", "REM"), ("core", "core")]:
            if sleep.get(k, 0) > 0:
                parts.append(f"{_fmt(sleep[k])}h {label}")
        lines.append(f"Sleep: {parts[0]} ({', '.join(parts[1:])}) [{src}]")

    rhr = rec.get("resting_heart_rate")
    hrv = rec.get("heart_rate_variability")
    hr_parts = []
    if rhr:
        hr_parts.append(f"Resting HR: {_fmt(rhr, 0)} bpm")
    if hrv:
        hr_parts.append(f"HRV: {_fmt(hrv, 0)}ms")
    if hr_parts:
        lines.append(". ".join(hr_parts))

    act_parts = []
    for key, label in [("step_count", "steps"), ("walking_running_distance", "km"),
                        ("active_energy", "kcal active")]:
        v = rec.get(key)
        if v:
            act_parts.append(f"{_fmt(v, 0)} {label}" if "step" in key or "kcal" in label else f"{_fmt(v)} {label}")
    if act_parts:
        lines.append("Activity: " + ", ".join(act_parts))

    if workouts:
        wo = [f"{w['name']} {w['duration_min']:.0f}min" for w in workouts]
        lines.append("Workouts: " + "; ".join(wo))

    return "\n".join(lines)


def parse(path: Path | None = None, **kwargs) -> Iterator[dict]:
    """Yield one record per day from HealthKit data.

    Args:
        path: Path to healthkit history directory containing per-metric JSONs.
    """
    if path is None:
        raise ValueError("healthkit parser requires path to history directory")
    if not path.is_dir():
        raise FileNotFoundError(f"HealthKit directory not found: {path}")

    daily = _load_metrics(path)
    workouts = _load_workouts(path)

    count = 0
    for date in sorted(daily):
        rec = daily[date]
        day_workouts = workouts.get(date, [])
        text = _narrative(date, rec, day_workouts)

        if len(text.split("\n")) <= 1:
            continue

        sleep, src = _get_sleep(rec)
        meta: dict = {"channel": "operational"}
        if sleep:
            meta["sleep_hours"] = round(sleep["totalSleep"], 1)
        if rec.get("resting_heart_rate"):
            meta["resting_hr"] = round(rec["resting_heart_rate"])
        if rec.get("step_count"):
            meta["steps"] = round(rec["step_count"])

        yield {
            "id": f"healthkit_{date}",
            "source": "healthkit",
            "title": f"[healthkit] {date}",
            "date": date,
            "text": text,
            "metadata": meta,
        }
        count += 1

    log.info(f"HealthKit: emitted {count} daily entries")
