import argparse
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import requests
except Exception:
    requests = None  # elevation optional


# ----------------------------
# Utilities
# ----------------------------
def haversine_m_np(lat1, lon1, lat2, lon2) -> np.ndarray:
    """
    Vectorized haversine distance in meters.
    lat1/lon1 arrays, lat2/lon2 scalars.
    """
    R = 6371000.0
    lat1 = np.radians(lat1.astype(float))
    lon1 = np.radians(lon1.astype(float))
    lat2 = math.radians(float(lat2))
    lon2 = math.radians(float(lon2))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * math.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
    return R * c


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_timestamp_series(ts: pd.Series) -> pd.Series:
    """
    Robust timestamp parse:
    - Try pandas datetime parse.
    - If timestamps are too coarse / duplicated, build a per-row 1Hz timeline.
    """
    t = pd.to_datetime(ts, errors="coerce")

    # If parsing failed badly, force 1Hz starting at epoch-like 0
    if t.isna().mean() > 0.5:
        base = pd.Timestamp("1970-01-01")
        return base + pd.to_timedelta(np.arange(len(ts)), unit="s")

    # If many duplicates or median diff is 0, treat as coarse and rebuild
    diffs = t.sort_values().diff().dt.total_seconds().dropna()
    if len(diffs) > 0:
        zero_frac = (diffs == 0).mean()
        med = diffs.median()
        if zero_frac > 0.2 or med == 0:
            base = t.dropna().iloc[0]
            return base + pd.to_timedelta(np.arange(len(ts)), unit="s")

    return t


@dataclass
class RSU:
    rsu_id: str
    lat: float
    lon: float


def parse_rsus(rsu_args: List[str]) -> List[RSU]:
    """
    --rsu "RSU1:36.11,-97.15"  (repeatable)
    """
    rsus: List[RSU] = []
    for item in rsu_args:
        try:
            name, coords = item.split(":")
            lat_s, lon_s = coords.split(",")
            rsus.append(RSU(name.strip(), float(lat_s), float(lon_s)))
        except Exception as e:
            raise ValueError(f"Bad --rsu format: {item}. Expected RSU_ID:lat,lon") from e
    if not rsus:
        raise ValueError("No RSUs provided. Use --rsu or --rsu-json.")
    return rsus


def load_rsus_from_json(path: str) -> List[RSU]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    rsus = []
    for r in obj.get("rsus", []):
        rsus.append(RSU(r["id"], float(r["lat"]), float(r["lon"])))
    if not rsus:
        raise ValueError(f"No RSUs found in {path}. Expect JSON with key 'rsus'.")
    return rsus


# ----------------------------
# Elevation (optional)  [EPQS version]
# ----------------------------
EPQS_URL = "https://epqs.nationalmap.gov/v1/json"


def load_elev_cache(cache_csv: str) -> Dict[Tuple[float, float], float]:
    if not cache_csv or not os.path.exists(cache_csv):
        return {}
    df = pd.read_csv(cache_csv)
    out: Dict[Tuple[float, float], float] = {}
    if df.empty:
        return out
    for _, r in df.iterrows():
        out[(float(r["lat_r"]), float(r["lon_r"]))] = float(r["elevation_m"])
    return out


def save_elev_cache(cache_csv: str, cache: Dict[Tuple[float, float], float]) -> None:
    if not cache_csv:
        return
    ensure_dir(os.path.dirname(cache_csv))
    rows = [{"lat_r": k[0], "lon_r": k[1], "elevation_m": v} for k, v in cache.items()]
    pd.DataFrame(rows).to_csv(cache_csv, index=False)


def _parse_epqs_elevation(j: dict) -> float:
    """
    EPQS responses can differ. Handle common shapes safely.
    """
    # Some code variants return top-level "value"
    if isinstance(j, dict) and "value" in j:
        return float(j["value"])

    # Common EPQS structure:
    # {"USGS_Elevation_Point_Query_Service": {"Elevation_Query": {"Elevation": 123.45, ...}}}
    svc = j.get("USGS_Elevation_Point_Query_Service", {}) if isinstance(j, dict) else {}
    q = svc.get("Elevation_Query", {}) if isinstance(svc, dict) else {}
    if isinstance(q, dict) and "Elevation" in q:
        return float(q["Elevation"])

    raise ValueError(f"Unrecognized EPQS JSON response shape: keys={list(j.keys())[:10] if isinstance(j, dict) else type(j)}")


def fetch_elevation_point_epqs(lat: float, lon: float, timeout: int = 10) -> float:
    """
    Single-point elevation query to EPQS (meters).
    EPQS expects lon as x, lat as y.
    """
    if requests is None:
        raise RuntimeError("requests is not installed. Run: pip install requests")

    params = {
        "x": float(lon),
        "y": float(lat),
        "units": "Meters",
        "wkid": 4326,
    }
    resp = requests.get(EPQS_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    return _parse_epqs_elevation(resp.json())


def add_elevation_column(
        df: pd.DataFrame,
        lat_col: str = "latitude",
        lon_col: str = "longitude",
        round_decimals: int = 5,
        sleep_s: float = 0.12,          # EPQS is per-point; keep a small delay
        cache_csv: Optional[str] = None,
        max_retries: int = 3,
) -> pd.DataFrame:
    """
    Adds elevation_m using USGS EPQS (per-point), with caching + unique rounded coords.
    If a point fails after retries, fill NaN for that coordinate (but do NOT crash the run).
    """
    if not {lat_col, lon_col}.issubset(df.columns):
        raise ValueError(f"Missing {lat_col}/{lon_col} columns for elevation.")

    cache: Dict[Tuple[float, float], float] = {}
    if cache_csv:
        cache = load_elev_cache(cache_csv)

    tmp = df[[lat_col, lon_col]].copy()
    tmp["lat_r"] = tmp[lat_col].round(round_decimals)
    tmp["lon_r"] = tmp[lon_col].round(round_decimals)

    uniq = tmp[["lat_r", "lon_r"]].drop_duplicates().reset_index(drop=True)

    missing: List[Tuple[float, float]] = []
    for _, r in uniq.iterrows():
        key = (float(r["lat_r"]), float(r["lon_r"]))
        if key not in cache:
            missing.append(key)

    print(f"Elevation (EPQS): unique coords={len(uniq)}, missing={len(missing)}")

    for idx, (lat_r, lon_r) in enumerate(missing, start=1):
        ok = False
        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                e = fetch_elevation_point_epqs(lat_r, lon_r, timeout=10)
                cache[(lat_r, lon_r)] = float(e)
                ok = True
                break
            except Exception as e:
                last_err = e
                time.sleep(0.4 * attempt)

        if not ok:
            print(f"Elevation point failed after {max_retries} retries. lat/lon=({lat_r},{lon_r}) err={last_err}")
            cache[(lat_r, lon_r)] = float("nan")

        time.sleep(sleep_s)
        if idx % 200 == 0:
            print(f"  elevation progress: {idx}/{len(missing)}")

    if cache_csv:
        save_elev_cache(cache_csv, cache)

    elev_map = {(k[0], k[1]): v for k, v in cache.items()}
    uniq["elevation_m"] = uniq.apply(
        lambda r: elev_map.get((float(r["lat_r"]), float(r["lon_r"])), np.nan),
        axis=1,
    )

    out = df.copy()
    out = out.join(tmp[["lat_r", "lon_r"]])
    out = out.merge(uniq, on=["lat_r", "lon_r"], how="left")
    out = out.drop(columns=["lat_r", "lon_r"])
    return out


# ----------------------------
# Core processing
# ----------------------------
def compute_time_features(metrics: pd.DataFrame) -> pd.DataFrame:
    metrics = metrics.copy()
    metrics["timestamp"] = parse_timestamp_series(metrics["timestamp"])
    metrics["timestamp"] = metrics["timestamp"].astype("datetime64[ns]")

    t0 = metrics["timestamp"].iloc[0]
    metrics["t_sec"] = (metrics["timestamp"] - t0).dt.total_seconds()

    metrics["dt_sec"] = metrics["timestamp"].diff().dt.total_seconds().fillna(0.0)
    metrics["has_pkts"] = (metrics["delta_bytes"] > 0).astype(int)

    gaps = np.zeros(len(metrics), dtype=float)
    current = 0.0
    for i in range(len(metrics)):
        if i == 0:
            current = 0.0
        else:
            if metrics.loc[metrics.index[i], "delta_bytes"] > 0:
                current = 0.0
            else:
                current += float(metrics.loc[metrics.index[i], "dt_sec"])
        gaps[i] = current
    metrics["time_since_last_pkt"] = gaps
    return metrics


def add_distance_features_metrics(metrics: pd.DataFrame, rsus: List[RSU]) -> pd.DataFrame:
    metrics = metrics.copy()
    lat = metrics["latitude"].to_numpy()
    lon = metrics["longitude"].to_numpy()

    dist_matrix = []
    for r in rsus:
        d = haversine_m_np(lat, lon, r.lat, r.lon)
        col = f"dist_{r.rsu_id}_m"
        metrics[col] = d
        dist_matrix.append(d)

    dist_matrix = np.vstack(dist_matrix).T
    nearest_idx = np.argmin(dist_matrix, axis=1)
    metrics["nearest_rsu"] = [rsus[i].rsu_id for i in nearest_idx]
    metrics["dist_from_rsu_m"] = dist_matrix[np.arange(len(metrics)), nearest_idx]

    if len(rsus) > 1:
        metrics["dist_union_m"] = np.min(dist_matrix, axis=1)

    return metrics


def add_distance_features_events(events: pd.DataFrame, rsus: List[RSU]) -> pd.DataFrame:
    events = events.copy()
    events["timestamp"] = pd.to_datetime(events["timestamp"], errors="coerce")

    lat = events["latitude"].to_numpy()
    lon = events["longitude"].to_numpy()

    dist_matrix = []
    for r in rsus:
        d = haversine_m_np(lat, lon, r.lat, r.lon)
        events[f"dist_{r.rsu_id}_m"] = d
        dist_matrix.append(d)

    dist_matrix = np.vstack(dist_matrix).T
    nearest_idx = np.argmin(dist_matrix, axis=1)
    events["nearest_rsu"] = [rsus[i].rsu_id for i in nearest_idx]
    events["dist_from_rsu_m"] = dist_matrix[np.arange(len(events)), nearest_idx]

    if len(rsus) > 1:
        events["dist_union_m"] = np.min(dist_matrix, axis=1)

    return events


def build_range_profile_nearest(metrics: pd.DataFrame, bin_m: float) -> pd.DataFrame:
    df = metrics.copy()
    df["dist_bin_m"] = ((df["dist_from_rsu_m"] // bin_m) * bin_m).astype(float)

    group_cols = ["dist_bin_m"]
    if "nearest_rsu" in df.columns and df["nearest_rsu"].nunique() > 1:
        group_cols = ["nearest_rsu", "dist_bin_m"]

    agg = (
        df.groupby(group_cols)
        .agg(
            n_samples=("dist_from_rsu_m", "size"),
            coverage_fraction=("has_pkts", "mean"),
            mean_delta_bytes=("delta_bytes", "mean"),
            mean_time_since_last_pkt=("time_since_last_pkt", "mean"),
        )
        .reset_index()
        .sort_values(group_cols)
    )
    agg["dist_bin_center_m"] = agg["dist_bin_m"] + bin_m / 2.0
    return agg


def build_range_profile_union(metrics: pd.DataFrame, bin_m: float) -> pd.DataFrame:
    if "dist_union_m" not in metrics.columns:
        raise ValueError("Union profile requested but dist_union_m not available (need >=2 RSUs).")

    df = metrics.copy()
    df["dist_bin_m"] = ((df["dist_union_m"] // bin_m) * bin_m).astype(float)
    agg = (
        df.groupby("dist_bin_m")
        .agg(
            n_samples=("dist_union_m", "size"),
            coverage_fraction=("has_pkts", "mean"),
            mean_delta_bytes=("delta_bytes", "mean"),
            mean_time_since_last_pkt=("time_since_last_pkt", "mean"),
        )
        .reset_index()
        .sort_values("dist_bin_m")
    )
    agg["dist_bin_center_m"] = agg["dist_bin_m"] + bin_m / 2.0
    return agg


# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop-dir", required=True, help="Raw loop folder containing metrics.csv and optional events.csv")
    ap.add_argument("--out-dir", required=True, help="Output folder for processed files")
    ap.add_argument("--bin-m", type=float, default=50.0, help="Distance bin size in meters")

    ap.add_argument("--rsu", action="append", default=[], help='Repeatable: "RSU1:lat,lon"')
    ap.add_argument("--rsu-json", default=None, help="JSON file with { rsus: [ {id,lat,lon}, ... ] }")

    ap.add_argument("--add-elevation", action="store_true", help="Add elevation_m using USGS EPQS")
    ap.add_argument("--elev-cache", default="cache/elevation_cache.csv", help="Cache CSV to reduce API calls")
    ap.add_argument("--elev-round", type=int, default=5, help="Round decimals for lat/lon caching")

    ap.add_argument("--write-union-profile", action="store_true", help="Also write a union profile (min distance across RSUs)")
    args = ap.parse_args()

    # RSUs
    if args.rsu_json:
        rsus = load_rsus_from_json(args.rsu_json)
    else:
        rsus = parse_rsus(args.rsu)

    ensure_dir(args.out_dir)

    metrics_path = os.path.join(args.loop_dir, "metrics.csv")
    events_path = os.path.join(args.loop_dir, "events.csv")

    if not os.path.exists(metrics_path):
        raise FileNotFoundError(f"metrics.csv not found in {args.loop_dir}")

    print(f"Loading metrics: {metrics_path}")
    metrics = pd.read_csv(metrics_path)

    for col in ["timestamp", "latitude", "longitude", "delta_bytes"]:
        if col not in metrics.columns:
            raise ValueError(f"metrics.csv missing required column: {col}")

    metrics = compute_time_features(metrics)
    metrics = add_distance_features_metrics(metrics, rsus)

    if args.add_elevation:
        print("Adding elevation_m...")
        cache_path = args.elev_cache
        if not os.path.isabs(cache_path):
            cache_path = os.path.join(os.getcwd(), cache_path)

        metrics = add_elevation_column(
            metrics,
            lat_col="latitude",
            lon_col="longitude",
            round_decimals=args.elev_round,
            cache_csv=cache_path,
        )

    events_out = None
    if os.path.exists(events_path):
        print(f"Loading events: {events_path}")
        events = pd.read_csv(events_path)
        for col in ["timestamp", "latitude", "longitude"]:
            if col not in events.columns:
                raise ValueError(f"events.csv missing required column: {col}")
        events = add_distance_features_events(events, rsus)
        events_out = events
    else:
        print("No events.csv found for this loop (OK).")

    profile_nearest = build_range_profile_nearest(metrics, args.bin_m)

    metrics_out_path = os.path.join(args.out_dir, "metrics_enhanced.csv")
    profile_nearest_path = os.path.join(args.out_dir, "range_profile_nearest.csv")

    print(f"Writing: {metrics_out_path}")
    metrics.to_csv(metrics_out_path, index=False)

    print(f"Writing: {profile_nearest_path}")
    profile_nearest.to_csv(profile_nearest_path, index=False)

    if events_out is not None:
        events_out_path = os.path.join(args.out_dir, "events_with_distance.csv")
        print(f"Writing: {events_out_path}")
        events_out.to_csv(events_out_path, index=False)

    if args.write_union_profile and len(rsus) > 1:
        profile_union = build_range_profile_union(metrics, args.bin_m)
        profile_union_path = os.path.join(args.out_dir, "range_profile_union.csv")
        print(f"Writing: {profile_union_path}")
        profile_union.to_csv(profile_union_path, index=False)

    rsu_meta_path = os.path.join(args.out_dir, "rsus_used.json")
    with open(rsu_meta_path, "w", encoding="utf-8") as f:
        json.dump({"rsus": [{"id": r.rsu_id, "lat": r.lat, "lon": r.lon} for r in rsus]}, f, indent=2)
    print(f"Writing: {rsu_meta_path}")

    print("Done.")


if __name__ == "__main__":
    main()
