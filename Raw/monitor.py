import os
import time
import math
import csv
from datetime import datetime

import paramiko
from paramiko.ssh_exception import SSHException
import folium  # extra dependency for map generation


# =========================
# Configuration
# =========================

OBU_HOST = "192.168.52.79"
OBU_USER = "user"
OBU_PASSWORD = "user"

# RX pcap we monitor
REMOTE_RX_FILE = "/mnt/rw/log/current/rx.pcap"

# Command that prints live GNSS
KINEMATICS_CMD = "cd /mnt/rw/example1609 && kinematics-sample-client -a -n1"

PACKET_SIZE_BYTES = 98          # bytes per packet (approx)
POLL_INTERVAL_SEC = 1           # seconds between checks

ENTRY_SECONDS = 3               # ENTRY: pps > 0 for 3s (smoothed)
EXIT_SECONDS = 4                # EXIT: pps == 0 for 4s (smoothed)

PPS_WINDOW_SEC = 4              # smoothing window in seconds

MPH_FACTOR = 2.23693629         # convert m/s -> mph


OUTPUT_DIR = "outputs"
METRICS_FILE = None   # set per run
EVENTS_FILE = None    # set per run
MAP_FILE = None       # set per run


# =========================
# Helper functions
# =========================

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def init_run_files():
    """
    For each run, create:
      - metrics_<timestamp>.csv
      - events_<timestamp>.csv
      - rsu_map_<timestamp>.html  (used by update_map)
    """
    global METRICS_FILE, EVENTS_FILE, MAP_FILE

    ensure_output_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    METRICS_FILE = os.path.join(OUTPUT_DIR, f"metrics_{ts}.csv")
    EVENTS_FILE = os.path.join(OUTPUT_DIR, f"events_{ts}.csv")
    MAP_FILE = os.path.join(OUTPUT_DIR, f"rsu_map_{ts}.html")

    # metrics CSV (mph only – as you wanted)
    with open(METRICS_FILE, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "rx_size",
            "delta_bytes",
            "pps",
            "pdr",
            "latitude",
            "longitude",
            "speed_mph",
        ])

    print(f"[info] Logging metrics to {METRICS_FILE}")

    # events CSV
    with open(EVENTS_FILE, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "event_type", "reason",
                         "latitude", "longitude"])
    print(f"[info] Logging events to {EVENTS_FILE}")

    # map file is just a path; it gets created on first update_map()
    print(f"[info] Map will be written to {MAP_FILE}")


def append_metrics_row(ts, size, delta_bytes, pps, pdr, lat, lon, speed_mph):
    with open(METRICS_FILE, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            ts,
            size,
            delta_bytes,
            f"{pps:.2f}",
            f"{pdr:.2f}",
            f"{lat:.8f}" if lat is not None else "",
            f"{lon:.8f}" if lon is not None else "",
            f"{speed_mph:.2f}" if speed_mph is not None else "",
        ])


def get_remote_file_size(sftp, path):
    """
    Return file size in bytes, or None if the SFTP/SSH connection is gone.
    """
    try:
        attrs = sftp.stat(path)
        return attrs.st_size
    except FileNotFoundError:
        # File not created yet → treat as 0 bytes
        return 0
    except (IOError, SSHException, EOFError) as e:
        # Connection died or SFTP broke
        print(f"[error] SFTP/SSH error while reading {path}: {e}")
        return None


def haversine_m(lat1, lon1, lat2, lon2):
    """
    Distance in meters between two GPS points using the Haversine formula.
    """
    R = 6371000.0  # Earth radius in meters

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def read_gps_from_kinematics(ssh, timeout=4):
    """
    Run kinematics-sample-client -a -n1 on the OBU and parse latitude/longitude.
    Returns (lat, lon) as floats, or (None, None) on failure.
    """
    try:
        stdin, stdout, stderr = ssh.exec_command(KINEMATICS_CMD, timeout=timeout)
        out = stdout.read().decode("utf-8", errors="ignore")

        lat = None
        lon = None

        for line in out.splitlines():
            line = line.strip()
            if line.startswith("latitude"):
                # e.g. "latitude          - 36.14096492..."
                parts = line.split("-", 1)
                if len(parts) == 2:
                    try:
                        lat = float(parts[1].strip())
                    except ValueError:
                        lat = None
            elif line.startswith("longitude"):
                parts = line.split("-", 1)
                if len(parts) == 2:
                    try:
                        lon = float(parts[1].strip())
                    except ValueError:
                        lon = None

        if lat is not None and lon is not None:
            return lat, lon

    except Exception as e:
        print(f"[warn] GPS read failed: {e}")

    return None, None


def update_map(events):
    """
    Regenerate rsu_map.html from the in-memory list of events.
    Each event is a dict: {timestamp, event_type, reason, lat, lon}.
    """
    if not events:
        return

    # center map on average position
    center_lat = sum(e["lat"] for e in events) / len(events)
    center_lon = sum(e["lon"] for e in events) / len(events)

    m = folium.Map(location=[center_lat, center_lon], zoom_start=16)

    for e in events:
        color = "green" if e["event_type"] == "ENTRY" else "red"
        popup = (
            f"{e['event_type']}<br>{e['timestamp']}<br>{e['reason']}<br>"
            f"({e['lat']:.6f}, {e['lon']:.6f})"
        )
        folium.Marker(
            location=[e["lat"], e["lon"]],
            popup=popup,
            icon=folium.Icon(color=color),
        ).add_to(m)

    m.save(MAP_FILE)
    print(f"[info] Map updated: {MAP_FILE}")


def record_event(timestamp, event_type, reason, lat, lon, events_list):
    """
    Append one ENTRY/EXIT row to events.csv and update in-memory list + map.
    """
    with open(EVENTS_FILE, mode="a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            timestamp,
            event_type,
            reason,
            f"{lat:.8f}" if lat is not None else "",
            f"{lon:.8f}" if lon is not None else "",
        ])

    if lat is not None and lon is not None:
        events_list.append(
            {
                "timestamp": timestamp,
                "event_type": event_type,
                "reason": reason,
                "lat": lat,
                "lon": lon,
            }
        )
        update_map(events_list)
    else:
        print("[warn] Event recorded without GPS; map not updated.")


# =========================
# Main monitoring loop
# =========================

def main():
    init_run_files()

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print(f"[info] Connecting to {OBU_HOST} ...")
    try:
        ssh.connect(OBU_HOST, username=OBU_USER, password=OBU_PASSWORD, timeout=10)
    except Exception as e:
        print(f"[error] Could not connect to {OBU_HOST}: {e}")
        return

    try:
        sftp = ssh.open_sftp()
    except Exception as e:
        print(f"[error] Could not open SFTP session: {e}")
        ssh.close()
        return

    print("[info] Connected. Starting RX monitor loop.")

    prev_size = get_remote_file_size(sftp, REMOTE_RX_FILE)
    if prev_size is None:
        print("[error] Could not read initial RX file size (connection problem). Stopping.")
        try:
            sftp.close()
        except Exception:
            pass
        ssh.close()
        return

    state = "OUTSIDE"
    entry_counter = 0
    exit_counter = 0

    delta_history = []          # sliding window for PPS smoothing
    events_list = []            # in-memory list of events for map
    last_gps_lat = None
    last_gps_lon = None
    last_gps_time = None

    try:
        while True:
            time.sleep(POLL_INTERVAL_SEC)
            ts = current_timestamp()

            size = get_remote_file_size(sftp, REMOTE_RX_FILE)
            if size is None:
                print("[error] Connection to OBU lost while reading RX file. Stopping monitor.")
                break

            delta_bytes = max(0, size - prev_size)
            prev_size = size

            # update smoothing window
            delta_history.append(delta_bytes)
            if len(delta_history) > PPS_WINDOW_SEC:
                delta_history.pop(0)

            window_bytes = sum(delta_history)

            if PACKET_SIZE_BYTES > 0 and len(delta_history) > 0:
                window_secs = len(delta_history) * POLL_INTERVAL_SEC
                packets = window_bytes / PACKET_SIZE_BYTES
                pps = packets / window_secs
            else:
                pps = 0.0

            # simple RX health: window has packets or not
            pdr = 1.0 if window_bytes > 0 else 0.0

            # ===== GPS + speed calculation (mph) =====
            lat, lon = read_gps_from_kinematics(ssh)

            speed_mph = None
            now_dt = datetime.now()

            if lat is not None and lon is not None:
                if last_gps_lat is not None and last_gps_lon is not None and last_gps_time is not None:
                    # Assume one GPS sample per loop (≈ POLL_INTERVAL_SEC seconds)
                    dt = float(POLL_INTERVAL_SEC)
                    if dt > 0:
                        dist_m = haversine_m(last_gps_lat, last_gps_lon, lat, lon)
                        speed_mps = dist_m / dt
                        speed_mph = speed_mps * MPH_FACTOR

                # update last GPS sample (position + time)
                last_gps_lat = lat
                last_gps_lon = lon
                last_gps_time = now_dt
            else:
                # GPS not available this second; no new position or speed
                lat = None
                lon = None
                # speed_mph stays None

            # write metrics row (with GPS + speed)
            append_metrics_row(ts, size, delta_bytes, pps, pdr, lat, lon, speed_mph)

            # debug print
            lat_str = f"{lat:.6f}" if lat is not None else "NA"
            lon_str = f"{lon:.6f}" if lon is not None else "NA"
            speed_str = f"{speed_mph:.2f} mph" if speed_mph is not None else "NA"

            print(
                f"[debug] size={size} delta={delta_bytes} "
                f"pps={pps:.2f} pdr={pdr:.2f} "
                f"lat={lat_str} lon={lon_str} speed={speed_str}"
            )

            # ENTRY / EXIT counters based on smoothed pps
            if pps > 0:
                entry_counter += 1
                exit_counter = 0
            else:
                exit_counter += 1
                entry_counter = 0

            # ENTRY: OUTSIDE -> INSIDE
            if state == "OUTSIDE" and entry_counter >= ENTRY_SECONDS:
                state = "INSIDE"
                # grab GPS once at the moment of ENTRY
                lat_e, lon_e = read_gps_from_kinematics(ssh)
                reason = f"pps={pps:.2f} for {ENTRY_SECONDS}s (smoothed)"
                print(f"[EVENT] {ts} — ENTRY (reason: {reason})")
                record_event(ts, "ENTRY", reason, lat_e, lon_e, events_list)
                entry_counter = 0

            # EXIT: INSIDE -> OUTSIDE
            elif state == "INSIDE" and exit_counter >= EXIT_SECONDS:
                state = "OUTSIDE"
                lat_x, lon_x = read_gps_from_kinematics(ssh)
                reason = f"pps~0 for {EXIT_SECONDS}s (smoothed)"
                print(f"[EVENT] {ts} — EXIT (reason: {reason})")
                record_event(ts, "EXIT", reason, lat_x, lon_x, events_list)
                exit_counter = 0

    except KeyboardInterrupt:
        print("\n[info] Stopping monitor (Ctrl+C).")
    finally:
        try:
            sftp.close()
        except Exception:
            pass
        ssh.close()
        print("[info] SSH connection closed.")


if __name__ == "__main__":
    main()























# import os
# import time
# import math
# import csv
# from datetime import datetime
#
# import paramiko
# from paramiko.ssh_exception import SSHException
# import folium  # extra dependency for map generation
#
#
# # =========================
# # Configuration
# # =========================
#
# OBU_HOST = "192.168.52.79"
# OBU_USER = "user"
# OBU_PASSWORD = "user"
#
# # RX pcap we monitor
# REMOTE_RX_FILE = "/mnt/rw/log/current/rx.pcap"
#
# # Command that prints live GNSS
# KINEMATICS_CMD = "cd /mnt/rw/example1609 && kinematics-sample-client -a -n1"
#
# # PACKET_SIZE_BYTES = 98          # bytes per packet (approx)
# # POLL_INTERVAL_SEC = 1           # seconds between checks
# #
# # ENTRY_SECONDS = 3               # ENTRY: pps > 0 for 3s (smoothed)
# # EXIT_SECONDS = 4                # EXIT: pps == 0 for 4s (smoothed)
# #
# # PPS_WINDOW_SEC = 4              # smoothing window in seconds
#
# PACKET_SIZE_BYTES = 98          # bytes per packet (approx)
# POLL_INTERVAL_SEC = 1           # seconds between checks
#
# ENTRY_SECONDS = 3               # ENTRY: pps > 0 for 3s (smoothed)
# EXIT_SECONDS = 4                # EXIT: pps == 0 for 4s (smoothed)
#
# PPS_WINDOW_SEC = 4              # smoothing window in seconds
#
# MPH_FACTOR = 2.23693629         # convert m/s -> mph
#
#
# OUTPUT_DIR = "outputs"
# METRICS_FILE = None   # set per run
# EVENTS_FILE = None    # set per run
# MAP_FILE = None       # set per run
#
#
# # =========================
# # Helper functions
# # =========================
#
# def ensure_output_dir():
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
#
#
# def init_run_files():
#     """
#     For each run, create:
#       - metrics_<timestamp>.csv
#       - events_<timestamp>.csv
#       - rsu_map_<timestamp>.html  (used by update_map)
#     """
#     global METRICS_FILE, EVENTS_FILE, MAP_FILE
#
#     ensure_output_dir()
#     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#
#     METRICS_FILE = os.path.join(OUTPUT_DIR, f"metrics_{ts}.csv")
#     EVENTS_FILE = os.path.join(OUTPUT_DIR, f"events_{ts}.csv")
#     MAP_FILE = os.path.join(OUTPUT_DIR, f"rsu_map_{ts}.html")
#
#     # init metrics CSV
#     # with open(METRICS_FILE, mode="w", newline="") as f:
#     #     writer = csv.writer(f)
#     #     writer.writerow([
#     #         "timestamp",
#     #         "rx_size",
#     #         "delta_bytes",
#     #         "pps",
#     #         "pdr",
#     #         "latitude",
#     #         "longitude",
#     #         "speed_mps",
#     #         "speed_kmh",
#     #     ])
#     with open(METRICS_FILE, mode="w", newline="") as f:
#         writer = csv.writer(f)
#         writer.writerow([
#                 "timestamp",
#                 "rx_size",
#                 "delta_bytes",
#                 "pps",
#                 "pdr",
#                 "latitude",
#                 "longitude",
#                 "speed_mph",
#             ])
#
#
#     print(f"[info] Logging metrics to {METRICS_FILE}")
#
#     # init events CSV
#     with open(EVENTS_FILE, mode="w", newline="") as f:
#         writer = csv.writer(f)
#         writer.writerow(["timestamp", "event_type", "reason",
#                          "latitude", "longitude"])
#     print(f"[info] Logging events to {EVENTS_FILE}")
#
#     # map file is just a path; it gets created on first update_map()
#     print(f"[info] Map will be written to {MAP_FILE}")
#
#
# # def append_metrics_row(ts, size, delta_bytes, pps, pdr, lat, lon, speed_mps, speed_kmh):
# #     with open(METRICS_FILE, mode="a", newline="") as f:
# #         writer = csv.writer(f)
# #         writer.writerow([
# #             ts,
# #             size,
# #             delta_bytes,
# #             f"{pps:.2f}",
# #             f"{pdr:.2f}",
# #             f"{lat:.8f}" if lat is not None else "",
# #             f"{lon:.8f}" if lon is not None else "",
# #             f"{speed_mps:.2f}" if speed_mps is not None else "",
# #             f"{speed_kmh:.2f}" if speed_kmh is not None else "",
# #         ])
# def append_metrics_row(ts, size, delta_bytes, pps, pdr, lat, lon, speed_mph):
#     with open(METRICS_FILE, mode="a", newline="") as f:
#         writer = csv.writer(f)
#         writer.writerow([
#             ts,
#             size,
#             delta_bytes,
#             f"{pps:.2f}",
#             f"{pdr:.2f}",
#             f"{lat:.8f}" if lat is not None else "",
#             f"{lon:.8f}" if lon is not None else "",
#             f"{speed_mph:.2f}" if speed_mph is not None else "",
#         ])
#
#
#
# def get_remote_file_size(sftp, path):
#     """
#     Return file size in bytes, or None if the SFTP/SSH connection is gone.
#     """
#     try:
#         attrs = sftp.stat(path)
#         return attrs.st_size
#     except FileNotFoundError:
#         # File not created yet → treat as 0 bytes
#         return 0
#     except (IOError, SSHException, EOFError) as e:
#         # Connection died or SFTP broke
#         print(f"[error] SFTP/SSH error while reading {path}: {e}")
#         return None
#
#
# def haversine_m(lat1, lon1, lat2, lon2):
#     """
#     Distance in meters between two GPS points using the Haversine formula.
#     """
#     R = 6371000.0  # Earth radius in meters
#
#     phi1 = math.radians(lat1)
#     phi2 = math.radians(lat2)
#     dphi = math.radians(lat2 - lat1)
#     dlambda = math.radians(lon2 - lon1)
#
#     a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
#
#     return R * c
#
#
# def current_timestamp():
#     return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#
#
# def read_gps_from_kinematics(ssh, timeout=4):
#     """
#     Run kinematics-sample-client -a -n1 on the OBU and parse latitude/longitude.
#     Returns (lat, lon) as floats, or (None, None) on failure.
#     """
#     try:
#         stdin, stdout, stderr = ssh.exec_command(KINEMATICS_CMD, timeout=timeout)
#         out = stdout.read().decode("utf-8", errors="ignore")
#
#         lat = None
#         lon = None
#
#         for line in out.splitlines():
#             line = line.strip()
#             if line.startswith("latitude"):
#                 # e.g. "latitude          - 36.14096492..."
#                 parts = line.split("-", 1)
#                 if len(parts) == 2:
#                     try:
#                         lat = float(parts[1].strip())
#                     except ValueError:
#                         lat = None
#             elif line.startswith("longitude"):
#                 parts = line.split("-", 1)
#                 if len(parts) == 2:
#                     try:
#                         lon = float(parts[1].strip())
#                     except ValueError:
#                         lon = None
#
#         if lat is not None and lon is not None:
#             return lat, lon
#
#     except Exception as e:
#         print(f"[warn] GPS read failed: {e}")
#
#     return None, None
#
#
# def update_map(events):
#     """
#     Regenerate rsu_map.html from the in-memory list of events.
#     Each event is a dict: {timestamp, event_type, reason, lat, lon}.
#     """
#     if not events:
#         return
#
#     # center map on average position
#     center_lat = sum(e["lat"] for e in events) / len(events)
#     center_lon = sum(e["lon"] for e in events) / len(events)
#
#     m = folium.Map(location=[center_lat, center_lon], zoom_start=16)
#
#     for e in events:
#         color = "green" if e["event_type"] == "ENTRY" else "red"
#         popup = (
#             f"{e['event_type']}<br>{e['timestamp']}<br>{e['reason']}<br>"
#             f"({e['lat']:.6f}, {e['lon']:.6f})"
#         )
#         folium.Marker(
#             location=[e["lat"], e["lon"]],
#             popup=popup,
#             icon=folium.Icon(color=color),
#         ).add_to(m)
#
#     m.save(MAP_FILE)
#     print(f"[info] Map updated: {MAP_FILE}")
#
#
# def record_event(timestamp, event_type, reason, lat, lon, events_list):
#     """
#     Append one ENTRY/EXIT row to events.csv and update in-memory list + map.
#     """
#     with open(EVENTS_FILE, mode="a", newline="") as f:
#         writer = csv.writer(f)
#         writer.writerow([
#             timestamp,
#             event_type,
#             reason,
#             f"{lat:.8f}" if lat is not None else "",
#             f"{lon:.8f}" if lon is not None else "",
#         ])
#
#     if lat is not None and lon is not None:
#         events_list.append(
#             {
#                 "timestamp": timestamp,
#                 "event_type": event_type,
#                 "reason": reason,
#                 "lat": lat,
#                 "lon": lon,
#             }
#         )
#         update_map(events_list)
#     else:
#         print("[warn] Event recorded without GPS; map not updated.")
#
#
# # =========================
# # Main monitoring loop
# # =========================
#
# def main():
#     init_run_files()
#
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#
#     print(f"[info] Connecting to {OBU_HOST} ...")
#     try:
#         ssh.connect(OBU_HOST, username=OBU_USER, password=OBU_PASSWORD, timeout=10)
#     except Exception as e:
#         print(f"[error] Could not connect to {OBU_HOST}: {e}")
#         return
#
#     try:
#         sftp = ssh.open_sftp()
#     except Exception as e:
#         print(f"[error] Could not open SFTP session: {e}")
#         ssh.close()
#         return
#
#     print("[info] Connected. Starting RX monitor loop.")
#
#     prev_size = get_remote_file_size(sftp, REMOTE_RX_FILE)
#     if prev_size is None:
#         print("[error] Could not read initial RX file size (connection problem). Stopping.")
#         try:
#             sftp.close()
#         except Exception:
#             pass
#         ssh.close()
#         return
#
#     state = "OUTSIDE"
#     entry_counter = 0
#     exit_counter = 0
#
#     delta_history = []          # sliding window for PPS smoothing
#     events_list = []            # in-memory list of events for map
#     last_gps_lat = None
#     last_gps_lon = None
#     last_gps_time = None
#
#     try:
#         while True:
#             time.sleep(POLL_INTERVAL_SEC)
#             ts = current_timestamp()
#
#             size = get_remote_file_size(sftp, REMOTE_RX_FILE)
#             if size is None:
#                 print("[error] Connection to OBU lost while reading RX file. Stopping monitor.")
#                 break
#
#             delta_bytes = max(0, size - prev_size)
#             prev_size = size
#
#             # update smoothing window
#             delta_history.append(delta_bytes)
#             if len(delta_history) > PPS_WINDOW_SEC:
#                 delta_history.pop(0)
#
#             window_bytes = sum(delta_history)
#
#             if PACKET_SIZE_BYTES > 0 and len(delta_history) > 0:
#                 window_secs = len(delta_history) * POLL_INTERVAL_SEC
#                 packets = window_bytes / PACKET_SIZE_BYTES
#                 pps = packets / window_secs
#             else:
#                 pps = 0.0
#
#             # simple RX health: window has packets or not
#             pdr = 1.0 if window_bytes > 0 else 0.0
#
#             # ===== GPS + speed calculation =====
#             # lat, lon = read_gps_from_kinematics(ssh)
#             #
#             # speed_mps = None
#             # speed_kmh = None
#             #
#             # now_dt = datetime.now()
#             #
#             # if lat is not None and lon is not None:
#             #     if last_gps_lat is not None and last_gps_lon is not None and last_gps_time is not None:
#             #         dt = (now_dt - last_gps_time).total_seconds()
#             #         if dt > 0:
#             #             dist_m = haversine_m(last_gps_lat, last_gps_lon, lat, lon)
#             #             speed_mps = dist_m / dt
#             #             speed_kmh = speed_mps * 3.6
#             #
#             #     # update last GPS sample
#             #     last_gps_lat = lat
#             #     last_gps_lon = lon
#             #     last_gps_time = now_dt
#             # else:
#             #     # GPS not available this second; keep last position but no new speed
#             #     lat = None
#             #     lon = None
#             #     # speed_mps, speed_kmh remain None
#             # ===== GPS + speed calculation =====
#             lat, lon = read_gps_from_kinematics(ssh)
#
#             speed_mph = None
#
#             now_dt = datetime.now()
#
#             if lat is not None and lon is not None:
#                 if last_gps_lat is not None and last_gps_lon is not None and last_gps_time is not None:
#                     # Assume one GPS sample per loop (≈ POLL_INTERVAL_SEC seconds)
#                     dt = float(POLL_INTERVAL_SEC)
#                     if dt > 0:
#                         dist_m = haversine_m(last_gps_lat, last_gps_lon, lat, lon)
#                         speed_mps = dist_m / dt
#                         speed_mph = speed_mps * MPH_FACTOR
#
#                 # update last GPS sample (still track last position + time)
#                 last_gps_lat = lat
#                 last_gps_lon = lon
#                 last_gps_time = now_dt
#             else:
#                 # GPS not available this second; no new position or speed
#                 lat = None
#                 lon = None
#                 # speed_mph remains None
#
#
#             # write metrics row (with GPS + speed)
#             # append_metrics_row(ts, size, delta_bytes, pps, pdr, lat, lon, speed_mps, speed_kmh)
#
#             append_metrics_row(ts, size, delta_bytes, pps, pdr, lat, lon, speed_mph)
#
#
#             # debug line
#             # lat_str = f"{lat:.6f}" if lat is not None else "NA"
#             # lon_str = f"{lon:.6f}" if lon is not None else "NA"
#             # speed_str = f"{speed_kmh:.2f} km/h" if speed_kmh is not None else "NA"
#             #
#             # print(
#             #     f"[debug] size={size} delta={delta_bytes} "
#             #     f"pps={pps:.2f} pdr={pdr:.2f} "
#             #     f"lat={lat_str} lon={lon_str} speed={speed_str}"
#             # )
#             lat_str = f"{lat:.6f}" if lat is not None else "NA"
#             lon_str = f"{lon:.6f}" if lon is not None else "NA"
#             speed_str = f"{speed_mph:.2f} mph" if speed_mph is not None else "NA"
#
#             print(
#                 f"[debug] size={size} delta={delta_bytes} "
#                 f"pps={pps:.2f} pdr={pdr:.2f} "
#                 f"lat={lat_str} lon={lon_str} speed={speed_str}"
# )
#
#
# # ENTRY / EXIT counters based on smoothed pps
# #             if pps > 0:
# #                 entry_counter += 1
# #                 exit_counter = 0
# #             else:
# #                 exit_counter += 1
# #                 entry_counter = 0
#             # ENTRY / EXIT counters based on smoothed BYTES (window_bytes), not pps
#             if window_bytes > 0:
#                 # RX file grew at least once in the last PPS_WINDOW_SEC seconds
#                 entry_counter += 1
#                 exit_counter = 0
#             else:
#                 # RX file did NOT grow at all in the last PPS_WINDOW_SEC seconds
#                 exit_counter += 1
#                 entry_counter = 0
#
#
#     # # ENTRY: OUTSIDE -> INSIDE
#     #         if state == "OUTSIDE" and entry_counter >= ENTRY_SECONDS:
#     #             state = "INSIDE"
#     #             # grab GPS once at the moment of ENTRY
#     #             lat_e, lon_e = read_gps_from_kinematics(ssh)
#     #             reason = f"pps={pps:.2f} for {ENTRY_SECONDS}s (smoothed)"
#     #             print(f"[EVENT] {ts} — ENTRY (reason: {reason})")
#     #             record_event(ts, "ENTRY", reason, lat_e, lon_e, events_list)
#     #             entry_counter = 0
#     #
#     #         # EXIT: INSIDE -> OUTSIDE
#     #         elif state == "INSIDE" and exit_counter >= EXIT_SECONDS:
#     #             state = "OUTSIDE"
#     #             lat_x, lon_x = read_gps_from_kinematics(ssh)
#     #             reason = f"pps~0 for {EXIT_SECONDS}s (smoothed)"
#     #             print(f"[EVENT] {ts} — EXIT (reason: {reason})")
#     #             record_event(ts, "EXIT", reason, lat_x, lon_x, events_list)
#     #             exit_counter = 0
#     # ENTRY: OUTSIDE -> INSIDE
#             if state == "OUTSIDE" and entry_counter >= ENTRY_SECONDS:
#                 state = "INSIDE"
#                 # grab GPS once at the moment of ENTRY
#                 lat_e, lon_e = read_gps_from_kinematics(ssh)
#                 reason = (
#                     f"window_bytes={window_bytes} (>0) for {ENTRY_SECONDS}s; "
#                     f"pps≈{pps:.2f} (smoothed)"
#                 )
#                 print(f"[EVENT] {ts} — ENTRY (reason: {reason})")
#                 record_event(ts, "ENTRY", reason, lat_e, lon_e, events_list)
#                 entry_counter = 0
#
#             # EXIT: INSIDE -> OUTSIDE
#             elif state == "INSIDE" and exit_counter >= EXIT_SECONDS:
#                 state = "OUTSIDE"
#                 lat_x, lon_x = read_gps_from_kinematics(ssh)
#                 reason = (
#                     f"window_bytes=0 for {EXIT_SECONDS}s; "
#                     f"pps≈{pps:.2f} (smoothed)"
#                 )
#                 print(f"[EVENT] {ts} — EXIT (reason: {reason})")
#                 record_event(ts, "EXIT", reason, lat_x, lon_x, events_list)
#                 exit_counter = 0
#
#
#     except KeyboardInterrupt:
#         print("\n[info] Stopping monitor (Ctrl+C).")
#     finally:
#         try:
#             sftp.close()
#         except Exception:
#             pass
#         ssh.close()
#         print("[info] SSH connection closed.")
#
#
# if __name__ == "__main__":
#     main()

# -------------------------------------------------------------------------
#
#
# import os
# import time
# import math
# import csv
# from datetime import datetime
# import webbrowser  # to open the map automatically
#
# import paramiko
# from paramiko.ssh_exception import SSHException
# import folium  # extra dependency for map generation
#
#
# # =========================
# # Configuration
# # =========================
#
# OBU_HOST = "192.168.52.79"
# OBU_USER = "user"
# OBU_PASSWORD = "user"
#
# # RX pcap we monitor
# REMOTE_RX_FILE = "/mnt/rw/log/current/rx.pcap"
#
# # Command that prints live GNSS
# KINEMATICS_CMD = "cd /mnt/rw/example1609 && kinematics-sample-client -a -n1"
#
# PACKET_SIZE_BYTES = 98          # bytes per packet (approx)
# POLL_INTERVAL_SEC = 1           # seconds between checks
#
# ENTRY_SECONDS = 3               # ENTRY: RX active for 3s
# EXIT_SECONDS = 4                # EXIT: RX inactive for 4s
#
# PPS_WINDOW_SEC = 1              # smoothing window in seconds (activity window)
#
# MPH_FACTOR = 2.23693629         # convert m/s -> mph
#
#
# OUTPUT_DIR = "outputs"
# METRICS_FILE = None   # set per run
# EVENTS_FILE = None    # set per run
# MAP_FILE = None       # set per run
# MAP_OPENED = False    # track if browser already opened
#
#
# # =========================
# # Helper functions
# # =========================
#
# def ensure_output_dir():
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
#
#
# def init_run_files():
#     """
#     For each run, create:
#       - metrics_<timestamp>.csv
#       - events_<timestamp>.csv
#       - rsu_map_<timestamp>.html  (used by update_map)
#     """
#     global METRICS_FILE, EVENTS_FILE, MAP_FILE
#
#     ensure_output_dir()
#     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#
#     METRICS_FILE = os.path.join(OUTPUT_DIR, f"metrics_{ts}.csv")
#     EVENTS_FILE = os.path.join(OUTPUT_DIR, f"events_{ts}.csv")
#     MAP_FILE = os.path.join(OUTPUT_DIR, f"rsu_map_{ts}.html")
#
#     # init metrics CSV
#     with open(METRICS_FILE, mode="w", newline="") as f:
#         writer = csv.writer(f)
#         writer.writerow([
#             "timestamp",
#             "rx_size",
#             "delta_bytes",
#             "pps",
#             "pdr",
#             "latitude",
#             "longitude",
#             "speed_mph",
#         ])
#
#     print(f"[info] Logging metrics to {METRICS_FILE}")
#
#     # init events CSV (also stores speed_mph)
#     with open(EVENTS_FILE, mode="w", newline="") as f:
#         writer = csv.writer(f)
#         writer.writerow([
#             "timestamp",
#             "event_type",
#             "reason",
#             "latitude",
#             "longitude",
#             "speed_mph",
#         ])
#     print(f"[info] Logging events to {EVENTS_FILE}")
#
#     # map file is just a path; it gets created on first update_map()
#     print(f"[info] Map will be written to {MAP_FILE}")
#
#
# def append_metrics_row(ts, size, delta_bytes, pps, pdr, lat, lon, speed_mph):
#     with open(METRICS_FILE, mode="a", newline="") as f:
#         writer = csv.writer(f)
#         writer.writerow([
#             ts,
#             size,
#             delta_bytes,
#             f"{pps:.2f}",
#             f"{pdr:.2f}",
#             f"{lat:.8f}" if lat is not None else "",
#             f"{lon:.8f}" if lon is not None else "",
#             f"{speed_mph:.2f}" if speed_mph is not None else "",
#         ])
#
#
# def get_remote_file_size(sftp, path):
#     """
#     Return file size in bytes, or None if the SFTP/SSH connection is gone.
#     """
#     try:
#         attrs = sftp.stat(path)
#         return attrs.st_size
#     except FileNotFoundError:
#         # File not created yet → treat as 0 bytes
#         return 0
#     except (IOError, SSHException, EOFError) as e:
#         # Connection died or SFTP broke
#         print(f"[error] SFTP/SSH error while reading {path}: {e}")
#         return None
#
#
# def haversine_m(lat1, lon1, lat2, lon2):
#     """
#     Distance in meters between two GPS points using the Haversine formula.
#     """
#     R = 6371000.0  # Earth radius in meters
#
#     phi1 = math.radians(lat1)
#     phi2 = math.radians(lat2)
#     dphi = math.radians(lat2 - lat1)
#     dlambda = math.radians(lon2 - lon1)
#
#     a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
#
#     return R * c
#
#
# def current_timestamp():
#     return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#
#
# def read_gps_from_kinematics(ssh, timeout=4):
#     """
#     Run kinematics-sample-client -a -n1 on the OBU and parse latitude/longitude.
#     Returns (lat, lon) as floats, or (None, None) on failure.
#     """
#     try:
#         stdin, stdout, stderr = ssh.exec_command(KINEMATICS_CMD, timeout=timeout)
#         out = stdout.read().decode("utf-8", errors="ignore")
#
#         lat = None
#         lon = None
#
#         for line in out.splitlines():
#             line = line.strip()
#             if line.startswith("latitude"):
#                 # e.g. "latitude          - 36.14096492..."
#                 parts = line.split("-", 1)
#                 if len(parts) == 2:
#                     try:
#                         lat = float(parts[1].strip())
#                     except ValueError:
#                         lat = None
#             elif line.startswith("longitude"):
#                 parts = line.split("-", 1)
#                 if len(parts) == 2:
#                     try:
#                         lon = float(parts[1].strip())
#                     except ValueError:
#                         lon = None
#
#         if lat is not None and lon is not None:
#             return lat, lon
#
#     except Exception as e:
#         print(f"[warn] GPS read failed: {e}")
#
#     return None, None
#
#
# def update_map(events):
#     """
#     Regenerate rsu_map.html from the in-memory list of events.
#     Each event is a dict: {timestamp, event_type, reason, lat, lon, speed}.
#     """
#     global MAP_OPENED
#
#     if not events:
#         return
#
#     # center map on average position
#     center_lat = sum(e["lat"] for e in events) / len(events)
#     center_lon = sum(e["lon"] for e in events) / len(events)
#
#     m = folium.Map(location=[center_lat, center_lon], zoom_start=16)
#
#     # auto-refresh page every 5 seconds
#     m.get_root().html.add_child(
#         folium.Element("<meta http-equiv='refresh' content='5'>")
#     )
#
#     for e in events:
#         color = "green" if e["event_type"] == "ENTRY" else "red"
#
#         speed_txt = ""
#         if e.get("speed") is not None:
#             speed_txt = f"<br>Speed: {e['speed']:.2f} mph"
#
#         popup = (
#             f"{e['event_type']}<br>{e['timestamp']}<br>{e['reason']}"
#             f"<br>({e['lat']:.6f}, {e['lon']:.6f})"
#             f"{speed_txt}"
#         )
#
#         folium.Marker(
#             location=[e["lat"], e["lon"]],
#             popup=popup,
#             icon=folium.Icon(color=color),
#         ).add_to(m)
#
#     m.save(MAP_FILE)
#     print(f"[info] Map updated: {MAP_FILE}")
#
#     # open browser the first time (if not already)
#     if not MAP_OPENED:
#         url = "file://" + os.path.abspath(MAP_FILE)
#         webbrowser.open(url)
#         MAP_OPENED = True
#
#
# def record_event(timestamp, event_type, reason, lat, lon, speed_mph, events_list):
#     """
#     Append one ENTRY/EXIT row to events.csv and update in-memory list + map.
#     """
#     with open(EVENTS_FILE, mode="a", newline="") as f:
#         writer = csv.writer(f)
#         writer.writerow([
#             timestamp,
#             event_type,
#             reason,
#             f"{lat:.8f}" if lat is not None else "",
#             f"{lon:.8f}" if lon is not None else "",
#             f"{speed_mph:.2f}" if speed_mph is not None else "",
#         ])
#
#     if lat is not None and lon is not None:
#         events_list.append(
#             {
#                 "timestamp": timestamp,
#                 "event_type": event_type,
#                 "reason": reason,
#                 "lat": lat,
#                 "lon": lon,
#                 "speed": speed_mph,
#             }
#         )
#         update_map(events_list)
#     else:
#         print("[warn] Event recorded without GPS; map not updated.")
#
#
# # =========================
# # Main monitoring loop
# # =========================
#
# def main():
#     init_run_files()
#
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#
#     print(f"[info] Connecting to {OBU_HOST} ...")
#     try:
#         ssh.connect(OBU_HOST, username=OBU_USER, password=OBU_PASSWORD, timeout=10)
#     except Exception as e:
#         print(f"[error] Could not connect to {OBU_HOST}: {e}")
#         return
#
#     try:
#         sftp = ssh.open_sftp()
#     except Exception as e:
#         print(f"[error] Could not open SFTP session: {e}")
#         ssh.close()
#         return
#
#     print("[info] Connected. Starting RX monitor loop.")
#
#     prev_size = get_remote_file_size(sftp, REMOTE_RX_FILE)
#     if prev_size is None:
#         print("[error] Could not read initial RX file size (connection problem). Stopping.")
#         try:
#             sftp.close()
#         except Exception:
#             pass
#         ssh.close()
#         return
#
#     # ===== INITIAL MAP ON STARTUP =====
#     global MAP_OPENED
#
#     # Try one GPS sample to center the initial map
#     lat0, lon0 = read_gps_from_kinematics(ssh)
#     if lat0 is None or lon0 is None:
#         # fallback location (Stillwater-ish; change if you want)
#         lat0, lon0 = 36.1410, -97.0660
#
#     m0 = folium.Map(location=[lat0, lon0], zoom_start=16)
#     # auto-refresh page every 5 seconds
#     m0.get_root().html.add_child(
#         folium.Element("<meta http-equiv='refresh' content='5'>")
#     )
#     m0.save(MAP_FILE)
#
#     # open browser to show the map
#     url0 = "file://" + os.path.abspath(MAP_FILE)
#     webbrowser.open(url0)
#     MAP_OPENED = True
#     # ===== END INITIAL MAP BLOCK =====
#
#     state = "OUTSIDE"
#     entry_counter = 0
#     exit_counter = 0
#
#     delta_history = []          # sliding window for smoothing
#     events_list = []            # in-memory list of events for map
#     last_gps_lat = None
#     last_gps_lon = None
#     last_gps_time = None
#     last_speed_mph = None       # remember last known speed
#
#     try:
#         while True:
#             time.sleep(POLL_INTERVAL_SEC)
#             ts = current_timestamp()
#
#             size = get_remote_file_size(sftp, REMOTE_RX_FILE)
#             if size is None:
#                 print("[error] Connection to OBU lost while reading RX file. Stopping monitor.")
#                 break
#
#             delta_bytes = max(0, size - prev_size)
#             prev_size = size
#
#             # update smoothing window
#             delta_history.append(delta_bytes)
#             if len(delta_history) > PPS_WINDOW_SEC:
#                 delta_history.pop(0)
#
#             window_bytes = sum(delta_history)
#
#             if PACKET_SIZE_BYTES > 0 and len(delta_history) > 0:
#                 window_secs = len(delta_history) * POLL_INTERVAL_SEC
#                 packets = window_bytes / PACKET_SIZE_BYTES
#                 pps = packets / window_secs
#             else:
#                 pps = 0.0
#
#             # simple RX health: window has bytes or not
#             pdr = 1.0 if window_bytes > 0 else 0.0
#
#             # ===== GPS + speed calculation =====
#             lat, lon = read_gps_from_kinematics(ssh)
#
#             speed_mph = None
#             now_dt = datetime.now()
#
#             if lat is not None and lon is not None:
#                 if last_gps_lat is not None and last_gps_lon is not None and last_gps_time is not None:
#                     # Assume one GPS sample per loop (≈ POLL_INTERVAL_SEC seconds)
#                     dt = float(POLL_INTERVAL_SEC)
#                     if dt > 0:
#                         dist_m = haversine_m(last_gps_lat, last_gps_lon, lat, lon)
#                         speed_mps = dist_m / dt
#                         speed_mph = speed_mps * MPH_FACTOR
#
#                 # update last GPS sample (still track last position + time)
#                 last_gps_lat = lat
#                 last_gps_lon = lon
#                 last_gps_time = now_dt
#             else:
#                 # GPS not available this second; no new position or speed
#                 lat = None
#                 lon = None
#                 # speed_mph remains None
#
#             if speed_mph is not None:
#                 last_speed_mph = speed_mph
#
#             # write metrics row (with GPS + speed)
#             append_metrics_row(ts, size, delta_bytes, pps, pdr, lat, lon, speed_mph)
#
#             # debug line
#             lat_str = f"{lat:.6f}" if lat is not None else "NA"
#             lon_str = f"{lon:.6f}" if lon is not None else "NA"
#             speed_str = f"{speed_mph:.2f} mph" if speed_mph is not None else "NA"
#
#             print(
#                 f"[debug] size={size} delta={delta_bytes} "
#                 f"pps={pps:.2f} pdr={pdr:.2f} "
#                 f"lat={lat_str} lon={lon_str} speed={speed_str}"
#             )
#
#             # ===== ENTRY / EXIT counters based on smoothed BYTES (window_bytes) =====
#             if window_bytes > 0:
#                 # RX file grew at least once in the last PPS_WINDOW_SEC seconds
#                 entry_counter += 1
#                 exit_counter = 0
#             else:
#                 # RX file did NOT grow at all in the last PPS_WINDOW_SEC seconds
#                 exit_counter += 1
#                 entry_counter = 0
#
#             # ENTRY: OUTSIDE -> INSIDE
#             if state == "OUTSIDE" and entry_counter >= ENTRY_SECONDS:
#                 state = "INSIDE"
#                 # grab GPS once at the moment of ENTRY
#                 lat_e, lon_e = read_gps_from_kinematics(ssh)
#                 reason = (
#                     f"window_bytes={window_bytes} (>0) for {ENTRY_SECONDS}s; "
#                     f"pps≈{pps:.2f} (smoothed)"
#                 )
#                 print(f"[EVENT] {ts} — ENTRY (reason: {reason})")
#                 record_event(ts, "ENTRY", reason, lat_e, lon_e, last_speed_mph, events_list)
#                 entry_counter = 0
#
#             # EXIT: INSIDE -> OUTSIDE
#             elif state == "INSIDE" and exit_counter >= EXIT_SECONDS:
#                 state = "OUTSIDE"
#                 lat_x, lon_x = read_gps_from_kinematics(ssh)
#                 reason = (
#                     f"window_bytes=0 for {EXIT_SECONDS}s; "
#                     f"pps≈{pps:.2f} (smoothed)"
#                 )
#                 print(f"[EVENT] {ts} — EXIT (reason: {reason})")
#                 record_event(ts, "EXIT", reason, lat_x, lon_x, last_speed_mph, events_list)
#                 exit_counter = 0
#
#     except KeyboardInterrupt:
#         print("\n[info] Stopping monitor (Ctrl+C).")
#     finally:
#         try:
#             sftp.close()
#         except Exception:
#             pass
#         ssh.close()
#         print("[info] SSH connection closed.")
#
#
# if __name__ == "__main__":
#     main()
