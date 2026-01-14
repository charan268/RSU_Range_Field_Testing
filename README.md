# RSU Range Field Testing (OBU/RSU Range Data Collection + Post-Processing)

This repository contains the end-to-end workflow for **RSU field testing** and **range/coverage analysis** using an onboard laptop connected to an OBU. The repo is organized into two main stages:

1. **Raw data collection (during the drive)** — handled by `monitor.py`
2. **Post-processing (after the drive)** — handled by `analyze_loop.py`

Each stage has its **own detailed README PDF** inside the corresponding folder:
- `raw/README.pdf` → how to run **monitor.py** (field setup + logging)
- `processed/README.pdf` → how to run **analyze_loop.py** (post-processing + outputs)

