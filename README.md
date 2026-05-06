# A Scientific Workflow for Forecast-Triggered Adaptive Sensor Orchestration across the Computing Continuum

Code, scripts, figures, and paper sources for our forecast-triggered adaptive sensor orchestration workflow. The air-quality instantiation uses Aurora/CAMS forecasts, NDP for catalog discovery, and SciDx/Kafka for streaming.

## Video Walkthrough

[![Watch the air-quality workflow demo](https://img.youtube.com/vi/rStwL65TpL8/hqdefault.jpg)](https://youtu.be/rStwL65TpL8)

Notebook demo video: https://youtu.be/rStwL65TpL8

Hosted notebook demo: https://ndp-prod-194.chpc.utah.edu/jupyter/hub/spawn

Sign in through CILogon, start the **Air Quality Demo** workspace, and choose the **NDP-EP/SciDx Air Quality Streaming** image. The session opens `notebooks/aq.ipynb`.

Public air-quality UI: https://dev.air-quality.ndp.utah.edu/

Public NDP endpoint and stream UI: https://dev.air-quality.ndp.utah.edu/ep/ui/

URL mapping:
- `https://dev.air-quality.ndp.utah.edu/` forwards to local `http://localhost:8000/`
- `https://dev.air-quality.ndp.utah.edu/ep/ui/` forwards to local `http://localhost:8002/ep/ui/`

The hosted notebook and public web UI are proof-of-concept demo environments, not production services.

## Repository Layout

- `src/`: FastAPI service, workflow logic, event detection, and orchestration components.
- `scripts/`: experiment runners and figure-generation utilities used in the paper.
- `paper/results/`: experiment outputs and summary tables used to build paper figures.
- `paper/figures/`: generated figure assets referenced by the manuscript.
- `notebooks/aq.ipynb`: interactive notebook for the air-quality workflow walkthrough.
- `streaming-py/`: local editable copy of the SciDx streaming client used by the service.
- `ui/`: UI assets used by the endpoint web interface (`/ep/ui/`) in the Docker demo deployment.

## Quick Start

### Option 1: Private-machine deployment with Docker Compose

Our demo deployment starts by creating an endpoint through https://nationaldataplatform.org/. After that, we run the NDP install script on a private machine and start this repository from this folder with Docker Compose.

Requirements:
- Docker with Compose support
- Copernicus ADS credentials available at `~/.cdsapirc`

Clone the repository, move into this folder, then start the services:

```bash
git clone <repo-url>
cd scientific-workflow-for-forecast-triggered-adaptive-sensor-orchestration
docker compose up --build -d
```

The Compose stack starts Redis, the forecast API, and an RQ worker on the private machine.

In our deployment, the public demo URLs point to that private machine:

- `https://dev.air-quality.ndp.utah.edu/` -> local `http://localhost:8000/`
- `https://dev.air-quality.ndp.utah.edu/ep/ui/` -> local `http://localhost:8002/ep/ui/`

The `ui/` folder is required for the Docker demo when you want the endpoint web interface at `/ep/ui/`. If `ui/` is omitted, the API service can still run, but the demo endpoint UI will not be available.

This forwarding setup is specific to our demo deployment. Other users can deploy the system however they prefer on their own infrastructure.

The public endpoints are provided so others can try the system, explore the UI, and get a sense of the workflow behavior. They are proof-of-concept demo services, not production deployments.

### Option 2: Local Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip uninstall -y scidx_streaming
pip install -e ./streaming-py
uvicorn src.forecast_api:app --host 0.0.0.0 --port 8000
```

The experiment scripts that query NDP discovery expect environment variables such as `API_URL`, `TOKEN`, `SERVER`, and `SERVICE_BASE_URL` to be available through your shell or a local `.env` file.

## Single-Day Demo

Use a single-day Utah run for the demo. This stays within the limits of the shared proof-of-concept deployment.

Example request:

```bash
curl -X POST http://localhost:8000/run-model \
  -H "Content-Type: application/json" \
  -d '{
    "region": {
      "name": "utah",
      "lat_min": 37.0,
      "lat_max": 42.1,
      "lon_min": 245.9,
      "lon_max": 251.0
    },
    "dates": {
      "start": "2025-01-16",
      "end": "2025-01-16"
    },
    "overwrite": false
  }'
```

The `POST /run-model` endpoint enqueues the Aurora workflow and returns a `job_id`. Use `GET /status/{job_id}` to watch progress, then inspect the generated prediction and hotspot artifacts through the dashboard or the `/predictions`, `/hotspots`, and `/files` endpoints.

## Reproducibility

This repository supports two different reproducibility paths:

- Full experiment rerun from archived January 2025 forecast inputs.
- Figure regeneration from the shipped CSV outputs.

### Archived inputs shipped with the repo

The reproducibility package is organized around these archived inputs and outputs:

- Forecast inputs: `data/processed/predictions/`
- Cohort definitions: `paper/results/cohorts/`
- Run-level and summary CSVs: `paper/results/`
- Figure outputs: `paper/figures/`

The evaluation window used by the scripts is the 14-day January 2025 set:

- `2025-01-13`
- `2025-01-14`
- `2025-01-15`
- `2025-01-16`
- `2025-01-17`
- `2025-01-21`
- `2025-01-22`
- `2025-01-23`
- `2025-01-24`
- `2025-01-27`
- `2025-01-28`
- `2025-01-29`
- `2025-01-30`
- `2025-01-31`

The main cohort sizes used in the evaluation are:

- Utah: `8`
- CONUS: `200`, `681`, `1000`, `2365`

### Environment setup

Create a local environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip uninstall -y scidx_streaming
pip install -e ./streaming-py
```

If you need NDP-backed discovery for the scaling runs, create a local `.env` from the example file and fill in the required values:

```bash
cp .env.example .env
```

For the full scaling rerun, the scripts expect at least:

- `API_URL`
- `TOKEN`
- `SERVER`

The pushdown and figure-regeneration steps operate on archived local files and do not require the live demo service.

### Fastest path: rebuild figures from shipped CSVs

If you only want to reproduce the paper figures from the archived CSV outputs already in the repository, run:

```bash
python3 scripts/build_scaling_paper_figures.py

python3 scripts/run_adapters.py \
  --linear-runs-csv paper/results/exp_runs.csv \
  --kdtree-runs-csv paper/results/exp_runs_kdtree.csv \
  --selection-out-csv paper/results/exp_selection.csv

python3 scripts/plot_selection_comparison.py \
  --csv paper/results/exp_selection.csv \
  --figures-dir paper/figures
```

This regenerates:

- `paper/figures/scaling_latency_vs_sensor_count_ci.png`
- `paper/figures/scaling_latency_vs_sensor_count_ci.svg`
- `paper/figures/scaling_total_latency_ecdf.png`
- `paper/figures/scaling_total_latency_ecdf.svg`
- `paper/figures/linear_vs_kdtree_selection_clean.png`
- `paper/figures/linear_vs_kdtree_selection_clean.svg`

The pushdown summary figure is derived from the pushdown CSV outputs:

```bash
python3 scripts/run_pushdown.py
```

This refreshes:

- `paper/results/exp_pushdown.csv`
- `paper/results/pushdown_speedup_table.csv`

### Full experiment rerun from archived forecast inputs

To rerun the main evaluation from the archived prediction CSVs, execute:

```bash
python3 scripts/run_scaling.py \
  --selector linear \
  --out-csv paper/results/exp_runs.csv

python3 scripts/run_scaling.py \
  --selector kdtree \
  --out-csv paper/results/exp_runs_kdtree.csv

python3 scripts/run_pushdown.py

python3 scripts/run_adapters.py \
  --linear-runs-csv paper/results/exp_runs.csv \
  --kdtree-runs-csv paper/results/exp_runs_kdtree.csv \
  --selection-out-csv paper/results/exp_selection.csv

python3 scripts/build_scaling_paper_figures.py

python3 scripts/plot_selection_comparison.py \
  --csv paper/results/exp_selection.csv \
  --figures-dir paper/figures
```

These commands write or refresh:

- `paper/results/exp_runs.csv`
- `paper/results/exp_runs_kdtree.csv`
- `paper/results/scaling_summary.csv`
- `paper/results/scaling_ci_summary.csv`
- `paper/results/size_k_summary.csv`
- `paper/results/latency_percentiles.csv`
- `paper/results/exp_pushdown.csv`
- `paper/results/pushdown_speedup_table.csv`
- `paper/results/exp_selection.csv`
- `paper/figures/scaling_latency_vs_sensor_count_ci.{png,svg}`
- `paper/figures/scaling_total_latency_ecdf.{png,svg}`
- `paper/figures/linear_vs_kdtree_selection_clean.{png,svg}`

### Demo notebook path

The notebook path is intended for interactive understanding of the workflow rather than batch reruns of the full evaluation.

- Hosted demo notebook: `https://ndp-prod-194.chpc.utah.edu/jupyter/hub/spawn`
- Notebook file: `notebooks/aq.ipynb`

Use the hosted notebook or the public UI for a single-day walkthrough. Use the scripts above for the offline reproducibility path.

## Status

This repository is the active development workspace. For public release, prepare a clean carve-out that includes only the reviewed subset of source files, scripts, figures, results, and documentation needed to reproduce the eScience artifact.
