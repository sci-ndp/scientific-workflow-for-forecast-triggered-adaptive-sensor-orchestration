# A Scientific Workflow for Forecast-Triggered Adaptive Sensor Orchestration across the Computing Continuum

Code, scripts, figures, and paper sources for our forecast-triggered adaptive sensor orchestration workflow. The air-quality instantiation uses Aurora/CAMS forecasts, NDP for catalog discovery, and SciDx/Kafka for streaming.

## Video Walkthrough

[![Watch the air-quality workflow demo](https://img.youtube.com/vi/rStwL65TpL8/hqdefault.jpg)](https://youtu.be/rStwL65TpL8)

Notebook demo video: https://youtu.be/rStwL65TpL8

Hosted notebook demo: https://ndp-prod-194.chpc.utah.edu/jupyter/hub/spawn

Sign in to CILogon, open the **Air Quality Demo** workspace, and select the **NDP-EP/SciDx Air Quality Streaming** image. The session opens `notebooks/aq.ipynb`.

Notebook demo access requirements:
- Request access to the **Adaptive Air Quality Sensing Endpoint**: https://nationaldataplatform.org/endpoints/view/6980dfcf538c0550584c492d.
- Request access to **NDP-Earthscope-Endpoint**: https://nationaldataplatform.org/endpoints/view/69e915bac1996db0fc21d3ab to run the workspace.

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
- `streaming_v2/`: pinned SciDx v2 submodule used by the service and notebook.
- `src/streaming_support.py`: resource selection, registration, and Kafka message compatibility helpers.
- `ui/`: UI assets used by the endpoint web interface in the Docker demo deployment.

## Quick Start

### Option 1: Private-machine deployment with Docker Compose

Our demo deployment starts by creating an endpoint through https://nationaldataplatform.org/. After that, we run the NDP install script on a private machine and start this repository from this folder with Docker Compose.

Requirements:
- Docker with Compose support
- Copernicus ADS credentials available at `~/.cdsapirc`

Clone the repository, move into this folder, then start the services:

```bash
git clone --recurse-submodules <repo-url>
cd scientific-workflow-for-forecast-triggered-adaptive-sensor-orchestration
docker compose up --build -d
```

The Compose stack starts Redis, the forecast API, and an RQ worker on the private machine.

In our deployment, the public demo URLs point to that private machine:

- `https://dev.air-quality.ndp.utah.edu/` -> local `http://localhost:8000/`
- `https://dev.air-quality.ndp.utah.edu/ep/ui/` -> local `http://localhost:8002/ep/ui/`

This forwarding setup is specific to our demo deployment. Other users can deploy the system however they prefer on their own infrastructure.

The public endpoints are provided so others can try the system, explore the UI, and get a sense of the workflow behavior. They are proof-of-concept demo services, not production deployments.

### Option 2: Local Python environment

```bash
git submodule update --init streaming_v2
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn src.forecast_api:app --host 0.0.0.0 --port 8000
```

**The experiment scripts that query NDP discovery expect environment variables such as `API_URL`, `TOKEN`, `SERVER`, and `SERVICE_BASE_URL` to be available through your shell or a local `.env` file.**

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
git submodule update --init streaming_v2
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
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
- `paper/figures/scaling_total_latency_ecdf.png`
- `paper/figures/linear_vs_kdtree_selection_clean.png`

The pushdown summary CSVs and figure can be regenerated with:

```bash
python3 scripts/run_pushdown.py

python3 scripts/plot_pushdown_summary.py \
  --summary-csv paper/results/pushdown_speedup_table.csv \
  --figures-dir paper/figures
```

This refreshes:

- `paper/results/exp_pushdown.csv`
- `paper/results/pushdown_speedup_table.csv`
- `paper/figures/pushdown_summary.png`

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

python3 scripts/plot_pushdown_summary.py \
  --summary-csv paper/results/pushdown_speedup_table.csv \
  --figures-dir paper/figures

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
- `paper/figures/scaling_latency_vs_sensor_count_ci.png`
- `paper/figures/scaling_total_latency_ecdf.png`
- `paper/figures/linear_vs_kdtree_selection_clean.png`
- `paper/figures/pushdown_summary.png`

### Demo notebook path

The notebook path is intended for interactive understanding of the workflow rather than batch reruns of the full evaluation.

- Hosted demo notebook: `https://ndp-prod-194.chpc.utah.edu/jupyter/hub/spawn`
- Notebook file: `notebooks/aq.ipynb`

Use the hosted notebook or the public UI for a single-day walkthrough. Use the scripts above for the offline reproducibility path.

## Status

This repository is the active development workspace. 

## Streaming v2 migration

This branch uses Python 3.12+ and `scidx_streaming_v2`. Run installation commands
from the repository root; `requirements.txt` installs the pinned local submodule.
For an existing checkout, run `git submodule update --init streaming_v2` first.
The legacy `streaming-py/` source stays on `main`; this branch contains only the
`streaming_v2/` dependency. Branch changes are committed independently.

Resource UUIDs accepted by the HTTP API are translated to v2 dataset/resource
name selectors. The optional `dataset_names` query parameter supplies source
dataset hints on older endpoints; the dashboard passes these automatically to
avoid slow catalog-wide lookups. The dashboard's `data_stream_id` response field is preserved.
The notebook uses `search_resources`, `acreate_stream`, and `result.topic`, with
no hard-coded resource UUID or Kafka topic. Filters use v2 comparison syntax
such as `value > 10`; natural-language filter descriptions need conversion.
V2 registers a derived stream on the first source dataset unless a target is
specified, rather than on every source dataset as the legacy library did.

Consumers explicitly start and stop v2 handles. A shared deserializer reads both
v2 JSON and legacy Blosc/MessagePack payloads. Callback queues track new messages
independently of the bounded dataframe preview; queue overflow reports an error.
The preview consumer assigns partitions before its initial seek to preserve
the Kafka backlog. The notebook loads complete hotspot and sensor-mapping CSVs
instead of the API previews and uses the returned prediction URL directly.

WebSocket (`ws://` / `wss://`) source ingestion is not implemented in the pinned
v2 library. The Synoptic feed cannot create a live stream on this branch until
that upstream support is added or the feed is bridged into Kafka. Discovery and
consumption of existing Kafka topics can still be used. The notebook reports
this limitation at stream creation and clears previous execution outputs.
Set `AQ_EXISTING_TOPIC` in `.env` to read an existing topic after skipping the
WebSocket creation cell. Consumption waits at most 60 seconds by default;
`AQ_MAX_WAIT_SECONDS` and `AQ_MAX_MESSAGES` control those limits. An empty topic
returns zero messages when the wait expires.

Offline migration checks (no credentials or running services required):

```bash
pip install -r requirements-dev.txt
python -m pytest -q tests
python -m pytest -q streaming_v2/tests -m 'not live'
```

These checks include the complete FastAPI application and execution of the changed
notebook cells with offline catalog and Kafka fixtures. The live deployment was
also checked with archived predictions and a temporary real Kafka topic; fresh
CAMS/Aurora downloads and inference were not rerun.

The forecast service runs without the optional agent-tool, exact-analytics,
station-log normalization, and saved-run-history integrations. Their imports,
initialization, and dependent `/exact/*`, `/tools/invoke`, and `/saved-results/*`
routes have been removed. Forecast jobs retain their RQ progress/status and return
prediction results directly; stream consumption retains message counts and recent
payloads. The notebook does not depend on the removed routes.

See [the migration validation report](docs/streaming-v2-validation.md) for results,
live endpoint checks, and remaining verification limits.

Compose caps container diagnostics at three 10 MB files. Services restart
automatically. Set `AQ_DATA_DIR` to preserve existing deployment data when
switching checkouts. Host log retention requires administrator access:

```bash
sudo bash scripts/configure_host_log_retention.sh
```
