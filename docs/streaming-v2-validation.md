# Streaming v2 validation and log maintenance

Validated October 22, 2026 on `migrate/streaming-v2`. The migration is committed separately from the legacy `main` branch.
Upstream streaming source remains unchanged at
`2ba8b56baf82ee25e4947a705e044a9b0eed8001`.

## Deployment and verification

The API and Aurora RQ worker run `forecast-streaming-v2:latest`, built from this
branch using the existing scientific Python image. The original Redis instance
and deployment data directory were preserved. Compose loads credentials from
the runtime `.env` file; new builds exclude `.env`, virtual environments, Git
metadata, and legacy streaming source. The API has a health check and both
application services restart automatically.

- 32 application/notebook tests passed, including full FastAPI startup,
  registration, lookup, stream creation, consumer cleanup, forecast job
  generation/reuse/failure, complete CSV loading, and startup backlog delivery.
- 233 upstream offline tests passed; 10 opt-in live tests were deselected.
- Notebook schema/compilation, Compose validation, Dockerfile checks, image
  dependency checks, shell syntax, and `git diff --check` passed.
- The migrated image was built and the API and worker started.
- Live notebook execution verified service discovery, submission/status,
  prediction loading, hotspot detection/loading, sensor selection/loading, and
  resource discovery. The January 16, 2025 Utah example returned 3,456 prediction
  rows, 67 hotspots, and 67 mappings covering 13 sensors. This exercised archived
  prediction reuse; fresh Aurora/CAMS downloads and inference were not rerun.
- A temporary real Kafka topic delivered all five test records (two JSON and
  three legacy compressed records) through the notebook. The consumer stopped
  and the temporary topic was deleted.

The live Kafka check exposed an upstream issue: startup polls while waiting for
partition assignment discard records. The application factory now assigns
preview partitions before v2 seeks its starting offset, preserving the backlog
without changing upstream source. The notebook loads complete hotspot and
mapping CSVs instead of the 10-row previews and avoids a redundant catalog query
when the workflow has returned a prediction URL.

Absent optional agent, exact-analytics, station-log, and saved-history imports
and their dependent routes were removed so the application can start. The
notebook does not use them; RQ job status and catalog reuse remain available.

WebSocket ingestion remains unsupported. Its creation cell raises an explicit
`NotImplementedError` and clears stale topics. Skip that cell and use
`AQ_EXISTING_TOPIC` to consume an existing topic. The configured old demo topic
currently has no retained messages (beginning/end offsets both 28), so an empty
result is expected until a producer sends more data. The default wait is 60
seconds; `AQ_MAX_WAIT_SECONDS` and `AQ_MAX_MESSAGES` adjust the limits.
Kafka-backed creation is covered by offline contract tests; no live Synoptic
WebSocket-derived stream was created.

## Log cleanup and retention

Identified application diagnostic files were cleared. Recreating the endpoint
also removed its accumulated writable-layer logs. Its dataset search was
dumping every catalog record to stdout; that statement was removed in the
endpoint checkout and the repaired module is mounted read-only by its Compose
service. Internal endpoint logs fell from tens of megabytes to about 350 KB
after the repair and verification requests.

All 14 containers have Docker diagnostic-log rotation set to 10 MB per file and
three files. The previously unbounded Hawser container was recreated with its
existing image, environment, socket mount, port, and restart policy; its health
check passed before the original container was removed.

The existing cron job runs hourly at minute 17:

```text
/usr/bin/python3 /uufs/chpc.utah.edu/common/home/u1494915/.local/bin/ndp-log-maintenance.py
```

It removes CMAK/Solr archives after three days and old closed endpoint metrics
logs after seven days. A manual run succeeded for all three containers. Kafka
message segments and database recovery journals were preserved.

Host system-journal and `/var/log` cleanup remains pending: `sudo -n` requires a
password. No host log files were removed. An administrator can apply the
prepared configuration from this repository:

```bash
sudo bash scripts/configure_host_log_retention.sh
```

The script limits persistent journals to 100 MB/seven days and runtime journals
to 50 MB, bounds the existing rsyslog rule, vacuums old journals, and runs
logrotate hourly. It was syntax-checked and has not been executed as root.

## Repeat the offline checks

```bash
python -m pytest -q tests
python -m pytest -q streaming_v2/tests -m 'not live' -o addopts=
docker compose config --quiet
docker build --check .
bash -n scripts/configure_host_log_retention.sh
```

The host retention script explicitly sets `su root adm` in the rsyslog rule.
This fixes direct rotation failing because it did not inherit the global user
and group settings. Restarting the timer applies its hourly schedule.
