import uuid
import traceback
import asyncio
import inspect
import os
import re
import math
import json
import hashlib
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Literal
from urllib.parse import quote, unquote, urlparse, parse_qs

import pandas as pd
from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv
import yaml
from ndp_ep import APIClient
from redis import Redis
from redis.exceptions import RedisError
from rq import Queue, get_current_job
from rq.exceptions import NoSuchJobError
from rq.job import Job
warnings.filterwarnings(
    "ignore",
    message=r"(?s).*google\.generativeai.*",
    category=FutureWarning,
)
from scidx_streaming import StreamingClient
import logging

from src.air_quality_workflow_service import AirQualityWorkflowService

app = FastAPI(title="Forecast Service", version="1.0")

# configure module logger
logger = logging.getLogger("forecast_api")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

load_dotenv(override=False)

FIXED_AURORA_CONFIG = {
    "times": ["00:00", "12:00"],
    "lead_hours": 12,
    "region_tag": "global",
}

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MAX_DAYS_PER_REQUEST = max(1, int(os.getenv("AQ_MAX_DAYS", "1")))
MAX_QUEUE_LENGTH = max(1, int(os.getenv("AQ_MAX_QUEUE_LENGTH", "2")))
MAX_ENQUEUES_PER_HOUR = max(1, int(os.getenv("AQ_MAX_ENQUEUES_PER_HOUR", "2")))
# Long-running forecast downloads can exceed 30 minutes, but we cap them at 1 hour.
RQ_JOB_TIMEOUT_SECONDS = min(
    3600,
    max(1800, int(os.getenv("AQ_RQ_JOB_TIMEOUT_SECONDS", "3600"))),
)
RQ_JOB_TTL_SECONDS = 3600
REQUEST_KEY_TTL_SECONDS = 2 * 3600
ENQUEUE_WINDOW_SECONDS = 3600
REDIS = Redis.from_url(REDIS_URL)
AURORA_QUEUE = Queue("aurora", connection=REDIS, default_timeout=RQ_JOB_TIMEOUT_SECONDS)
ENQUEUE_EVENTS_KEY = "forecast_api:enqueue_events"
REQUEST_KEY_PREFIX = "forecast_api:request"
ENQUEUE_GUARD_LOCK = "forecast_api:enqueue_guard"


@app.on_event("startup")
def log_env_on_startup():
    # Load .env without overriding existing envs
    load_dotenv(override=False)
    token = os.getenv("TOKEN")
    api_url = os.getenv("API_URL")
    svc = os.getenv("SERVICE_BASE_URL")
    server = os.getenv("SERVER")
    logger.info("ENV: TOKEN set=%s, API_URL=%s, SERVICE_BASE_URL=%s, SERVER=%s",
                bool(token), api_url or "<unset>", svc or "<unset>", server or "<unset>")


_DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Forecast Service</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Lexend:wght@500;600&family=Roboto:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
  :root{
    --ndp-bg:#f4f8fc;
    --ndp-surface:#ffffff;
    --ndp-surface-alt:#f7fbff;
    --ndp-surface-soft:#eef5fb;
    --ndp-line:#d7e3ef;
    --ndp-line-strong:#bdd0e4;
    --ndp-text:#14263a;
    --ndp-text-muted:#63788f;
    --ndp-primary:#0071ff;
    --ndp-primary-hover:#0885ff;
    --ndp-primary-soft:#edf5ff;
    --ndp-primary-soft-strong:#dff0ff;
    --ndp-danger:#c2574c;
    --ndp-success:#2f8f62;
    --bg:var(--ndp-bg);
    --card:var(--ndp-surface);
    --accent:var(--ndp-primary);
    --accent-hover:var(--ndp-primary-hover);
    --accent-dark:#005bcf;
    --border:var(--ndp-line);
    --text:var(--ndp-text);
    --muted:var(--ndp-text-muted);
    --success:var(--ndp-success);
    --danger:var(--ndp-danger);
    --warn:#b9872f;
    --radius:10px;
    --shadow:none;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:'Roboto','Helvetica Neue',sans-serif;background:var(--bg);color:var(--text);line-height:1.5}
  a{color:var(--accent-dark);text-decoration:none}
  a:hover{text-decoration:underline}
  header{background:var(--card);border-bottom:1px solid var(--border)}
  .header-inner{max-width:1200px;margin:0 auto;padding:16px 18px;display:flex;justify-content:space-between;align-items:center;gap:12px}
  .brand{display:flex;align-items:center;gap:14px;min-width:0}
  .brand-logo{width:220px;max-width:48vw;height:auto}
  .brand-copy{min-width:0}
  header h1{font-family:'Lexend','Roboto',sans-serif;font-size:1.5rem;font-weight:600;letter-spacing:.1px}
  header p{font-size:.88rem;margin-top:3px;color:var(--muted)}
  .header-actions{display:flex;align-items:center;gap:8px}
  .header-docs{padding:8px 14px;border-radius:8px;background:var(--ndp-primary-soft);color:var(--accent-dark);
               font-size:.82rem;font-weight:500;border:1px solid var(--ndp-line-strong);transition:background .16s,border-color .16s,color .16s;
               text-decoration:none;display:inline-flex;align-items:center;justify-content:center;white-space:nowrap}
  .header-docs:hover{background:var(--ndp-primary-soft-strong);border-color:var(--accent);color:var(--accent-dark);text-decoration:none}
  .container{max-width:1520px;margin:20px auto;padding:0 18px}
  .stepper{display:flex;gap:6px;flex-wrap:nowrap;overflow-x:auto;margin-bottom:16px;padding:10px;border:1px solid var(--border);border-radius:var(--radius);background:var(--card)}
  .step-pill{display:flex;align-items:center;gap:8px;padding:6px 10px;border:1px solid var(--ndp-line-strong);border-radius:8px;
             font-size:.75rem;font-weight:500;background:var(--ndp-surface-alt);color:var(--muted);cursor:pointer;transition:background .16s,border-color .16s,color .16s;
             white-space:nowrap;flex:0 0 auto}
  .step-pill:hover{border-color:var(--accent);color:var(--accent-dark)}
  .step-pill.active{background:var(--accent);color:#fff;border-color:var(--accent)}
  .step-pill.done{background:var(--success);color:#fff}
  .step-num{width:20px;height:20px;border-radius:6px;display:flex;align-items:center;justify-content:center;
            font-size:.68rem;font-weight:600;background:rgba(255,255,255,.24)}
  .card{background:var(--card);border-radius:var(--radius);box-shadow:var(--shadow);padding:22px;margin-bottom:16px;display:none;
        border:1px solid var(--border)}
  .card.visible{display:block}
  .card h2{font-family:'Lexend','Roboto',sans-serif;font-size:1.12rem;font-weight:600;margin-bottom:14px;color:var(--text)}
  .form-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px 18px}
  .form-group{display:flex;flex-direction:column;gap:3px}
  .form-group.full{grid-column:1/-1}
  label{font-size:.78rem;font-weight:500;color:var(--muted)}
  input,select,textarea{border:1px solid var(--border);border-radius:8px;padding:8px 10px;font-size:.86rem;width:100%;
                        transition:border-color .16s,box-shadow .16s;outline:none;font-family:inherit;background:#fff;color:var(--text)}
  input:focus,select:focus,textarea:focus{border-color:var(--accent);box-shadow:0 0 0 2px rgba(0,113,255,.13)}
  textarea{resize:vertical;min-height:60px;font-family:'Cascadia Code',monospace;font-size:.8rem}
  .btn{display:inline-flex;align-items:center;gap:6px;padding:9px 18px;border-radius:8px;border:1px solid transparent;
       font-size:.84rem;font-weight:500;cursor:pointer;transition:background .16s,border-color .16s,color .16s}
  .btn-primary{background:var(--accent);color:#fff}
  .btn-primary:hover{background:var(--accent-hover)}
  .btn-pink{background:var(--ndp-primary-soft);color:var(--accent-dark);border-color:var(--ndp-line-strong)}
  .btn-pink:hover{background:var(--ndp-primary-soft-strong);border-color:var(--accent)}
  .btn-outline{background:#fff;color:var(--accent-dark);border-color:var(--ndp-line-strong)}
  .btn-outline:hover{background:var(--ndp-primary-soft);border-color:var(--accent);color:var(--accent-dark)}
  .btn-sm{padding:6px 12px;font-size:.77rem}
  .btn:disabled{opacity:.45;cursor:not-allowed}
  .actions{display:flex;gap:10px;margin-top:16px;flex-wrap:wrap;align-items:center}
  .log{background:#15293f;color:#dbe8f8;border:1px solid #1f3a57;border-radius:8px;padding:14px;font-family:'Cascadia Code',monospace;
       font-size:.77rem;max-height:260px;overflow-y:auto;white-space:pre-wrap;margin-top:12px;line-height:1.5}
  #consume-live{min-height:220px;height:280px;max-height:420px;overflow-y:auto;white-space:pre;line-height:1.35}
  #consume-log{max-height:360px}
  .badge{display:inline-block;padding:3px 10px;border-radius:8px;font-size:.72rem;font-weight:600;border:1px solid transparent}
  .badge-ok{background:#e9f7f0;color:#246348;border-color:#c6e6d6}
  .badge-fail{background:#fff0ee;color:#8f322d;border-color:#f2c8c3}
  .badge-run{background:#fff6df;color:#805e16;border-color:#ebd9a6}
  .badge-queue{background:var(--ndp-primary-soft);color:#205897;border-color:var(--ndp-line-strong)}
  table{width:100%;border-collapse:collapse;font-size:.82rem;margin-top:10px}
  th,td{padding:7px 10px;text-align:left;border-bottom:1px solid var(--border)}
  th{background:var(--ndp-surface-soft);font-weight:600;color:var(--muted);font-size:.73rem}
  tr:hover td{background:var(--ndp-primary-soft)}
  .file-list{list-style:none;max-height:240px;overflow-y:auto}
  .file-list li{padding:6px 0;border-bottom:1px solid #e5edf5;font-size:.82rem}
  .spinner{display:inline-block;width:16px;height:16px;border:2.5px solid #d7e4ef;border-top-color:var(--accent-dark);
           border-radius:50%;animation:spin .7s linear infinite}
  @keyframes spin{to{transform:rotate(360deg)}}
  .nav-btns{display:flex;justify-content:space-between;align-items:center;margin-top:18px}
  .hidden{display:none!important}
  .preview-table-wrap{max-height:300px;overflow:auto;margin-top:10px}
  .fig-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:14px}
  .fig-grid img{width:100%;border-radius:8px;border:1px solid var(--border);cursor:pointer;transition:border-color .16s}
  .fig-grid img:hover{border-color:var(--accent)}
  .fig-caption{font-size:.75rem;color:var(--muted);text-align:center;margin-top:4px}
  .lb-overlay{position:fixed;inset:0;background:rgba(0,0,0,.72);display:flex;align-items:center;justify-content:center;
              z-index:9999;cursor:zoom-out;display:none}
  .lb-overlay.open{display:flex}
  .lb-overlay img{max-width:92vw;max-height:90vh;border-radius:10px;box-shadow:0 4px 30px rgba(0,0,0,.4)}
  @media(max-width:1200px){
    .stepper{flex-wrap:wrap;overflow-x:visible;padding:12px;gap:8px}
    .step-pill{font-size:.77rem;padding:6px 10px}
  }
  @media(max-width:900px){
    .header-inner{flex-direction:column;align-items:flex-start}
    .header-actions{width:100%}
  }
  @media(max-width:640px){
    .form-grid{grid-template-columns:1fr}
    .fig-grid{grid-template-columns:1fr}
    .brand-logo{width:170px}
    .stepper{padding:10px;gap:6px}
    .step-pill{padding:6px 8px;font-size:.74rem}
  }
</style>
</head>
<body>
<header>
  <div class="header-inner">
    <div class="brand">
      <img class="brand-logo" src="/assets/ndp-logo.png" alt="National Data Platform logo"/>
      <div class="brand-copy">
        <h1>Forecast Service</h1>
        <p>End-to-end air-quality forecast workflow from CAMS download to sensor orchestration.</p>
      </div>
    </div>
    <div class="header-actions">
      <a class="header-docs" href="/docs" target="_blank">API Docs</a>
      <a class="header-docs" href="/health">Health</a>
    </div>
  </div>
</header>

<div class="container">

<!-- Stepper -->
<div class="stepper" id="stepper">
  <div class="step-pill active" data-step="0"><span class="step-num">1</span> Run Model</div>
  <div class="step-pill" data-step="1"><span class="step-num">2</span> Job Status</div>
  <div class="step-pill" data-step="2"><span class="step-num">3</span> Prediction Files</div>
  <div class="step-pill" data-step="3"><span class="step-num">4</span> Detect Hotspots</div>
  <div class="step-pill" data-step="4"><span class="step-num">5</span> Hotspot Files</div>
  <div class="step-pill" data-step="5"><span class="step-num">6</span> Select Sensors</div>
  <div class="step-pill" data-step="6"><span class="step-num">7</span> Sensors</div>
  <div class="step-pill" data-step="7"><span class="step-num">8</span> Create Stream</div>
  <div class="step-pill" data-step="8"><span class="step-num">9</span> Consume Stream</div>
  <div class="step-pill" data-step="9"><span class="step-num">10</span> Results</div>
</div>

<!-- STEP 0 — Run Model -->
<div class="card visible" id="step-0">
  <h2>1 &middot; Run Forecast Model</h2>
  <p style="font-size:.84rem;color:var(--muted);margin-bottom:14px">Configure region and date range, then launch the workflow.</p>
  <div class="form-grid">
    <div class="form-group"><label>Region name</label><input id="r-name" value="utah"/></div>
    <div class="form-group"><label>Overwrite existing</label>
      <select id="r-overwrite"><option value="false" selected>No</option><option value="true">Yes</option></select></div>
    <div class="form-group"><label>Lat min</label><input id="r-lat-min" type="number" step="0.1" value="37.0"/></div>
    <div class="form-group"><label>Lat max</label><input id="r-lat-max" type="number" step="0.1" value="42.1"/></div>
    <div class="form-group"><label>Lon min</label><input id="r-lon-min" type="number" step="0.1" value="245.9"/></div>
    <div class="form-group"><label>Lon max</label><input id="r-lon-max" type="number" step="0.1" value="251.0"/></div>
    <div class="form-group"><label>Start date</label><input id="r-start" type="date" value="2025-01-16"/></div>
    <div class="form-group"><label>End date</label><input id="r-end" type="date" value="2025-01-16"/></div>
  </div>
  <div class="actions">
    <button class="btn btn-primary" id="btn-run" onclick="submitRun()">Launch Workflow</button>
    <span id="run-status"></span>
  </div>
  <div class="log hidden" id="run-log"></div>
  <div class="nav-btns"><span></span><button class="btn btn-outline btn-sm" onclick="goStep(1)">Next &rarr;</button></div>
</div>

<!-- STEP 1 — Job Status -->
<div class="card" id="step-1">
  <h2>2 &middot; Job Status</h2>
  <div class="form-grid" style="grid-template-columns:1fr auto">
    <div class="form-group"><label>Job ID</label><input id="s-jobid" placeholder="paste or auto-filled from step 1"/></div>
    <div class="form-group" style="justify-content:flex-end"><button class="btn btn-primary btn-sm" onclick="pollStatus()" id="btn-poll">Poll Status</button></div>
  </div>
  <div id="status-box" style="margin-top:12px"></div>
  <div class="log hidden" id="status-log"></div>
  <div class="nav-btns">
    <button class="btn btn-outline btn-sm" onclick="goStep(0)">&larr; Back</button>
    <button class="btn btn-outline btn-sm" onclick="goStep(2)">Next &rarr;</button>
  </div>
</div>

<!-- STEP 2 — Prediction Files -->
<div class="card" id="step-2">
  <h2>3 &middot; Prediction Files</h2>
  <div class="actions">
    <button class="btn btn-primary btn-sm" id="btn-pred-files" onclick="togglePredictionFiles()">Load Prediction Files</button>
  </div>
  <ul class="file-list" id="pred-list" style="margin-top:10px"><li style="color:var(--muted)">Click load to show&hellip;</li></ul>
  <div class="preview-table-wrap" id="pred-preview"></div>
  <div class="nav-btns">
    <button class="btn btn-outline btn-sm" onclick="goStep(1)">&larr; Back</button>
    <button class="btn btn-outline btn-sm" onclick="goStep(3)">Next &rarr;</button>
  </div>
</div>

<!-- STEP 3 — Detect Hotspots -->
<div class="card" id="step-3">
  <h2>4 &middot; Detect Hotspots</h2>
  <p style="font-size:.84rem;color:var(--muted);margin-bottom:14px">Select a prediction file and configure hotspot detection parameters.</p>
  <div class="form-grid">
    <div class="form-group full"><label>Prediction file (URL, ref, or filename)</label>
      <input id="h-predfile" placeholder="e.g. prediction:2025-01-16_0000-1200_12h_utah.csv"/></div>
    <div class="form-group"><label>Default threshold</label><input id="h-thresh" type="number" step="0.1" value="10.0"/></div>
    <div class="form-group"><label>Persistence k</label><input id="h-pk" type="number" value="12"/></div>
    <div class="form-group"><label>Timestep hours</label><input id="h-ts" type="number" value="1"/></div>
    <div class="form-group"><label>Top-k hotspots (0 = all)</label><input id="h-topk" type="number" value="0"/></div>
    <div class="form-group full">
      <label>Threshold Fields (checkbox + per-field threshold)</label>
      <div class="actions" style="margin-top:6px">
        <button class="btn btn-outline btn-sm" id="btn-h-preview" onclick="toggleHotspotInputPreview()">Preview File and Load Fields</button>
        <span id="h-preview-status" style="font-size:.78rem;color:var(--muted)"></span>
      </div>
      <div id="h-field-options" class="preview-table-wrap" style="margin-top:8px"></div>
    </div>
  </div>
  <div class="actions">
    <button class="btn btn-pink" id="btn-detect" onclick="detectHotspots()">Detect Hotspots</button>
    <span id="detect-status"></span>
  </div>
  <div class="preview-table-wrap" id="h-pred-preview"></div>
  <div class="preview-table-wrap" id="hotspot-preview"></div>
  <div class="log hidden" id="detect-log"></div>
  <div class="nav-btns">
    <button class="btn btn-outline btn-sm" onclick="goStep(2)">&larr; Back</button>
    <button class="btn btn-outline btn-sm" onclick="goStep(4)">Next &rarr;</button>
  </div>
</div>

<!-- STEP 4 — Hotspot Files (like Predictions) -->
<div class="card" id="step-4">
  <h2>5 &middot; Hotspot Files</h2>
  <p style="font-size:.84rem;color:var(--muted);margin-bottom:10px">Browse all generated hotspot CSV files. Click a file to preview, download, or use it for sensor selection.</p>
  <div class="actions">
    <button class="btn btn-primary btn-sm" id="btn-hot-files" onclick="toggleHotspotFiles()">Load Hotspot Files</button>
  </div>
  <ul class="file-list" id="hot-list" style="margin-top:10px"><li style="color:var(--muted)">Click load to show&hellip;</li></ul>
  <div class="preview-table-wrap" id="hot-preview"></div>
  <div class="nav-btns">
    <button class="btn btn-outline btn-sm" onclick="goStep(3)">&larr; Back</button>
    <button class="btn btn-outline btn-sm" onclick="goStep(5)">Next &rarr;</button>
  </div>
</div>

<!-- STEP 5 — Select Sensors -->
<div class="card" id="step-5">
  <h2>6 &middot; Select Sensors</h2>
  <div class="form-grid">
    <div class="form-group full"><label>Hotspots file (URL, ref, or filename)</label>
      <input id="ss-file" placeholder="e.g. hotspot:2025-01-16_0000-1200_12h_utah_k12_hotspots.csv"/></div>
    <div class="form-group"><label>Selection mode</label>
      <select id="ss-mode"><option value="nearest_k" selected>nearest_k</option><option value="radius_km">radius_km</option></select></div>
    <div class="form-group"><label>k (nearest sensors)</label><input id="ss-k" type="number" value="1" min="1"/></div>
    <div class="form-group"><label>Radius km</label><input id="ss-radius" type="number" step="0.1" value="10.0"/></div>
  </div>
  <div class="actions">
    <button class="btn btn-pink" id="btn-sensor" onclick="selectSensors()">Select Sensors</button>
    <span id="sensor-status"></span>
  </div>
  <div class="preview-table-wrap" id="sensor-preview"></div>
  <div class="log hidden" id="sensor-log"></div>
  <div class="nav-btns">
    <button class="btn btn-outline btn-sm" onclick="goStep(4)">&larr; Back</button>
    <button class="btn btn-outline btn-sm" onclick="goStep(6)">Next &rarr;</button>
  </div>
</div>

<!-- STEP 6 — Sensors -->
<div class="card" id="step-6">
  <h2>7 &middot; Sensors</h2>
  <p style="font-size:.84rem;color:var(--muted);margin-bottom:14px">Discover registered sensors and look up their consumption methods for streaming.</p>
  <div class="form-grid">
    <div class="form-group"><label>Server override (optional)</label><input id="sen-server" placeholder="leave blank for default"/></div>
  </div>
  <div class="actions">
    <button class="btn btn-primary btn-sm" id="btn-sensors" onclick="loadSensorList()">Load Sensors</button>
    <span id="sensors-status"></span>
  </div>
  <div class="preview-table-wrap" id="sensors-table" style="margin-top:10px"></div>

  <h3 style="margin-top:20px;font-size:1rem;color:var(--accent-dark)">Consumption Methods</h3>
  <div class="form-grid" style="margin-top:8px">
    <div class="form-group full"><label>Sensor name</label>
      <input id="cm-sensor-name" placeholder="e.g. synoptic_sensor_qsvn_utah_pm2_5"/></div>
  </div>
  <div class="actions">
    <button class="btn btn-pink btn-sm" id="btn-cm" onclick="lookupConsumptionMethod()">Lookup Methods</button>
    <span id="cm-status"></span>
  </div>
  <div class="preview-table-wrap" id="cm-table" style="margin-top:10px"></div>
  <div class="log hidden" id="cm-log"></div>
  <div class="nav-btns">
    <button class="btn btn-outline btn-sm" onclick="goStep(5)">&larr; Back</button>
    <button class="btn btn-outline btn-sm" onclick="goStep(7)">Next &rarr;</button>
  </div>
</div>

<!-- STEP 7 — Create Stream -->
<div class="card" id="step-7">
  <h2>8 &middot; Create Stream</h2>
  <p style="font-size:.84rem;color:var(--muted);margin-bottom:14px">Create a Kafka stream from one or more consumption method IDs.</p>
  <div class="form-grid">
    <div class="form-group full"><label>Consumption method IDs (comma-separated)</label>
      <input id="cs-method-ids" placeholder="e.g. 8332f19d-8bf6-43ce-af88-ff2bd9a9bc07"/></div>
    <div class="form-group full"><label>Filter semantics (comma-separated, optional)</label>
      <input id="cs-filters" placeholder="leave blank for none"/></div>
    <div class="form-group"><label>Server override (optional)</label><input id="cs-server" placeholder="leave blank for default"/></div>
  </div>
  <div class="actions">
    <button class="btn btn-primary" id="btn-create-stream" onclick="createStream()">Create Stream</button>
    <span id="stream-create-status"></span>
  </div>
  <div class="log hidden" id="stream-create-log"></div>
  <div class="nav-btns">
    <button class="btn btn-outline btn-sm" onclick="goStep(6)">&larr; Back</button>
    <button class="btn btn-outline btn-sm" onclick="goStep(8)">Next &rarr;</button>
  </div>
</div>

<!-- STEP 8 — Consume Stream -->
<div class="card" id="step-8">
  <h2>9 &middot; Consume Stream</h2>
  <p style="font-size:.84rem;color:var(--muted);margin-bottom:14px">Start consuming messages from a stream topic.</p>
  <div class="form-grid">
    <div class="form-group full"><label>Topic / data_stream_id</label>
      <input id="co-topic" placeholder="auto-filled from Create Stream, or paste a topic"/></div>
    <div class="form-group"><label>Run mode</label>
      <select id="co-mode" onchange="toggleConsumeMode()">
        <option value="timed" selected>Auto-close (60 min)</option>
        <option value="manual">Run until stopped</option>
      </select>
    </div>
    <div class="form-group" id="co-auto-wrap"><label>Auto-close minutes (strict cap)</label><input id="co-auto-minutes" type="number" value="60" min="1" max="1440"/></div>
    <div class="form-group"><label>Poll interval (seconds)</label><input id="co-poll" type="number" step="0.5" value="2.0" min="0.5"/></div>
    <div class="form-group"><label>Max messages</label><input id="co-max" type="number" value="5000" min="1"/></div>
  </div>
  <div class="actions">
    <button class="btn btn-pink" id="btn-consume" onclick="startConsume()">Start Consuming</button>
    <button class="btn btn-outline" id="btn-consume-stop" onclick="stopConsume()">Stop</button>
    <button class="btn btn-outline" id="btn-consume-status" onclick="checkConsumeStatus()">Stream Status</button>
    <span id="consume-status"></span>
  </div>
  <div id="consume-meta" style="font-size:.8rem;color:var(--muted);margin-top:8px"></div>
  <div class="form-group full" style="margin-top:10px">
    <label>Live Feed (recent payload messages)</label>
    <textarea id="consume-live" readonly placeholder="Live messages appear here" spellcheck="false"
      style="height:240px;min-height:240px;max-height:240px;overflow-y:scroll;overflow-x:auto;white-space:pre;resize:none;line-height:1.35"></textarea>
  </div>
  <div class="log hidden" id="consume-log"></div>
  <div class="nav-btns">
    <button class="btn btn-outline btn-sm" onclick="goStep(7)">&larr; Back</button>
    <button class="btn btn-outline btn-sm" onclick="goStep(9)">Next &rarr;</button>
  </div>
</div>

<!-- STEP 9 — Results (Baseline Evaluation Figures) -->
<div class="card" id="step-9">
  <h2>10 &middot; Results</h2>
  <div id="summary-content" style="font-size:.88rem;color:var(--muted);margin-bottom:14px">
    14-Day Utah Baseline Evaluation &mdash; figures across multiple persistence settings. Click any image to enlarge.
  </div>
  <div class="fig-grid">
    <div>
      <img src="/static-figs/summary_metrics_by_k.svg" alt="Summary by k" onclick="openLB(this)"
        onerror="this.onerror=null;this.src='/static-figs/figS1_summary_small_multiples_bars.png';"/>
      <div class="fig-caption">Fig 1 &mdash; Summary Metrics by Persistence (k)</div>
      <p style="font-size:.78rem;color:var(--muted);margin-top:6px;line-height:1.45">Mean hotspot count and relative sensor workload (% of always-on 5 sensors) averaged across 14 days, grouped by persistence setting k. Higher k produces fewer hotspots and lower workload.</p>
    </div>
    <div>
      <img src="/static-figs/fig_latency_option3_stage_small_multiples.svg" alt="Latency" onclick="openLB(this)"
        onerror="this.onerror=null;this.src='/static-figs/fig_latency_option3_stage_small_multiples.png';"/>
      <div class="fig-caption">Fig 2 &mdash; Stage-wise Latency</div>
      <p style="font-size:.78rem;color:var(--muted);margin-top:6px;line-height:1.45">Box-and-jitter plots showing the distribution of detect latency, sensor-selection latency, and total workflow latency (seconds) for each persistence k across all 14 evaluation days.</p>
    </div>
    <div>
      <img src="/static-figs/per_day_workload_reduction.svg" alt="Workload reduction" onclick="openLB(this)"
        onerror="this.onerror=null;this.src='/static-figs/fig_workload_reduction_by_day_colored_lines.png';"/>
      <div class="fig-caption">Fig 3 &mdash; Per-day Workload Reduction</div>
      <p style="font-size:.78rem;color:var(--muted);margin-top:6px;line-height:1.45">Per-day workload reduction (%) vs. always-on 5 sensors across persistence settings. Each colored line represents one of the 14 forecast days; steeper curves indicate greater efficiency gains at higher k.</p>
    </div>
    <div>
      <img src="/static-figs/fig_hotspot_count_heatmap_blue.svg" alt="Hotspot heatmap" onclick="openLB(this)"
        onerror="this.onerror=null;this.src='/static-figs/fig_hotspot_count_heatmap_blue.png';"/>
      <div class="fig-caption">Fig 4 &mdash; Hotspot Count Heatmap</div>
      <p style="font-size:.78rem;color:var(--muted);margin-top:6px;line-height:1.45">Heatmap of hotspot counts across all 14 days (rows) and persistence settings k (columns). Darker cells indicate more detected hotspots; days with elevated PM2.5 events (e.g., Jan 17, Jan 31) stand out clearly.</p>
    </div>
  </div>
  <div class="nav-btns" style="margin-top:22px">
    <button class="btn btn-outline btn-sm" onclick="goStep(8)">&larr; Back</button>
    <span></span>
  </div>
</div>

</div><!-- /container -->

<!-- lightbox -->
<div class="lb-overlay" id="lb" onclick="closeLB()"><img id="lb-img" src="" alt=""/></div>

<footer style="text-align:center;padding:18px;font-size:.74rem;color:var(--muted)">
  Forecast Service v1.0 &middot; <a href="/docs" target="_blank">OpenAPI Docs</a> &middot; <a href="/health">Health</a>
</footer>

<script>
/* ── helpers ─────────────────────────────────── */
const $ = s => document.getElementById(s);
const BASE = location.origin;
let currentStep = 0, _jobId = null, _lastPredUrl = null, _lastHotUrl = null, _pollTimer = null, _finalJobState = {};
let _consumePollTimer = null;
let _predFilesOpen = false, _hotspotFilesOpen = false, _hotspotInputPreviewOpen = false;
const _defaultPredListHtml = '<li style="color:var(--muted)">Click load to show&hellip;</li>';
const _defaultHotListHtml = '<li style="color:var(--muted)">Click load to show&hellip;</li>';
const _csvPreviewState = {};

/* ── lightbox ────────────────────────────────── */
function openLB(img){$('lb-img').src=img.src;$('lb').classList.add('open')}
function closeLB(){$('lb').classList.remove('open')}

/* ── stepper ─────────────────────────────────── */
function goStep(n){
  currentStep = n;
  document.querySelectorAll('.card').forEach((c,i)=>c.classList.toggle('visible',i===n));
  document.querySelectorAll('.step-pill').forEach((p,i)=>p.classList.toggle('active',i===n));
}
document.querySelectorAll('.step-pill').forEach(p=>p.addEventListener('click',()=>goStep(+p.dataset.step)));
function markStepDone(n){document.querySelectorAll('.step-pill')[n].classList.add('done')}

function log(id,msg){const el=$(id);el.classList.remove('hidden');el.textContent+=msg+'\\n';el.scrollTop=el.scrollHeight}
function clearLog(id){const el=$(id);el.textContent='';el.classList.add('hidden')}

function badge(status){
  const m={'done':'badge-ok','ok':'badge-ok','failed':'badge-fail','running':'badge-run','queued':'badge-queue'};
  return '<span class="badge '+(m[status]||'badge-queue')+'">'+status+'</span>';
}

function makeTable(rows,cols){
  if(!rows||!rows.length)return '<p style="color:var(--muted);font-size:.82rem">No rows.</p>';
  if(!cols) cols=Object.keys(rows[0]);
  let h='<table><thead><tr>'+cols.map(c=>'<th>'+c+'</th>').join('')+'</tr></thead><tbody>';
  rows.forEach(r=>{h+='<tr>'+cols.map(c=>'<td>'+(r[c]!=null?r[c]:'')+'</td>').join('')+'</tr>'});
  return h+'</tbody></table>';
}

async function api(method,path,body){
  const opts={method,headers:{'Content-Type':'application/json'}};
  if(body) opts.body=JSON.stringify(body);
  const r=await fetch(BASE+path,opts);
  const j=await r.json();
  if(!r.ok) throw{status:r.status,detail:j.detail||JSON.stringify(j)};
  return j;
}

function _setToggleButton(buttonId,isOpen,openLabel,closeLabel){
  const btn=$(buttonId);
  if(btn) btn.textContent=isOpen?closeLabel:openLabel;
}

function _resetCsvPreview(targetId){
  const st=_csvPreviewState[targetId];
  if(st&&st.button) st.button.textContent='Preview';
  _csvPreviewState[targetId]={url:null,button:null,open:false};
  const el=$(targetId);
  if(el) el.innerHTML='';
}

function closePredictionFiles(){
  _predFilesOpen=false;
  _setToggleButton('btn-pred-files',false,'Load Prediction Files','Close Prediction Files');
  const listEl=$('pred-list');
  if(listEl) listEl.innerHTML=_defaultPredListHtml;
  _resetCsvPreview('pred-preview');
}

function closeHotspotFiles(){
  _hotspotFilesOpen=false;
  _setToggleButton('btn-hot-files',false,'Load Hotspot Files','Close Hotspot Files');
  const listEl=$('hot-list');
  if(listEl) listEl.innerHTML=_defaultHotListHtml;
  _resetCsvPreview('hot-preview');
}

function closeHotspotInputPreview(){
  _hotspotInputPreviewOpen=false;
  _setToggleButton('btn-h-preview',false,'Preview File and Load Fields','Close Preview and Fields');
  $('h-preview-status').textContent='';
  $('h-field-options').innerHTML='';
  $('h-pred-preview').innerHTML='';
}

/* ── Step 0: Run model ───────────────────────── */
async function submitRun(){
  const btn=$('btn-run');btn.disabled=true;clearLog('run-log');
  $('run-status').innerHTML='<span class="spinner"></span> Submitting&hellip;';
  const payload={
    region:{name:$('r-name').value,lat_min:+$('r-lat-min').value,lat_max:+$('r-lat-max').value,
            lon_min:+$('r-lon-min').value,lon_max:+$('r-lon-max').value},
    dates:{start:$('r-start').value,end:$('r-end').value},
    overwrite:$('r-overwrite').value==='true'
  };
  log('run-log','POST /run-model\\n'+JSON.stringify(payload,null,2));
  try{
    const j=await api('POST','/run-model',payload);
    _jobId=j.job_id;$('s-jobid').value=_jobId;
    $('run-status').innerHTML=badge('queued')+' Job: <code>'+_jobId+'</code>';
    log('run-log','Response: '+JSON.stringify(j,null,2));
    markStepDone(0);goStep(1);pollStatus();
  }catch(e){$('run-status').innerHTML=badge('failed')+' '+e.detail;log('run-log','ERROR: '+JSON.stringify(e))}
  btn.disabled=false;
}

/* ── Step 1: Poll status ─────────────────────── */
async function pollStatus(){
  const jid=$('s-jobid').value.trim();
  if(!jid){$('status-box').innerHTML='<p style="color:var(--danger)">Enter a Job ID</p>';return}
  if(_pollTimer)clearInterval(_pollTimer);clearLog('status-log');
  delete _finalJobState[jid];
  $('btn-poll').disabled=true;$('status-box').innerHTML='<span class="spinner"></span> Polling&hellip;';
  async function tick(){
    try{
      const j=await api('GET','/status/'+jid);const st=j.status||'unknown';
      if(_finalJobState[jid] && (st==='done' || st==='failed')) return;
      $('status-box').innerHTML=badge(st)+' step: <b>'+(j.step||'')+'</b>';
      log('status-log',new Date().toLocaleTimeString()+' | status='+st+' step='+(j.step||''));
      if(st==='done'){_finalJobState[jid]=true;clearInterval(_pollTimer);_pollTimer=null;$('btn-poll').disabled=false;markStepDone(1);
        const res=j.result||{};const urls=res.prediction_urls||[];
        if(urls.length){_lastPredUrl=urls[0];$('h-predfile').value=urls[0]}
        log('status-log','\\nResult:\\n'+JSON.stringify(res,null,2));
        return}
      if(st==='failed'){_finalJobState[jid]=true;clearInterval(_pollTimer);_pollTimer=null;$('btn-poll').disabled=false;
        log('status-log','\\nError: '+(j.error||'unknown'));return}
    }catch(e){log('status-log','poll error: '+JSON.stringify(e))}
  }
  await tick();_pollTimer=setInterval(tick,5000);
}

/* ── Step 2: Predictions ─────────────────────── */
async function togglePredictionFiles(){
  if(_predFilesOpen){closePredictionFiles();return}
  await loadPredictions();
}

async function loadPredictions(){
  const listEl=$('pred-list');
  _setToggleButton('btn-pred-files',true,'Load Prediction Files','Close Prediction Files');
  listEl.innerHTML='<li><span class="spinner"></span> Loading&hellip;</li>';
  try{
    const j=await api('GET','/predictions');const files=j.files||[];
    const html=files.map(f=>
      '<li><a href="'+f.url+'" target="_blank">'+f.name+'</a> '+
      '<button class="btn btn-outline btn-sm" style="margin-left:6px" onclick="usePrediction(\\''+f.url+'\\')">Use for hotspot</button> '+
      '<button class="btn btn-outline btn-sm" onclick="previewCSV(this,\\''+f.url+'\\',\\'pred-preview\\')">Preview</button></li>'
    ).join('');
    listEl.innerHTML=html||'<li>No prediction files found.</li>';
    _predFilesOpen=true;
    markStepDone(2);
  }catch(e){
    _predFilesOpen=false;
    _setToggleButton('btn-pred-files',false,'Load Prediction Files','Close Prediction Files');
    listEl.innerHTML='<li style="color:var(--danger)">Error: '+e.detail+'</li>';
  }
}
function usePrediction(url){_lastPredUrl=url;$('h-predfile').value=url;goStep(3)}

async function previewCSV(btn,url,targetId){
  const st=_csvPreviewState[targetId]||{url:null,button:null,open:false};
  const el=$(targetId);
  _csvPreviewState[targetId]=st;
  if(st.open&&st.url===url){
    _resetCsvPreview(targetId);
    return;
  }
  if(st.button&&st.button!==btn) st.button.textContent='Preview';
  if(btn) btn.textContent='Loading...';
  el.innerHTML='<span class="spinner"></span>';
  try{
    const resp=await fetch(url);const txt=await resp.text();
    const lines=txt.trim().split('\\n');
    if(lines.length<2){
      el.innerHTML='<p>Empty or header-only CSV.</p>';
      st.open=true;
      st.url=url;
      st.button=btn||null;
      if(btn) btn.textContent='Close Preview';
      return;
    }
    const hdr=lines[0].split(',');
    const rows=lines.slice(1,21).map(l=>{const c=l.split(',');const o={};hdr.forEach((h,i)=>o[h]=c[i]||'');return o});
    el.innerHTML=makeTable(rows,hdr)+(lines.length>21?'<p style="color:var(--muted);font-size:.76rem">Showing 20 of '+(lines.length-1)+' rows</p>':'');
    st.open=true;
    st.url=url;
    st.button=btn||null;
    if(btn) btn.textContent='Close Preview';
  }catch(e){
    _csvPreviewState[targetId]={url:null,button:null,open:false};
    if(btn) btn.textContent='Preview';
    el.innerHTML='<p style="color:var(--danger)">'+e+'</p>';
  }
}

/* ── Step 3: Detect Hotspots ─────────────────── */
async function detectHotspots(){
  const btn=$('btn-detect');btn.disabled=true;clearLog('detect-log');
  $('detect-status').innerHTML='<span class="spinner"></span> Detecting&hellip;';
  const topk=+$('h-topk').value;
  const selectedFieldThresholds=getSelectedFieldThresholds();
  if(!selectedFieldThresholds.length){
    $('detect-status').innerHTML=badge('failed')+' select at least one numeric field';
    log('detect-log','ERROR: no threshold fields selected. Use "Preview File and Load Fields" first.');
    btn.disabled=false;
    return;
  }
  const payload={prediction_file:$('h-predfile').value,threshold:+$('h-thresh').value,
    persistence_k:+$('h-pk').value,timestep_hours:+$('h-ts').value,field_thresholds:selectedFieldThresholds};
  if(topk>0) payload.top_k_hotspots=topk;
  log('detect-log','POST /detect-hotspot\\n'+JSON.stringify(payload,null,2));
  try{
    const j=await api('POST','/detect-hotspot',payload);
    $('detect-status').innerHTML=badge('ok')+' '+j.hotspot_count+' hotspots detected';
    log('detect-log','Response (meta):\\n'+JSON.stringify({hotspots_csv:j.hotspots_csv,hotspot_count:j.hotspot_count,ndp:j.ndp},null,2));
    if(j.preview&&j.preview.length)$('hotspot-preview').innerHTML=makeTable(j.preview);
    const ndp=j.ndp||{};const reg=(ndp.registered||[])[0]||{};
    _lastHotUrl=reg.url||j.hotspots_csv;$('ss-file').value=_lastHotUrl;markStepDone(3);
  }catch(e){$('detect-status').innerHTML=badge('failed')+' '+e.detail;log('detect-log','ERROR: '+JSON.stringify(e))}
  btn.disabled=false;
}

function _fieldId(field){
  return (field||'').replace(/[^a-zA-Z0-9_-]/g,'_');
}

function getSelectedFieldThresholds(){
  const opts=$('h-field-options');
  if(!opts) return [];
  const selected=[];
  opts.querySelectorAll('input[type="checkbox"][data-field]').forEach(cb=>{
    if(!cb.checked) return;
    const field=cb.getAttribute('data-field');
    const inp=$('h-thr-'+_fieldId(field));
    const thr=inp?+inp.value:NaN;
    if(field && Number.isFinite(thr)) selected.push({field,threshold:thr});
  });
  return selected;
}

function renderHotspotFieldOptions(fields, defaultThreshold){
  const el=$('h-field-options');
  if(!el) return;
  if(!fields || !fields.length){
    el.innerHTML='<p style="color:var(--muted);font-size:.8rem">No numeric threshold fields detected.</p>';
    return;
  }
  const rows=fields.map((f,idx)=>{
    const id=_fieldId(f);
    const checked=(f==='pm2p5_ugm3'||idx===0)?'checked':'';
    return '<div style="display:grid;grid-template-columns:24px 1fr 140px;gap:8px;align-items:center;margin:5px 0">'+
      '<input type="checkbox" data-field="'+f+'" '+checked+'/>'+
      '<code>'+f+'</code>'+
      '<input id="h-thr-'+id+'" type="number" step="0.1" value="'+defaultThreshold+'"/>'+
      '</div>';
  }).join('');
  el.innerHTML='<div style="font-size:.78rem;color:var(--muted);margin-bottom:4px">Select one or more fields and set threshold per field:</div>'+rows;
}

async function previewHotspotInput(){
  const pred=$('h-predfile').value.trim();
  if(!pred){$('h-preview-status').textContent='Enter prediction file first';return}
  $('h-preview-status').innerHTML='<span class="spinner"></span> Loading preview...';
  try{
    const q='?prediction_file='+encodeURIComponent(pred);
    const j=await api('GET','/prediction-preview'+q);
    $('h-preview-status').textContent='Loaded: '+(j.row_count||0)+' rows previewed';
    renderHotspotFieldOptions(j.numeric_columns||[], +$('h-thresh').value);
    const previewRows=j.preview||[];
    if(previewRows.length){
      $('h-pred-preview').innerHTML=makeTable(previewRows);
    }else{
      $('h-pred-preview').innerHTML='<p style="color:var(--muted)">No preview rows.</p>';
    }
    _hotspotInputPreviewOpen=true;
    _setToggleButton('btn-h-preview',true,'Preview File and Load Fields','Close Preview and Fields');
  }catch(e){
    _hotspotInputPreviewOpen=false;
    _setToggleButton('btn-h-preview',false,'Preview File and Load Fields','Close Preview and Fields');
    $('h-preview-status').textContent='Preview failed';
    $('h-pred-preview').innerHTML='<p style="color:var(--danger)">'+(e.detail||e)+'</p>';
  }
}

async function toggleHotspotInputPreview(){
  if(_hotspotInputPreviewOpen){closeHotspotInputPreview();return}
  await previewHotspotInput();
}

/* ── Step 4: Hotspot Files ───────────────────── */
async function toggleHotspotFiles(){
  if(_hotspotFilesOpen){closeHotspotFiles();return}
  await loadHotspotFiles();
}

async function loadHotspotFiles(){
  const el=$('hot-list');
  _setToggleButton('btn-hot-files',true,'Load Hotspot Files','Close Hotspot Files');
  el.innerHTML='<li><span class="spinner"></span> Loading&hellip;</li>';
  try{
    const j=await api('GET','/hotspots');const files=j.files||[];
    el.innerHTML=files.map(f=>
      '<li><a href="'+f.url+'" target="_blank">'+f.name+'</a> '+
      '<button class="btn btn-outline btn-sm" style="margin-left:6px" onclick="useHotspot(\\''+f.url+'\\')">Use for sensor selection</button> '+
      '<button class="btn btn-outline btn-sm" onclick="previewCSV(this,\\''+f.url+'\\',\\'hot-preview\\')">Preview</button> '+
      '<a class="btn btn-outline btn-sm" href="'+f.download_url+'" download>Download</a></li>'
    ).join('')||'<li>No hotspot files found.</li>';
    _hotspotFilesOpen=true;
    markStepDone(4);
  }catch(e){
    _hotspotFilesOpen=false;
    _setToggleButton('btn-hot-files',false,'Load Hotspot Files','Close Hotspot Files');
    el.innerHTML='<li style="color:var(--danger)">Error: '+e.detail+'</li>';
  }
}
function useHotspot(url){_lastHotUrl=url;$('ss-file').value=url;goStep(5)}

/* ── Step 5: Select Sensors ──────────────────── */
async function selectSensors(){
  const btn=$('btn-sensor');btn.disabled=true;clearLog('sensor-log');
  $('sensor-status').innerHTML='<span class="spinner"></span> Selecting&hellip;';
  const payload={hotspots_file:$('ss-file').value,selection_mode:$('ss-mode').value,
    k:+$('ss-k').value,radius_km:+$('ss-radius').value};
  log('sensor-log','POST /select-sensor\\n'+JSON.stringify(payload,null,2));
  try{
    const j=await api('POST','/select-sensor',payload);
    $('sensor-status').innerHTML=badge('ok')+' '+j.sensors_found+' sensors, '+j.mapping_count+' mappings';
    log('sensor-log','Response (meta):\\n'+JSON.stringify({sensors_found:j.sensors_found,mapping_count:j.mapping_count,map_csv:j.map_csv},null,2));
    if(j.preview&&j.preview.length)$('sensor-preview').innerHTML=makeTable(j.preview);
    markStepDone(5);
  }catch(e){$('sensor-status').innerHTML=badge('failed')+' '+e.detail;log('sensor-log','ERROR: '+JSON.stringify(e))}
  btn.disabled=false;
}

/* ── Step 6: Sensors ─────────────────────────── */
let _sensorItems = [];
async function loadSensorList(){
  const btn=$('btn-sensors');btn.disabled=true;
  $('sensors-status').innerHTML='<span class="spinner"></span> Loading&hellip;';
  const server=$('sen-server').value.trim();
  const qs=server?'?server='+encodeURIComponent(server):'';
  try{
    const j=await api('GET','/sensors'+qs);
    _sensorItems=j.items||[];
    $('sensors-status').innerHTML=badge('ok')+' '+j.count+' sensors';
    if(_sensorItems.length){
      $('sensors-table').innerHTML=makeTable(_sensorItems.map(s=>({
        name:s.dataset_name||'',station_id:s.station_id||'',station_name:s.station_name||'',
        region:s.region||'',resources:s.resources_count,
        action:'<button class=\\"btn btn-outline btn-sm\\" onclick=\\"useSensorForCM(\\''+
               (s.dataset_name||'')+'\\')\\">Lookup CM</button>'
      })),['name','station_id','station_name','region','resources','action']);
    }else{$('sensors-table').innerHTML='<p style="color:var(--muted)">No sensors found.</p>'}
    markStepDone(6);
  }catch(e){$('sensors-status').innerHTML=badge('failed')+' '+(e.detail||e);$('sensors-table').innerHTML=''}
  btn.disabled=false;
}

function useSensorForCM(name){$('cm-sensor-name').value=name;lookupConsumptionMethod()}

async function lookupConsumptionMethod(){
  const name=$('cm-sensor-name').value.trim();
  if(!name){$('cm-status').innerHTML='<span style="color:var(--danger)">Enter a sensor name</span>';return}
  const btn=$('btn-cm');btn.disabled=true;clearLog('cm-log');
  $('cm-status').innerHTML='<span class="spinner"></span> Looking up&hellip;';
  const server=$('sen-server').value.trim();
  const qs=server?'?server='+encodeURIComponent(server):'';
  try{
    const j=await api('GET','/sensors/'+encodeURIComponent(name)+'/consumption-method'+qs);
    $('cm-status').innerHTML=badge('ok')+' '+j.count+' methods';
    log('cm-log','GET /sensors/'+name+'/consumption-method\\n'+JSON.stringify(j,null,2));
    const methods=j.consumption_methods||[];
    if(methods.length){
      $('cm-table').innerHTML=makeTable(methods.map(m=>({
        id:m.id,name:m.name||'',type:m.type||'',
        action:'<button class=\\"btn btn-outline btn-sm\\" onclick=\\"useMethodForStream(\\''+
               (m.id||'')+'\\')\\">Use for Stream</button>'
      })),['id','name','type','action']);
    }else{$('cm-table').innerHTML='<p style="color:var(--muted)">No consumption methods found.</p>'}
  }catch(e){$('cm-status').innerHTML=badge('failed')+' '+(e.detail||e);log('cm-log','ERROR: '+JSON.stringify(e))}
  btn.disabled=false;
}

function useMethodForStream(methodId){
  const existing=$('cs-method-ids').value.trim();
  $('cs-method-ids').value=existing?(existing+','+methodId):methodId;
  goStep(7);
}

function useTopicForConsume(topic){
  $('co-topic').value=topic;
  goStep(8);
}

/* ── Step 7: Create Stream ───────────────────── */
let _lastTopic=null;
async function createStream(){
  const btn=$('btn-create-stream');btn.disabled=true;clearLog('stream-create-log');
  $('stream-create-status').innerHTML='<span class="spinner"></span> Creating&hellip;';
  const ids=$('cs-method-ids').value.split(',').map(s=>s.trim()).filter(Boolean);
  const filters=$('cs-filters').value.split(',').map(s=>s.trim()).filter(Boolean);
  const server=$('cs-server').value.trim();
  if(!ids.length){$('stream-create-status').innerHTML='<span style="color:var(--danger)">Enter at least one consumption method ID</span>';btn.disabled=false;return}
  const params=new URLSearchParams();
  ids.forEach(id=>params.append('consumption_method_ids',id));
  filters.forEach(f=>params.append('filter_semantics',f));
  if(server)params.append('server',server);
  log('stream-create-log','POST /streams/create?'+params.toString());
  try{
    const j=await api('POST','/streams/create?'+params.toString());
    _lastTopic=j.data_stream_id;
    $('co-topic').value=_lastTopic;
    $('stream-create-status').innerHTML=badge('ok')+' topic: <code>'+_lastTopic+'</code> '+
      '<button class=\\"btn btn-outline btn-sm\\" style=\\"margin-left:8px\\" onclick=\\"useTopicForConsume(\\''+_lastTopic+'\\')\\">Use in Consume Stream &rarr;</button>';
    log('stream-create-log','Response:\\n'+JSON.stringify(j,null,2));
    if(j.warnings&&j.warnings.length)log('stream-create-log','Warnings: '+j.warnings.join(', '));
    markStepDone(7);
  }catch(e){$('stream-create-status').innerHTML=badge('failed')+' '+(e.detail||e);log('stream-create-log','ERROR: '+JSON.stringify(e))}
  btn.disabled=false;
}

/* ── Step 8: Consume Stream ──────────────────── */
function toggleConsumeMode(){
  const mode=$('co-mode').value;
  $('co-auto-wrap').classList.toggle('hidden',mode!=='timed');
}

function stopConsumePolling(){
  if(_consumePollTimer){clearInterval(_consumePollTimer);_consumePollTimer=null}
}

function renderConsumeState(j){
  const st=j.status||'unknown';
  const running=(st==='running'||st==='started'||st==='already_running');
  $('consume-status').innerHTML=badge(st==='failed'?'failed':(running||st==='stopped'?'ok':'queue'))+' '+st;
  $('btn-consume').disabled=running;
  $('btn-consume-stop').disabled=!running;
  $('consume-meta').innerHTML=
    'mode=<b>'+(j.mode||'-')+'</b> &middot; '+
    'messages=<b>'+(j.message_count||0)+'</b> &middot; '+
    'elapsed=<b>'+((j.elapsed_seconds!=null)?(j.elapsed_seconds+'s'):'-')+'</b> &middot; '+
    'planned_stop=<b>'+(j.planned_stop_at||'-')+'</b> &middot; '+
    'reason=<b>'+(j.stop_reason||'-')+'</b>';
  const msgs=Array.isArray(j.recent_messages)?j.recent_messages:[];
  $('consume-live').value=msgs.slice().reverse().map(m=>JSON.stringify(m,null,2)).join('\\n\\n');
}

async function consumeStatus(topic){
  const j=await api('POST','/streams/'+encodeURIComponent(topic)+'/consume/start',{action:'status'});
  renderConsumeState(j);
  log('consume-log','Status:\\n'+JSON.stringify(j,null,2));
  if(j.status==='stopped'||j.status==='failed'||j.status==='not_started'){
    stopConsumePolling();
  }
}

function startConsumePolling(topic){
  stopConsumePolling();
  _consumePollTimer=setInterval(()=>consumeStatus(topic).catch(()=>stopConsumePolling()),2000);
}

async function startConsume(){
  const btn=$('btn-consume');btn.disabled=true;clearLog('consume-log');
  $('consume-status').innerHTML='<span class="spinner"></span> Starting&hellip;';
  const topic=$('co-topic').value.trim();
  if(!topic){$('consume-status').innerHTML='<span style="color:var(--danger)">Enter a topic</span>';btn.disabled=false;return}
  const mode=$('co-mode').value;
  const payload={action:'start',mode,poll_interval:+$('co-poll').value,max_messages:+$('co-max').value};
  if(mode==='timed') payload.auto_close_minutes=+$('co-auto-minutes').value;
  log('consume-log','POST /streams/'+topic+'/consume/start\\n'+JSON.stringify(payload,null,2));
  try{
    const j=await api('POST','/streams/'+encodeURIComponent(topic)+'/consume/start',payload);
    renderConsumeState(j);
    log('consume-log','Start Response:\\n'+JSON.stringify(j,null,2));
    startConsumePolling(topic);
    await consumeStatus(topic);
    markStepDone(8);
  }catch(e){$('consume-status').innerHTML=badge('failed')+' '+(e.detail||e);log('consume-log','ERROR: '+JSON.stringify(e))}
  if(!$('btn-consume-stop').disabled){btn.disabled=true}else{btn.disabled=false}
}

async function stopConsume(){
  const topic=$('co-topic').value.trim();
  if(!topic){$('consume-status').innerHTML='<span style="color:var(--danger)">Enter a topic</span>';return}
  $('consume-status').innerHTML='<span class="spinner"></span> Stopping&hellip;';
  $('btn-consume-stop').disabled=true;
  try{
    const j=await api('POST','/streams/'+encodeURIComponent(topic)+'/consume/start',{action:'stop'});
    renderConsumeState(j);
    log('consume-log','Stop Response:\\n'+JSON.stringify(j,null,2));
    await consumeStatus(topic);
  }catch(e){$('consume-status').innerHTML=badge('failed')+' '+(e.detail||e);log('consume-log','ERROR: '+JSON.stringify(e))}
}

async function checkConsumeStatus(){
  const topic=$('co-topic').value.trim();
  if(!topic){$('consume-status').innerHTML='<span style="color:var(--danger)">Enter a topic</span>';return}
  $('consume-status').innerHTML='<span class="spinner"></span> Checking&hellip;';
  try{
    await consumeStatus(topic);
  }catch(e){$('consume-status').innerHTML=badge('failed')+' '+(e.detail||e);log('consume-log','ERROR: '+JSON.stringify(e))}
}
toggleConsumeMode();
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root():
    return HTMLResponse(_DASHBOARD_HTML)


JOBS: Dict[str, Dict[str, Any]] = {}

# PROJECT_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PREDICTIONS_DIR = (PROJECT_ROOT / "data/processed/predictions").resolve()
PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
HOTSPOTS_DIR = (PROJECT_ROOT / "data/processed/hotspots").resolve()
HOTSPOTS_DIR.mkdir(parents=True, exist_ok=True)
MAP_DIR = (PROJECT_ROOT / "data/processed/map").resolve()
MAP_DIR.mkdir(parents=True, exist_ok=True)
FIGS_DIR = (PROJECT_ROOT / "paper/figs").resolve()
ALT_FIGS_DIR = (PROJECT_ROOT / "paper/figures").resolve()
if (not FIGS_DIR.exists() or not any(FIGS_DIR.iterdir())) and ALT_FIGS_DIR.exists() and any(ALT_FIGS_DIR.iterdir()):
    FIGS_DIR = ALT_FIGS_DIR
FIGS_DIR.mkdir(parents=True, exist_ok=True)
ASSETS_DIR = (PROJECT_ROOT / "assets").resolve()
ASSETS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/static-figs", StaticFiles(directory=str(FIGS_DIR)), name="static-figs")
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")
FILE_KIND_DIRS: Dict[str, Path] = {
    "prediction": PREDICTIONS_DIR,
    "hotspot": HOTSPOTS_DIR,
}


def _build_internal_file_ref(kind: str, filename: str) -> str:
    safe_kind = (kind or "").strip().lower()
    safe_name = Path(filename).name
    if safe_kind not in FILE_KIND_DIRS:
        raise ValueError(f"unsupported file kind: {kind}")
    if not safe_name or safe_name != filename:
        raise ValueError("invalid filename")
    return f"{safe_kind}:{safe_name}"


def _build_public_file_view_url(kind: str, filename: str) -> str:
    base_url = os.getenv("SERVICE_BASE_URL", "http://localhost:8000").rstrip("/")
    ref = _build_internal_file_ref(kind, filename)
    return f"{base_url}/files?url={quote(ref, safe='')}"


def _build_public_file_download_url(kind: str, filename: str) -> str:
    base_url = os.getenv("SERVICE_BASE_URL", "http://localhost:8000").rstrip("/")
    ref = _build_internal_file_ref(kind, filename)
    return f"{base_url}/download?url={quote(ref, safe='')}"


def _resolve_hosted_file_url(url_param: str) -> tuple[str, str, Path]:
    if not url_param:
        raise HTTPException(status_code=400, detail="missing url query parameter")

    raw = unquote(url_param).strip()
    # Canonical hosted reference format generated by this service: "<kind>:<filename>".
    if raw.count(":") == 1 and "://" not in raw and "?" not in raw and "/" not in raw:
        ref = raw
    else:
        parsed = urlparse(raw)
        if parsed.path.rstrip("/") in ("/files", "/download"):
            nested = parse_qs(parsed.query).get("url", [None])[0]
            if not nested:
                raise HTTPException(status_code=400, detail="hosted file URL missing nested 'url' query parameter")
            ref = unquote(nested)
        elif parsed.scheme or parsed.netloc:
            nested = parse_qs(parsed.query).get("url", [None])[0]
            if not nested:
                raise HTTPException(status_code=400, detail="hosted file URL missing nested 'url' query parameter")
            ref = unquote(nested)
        else:
            ref = raw

    if ":" not in ref:
        raise HTTPException(status_code=400, detail="invalid hosted file reference format")

    kind, filename = ref.split(":", 1)
    kind = kind.strip().lower()
    filename = filename.strip()
    if kind not in FILE_KIND_DIRS:
        raise HTTPException(status_code=400, detail=f"unsupported file kind '{kind}'")
    if not filename or Path(filename).name != filename:
        raise HTTPException(status_code=400, detail="invalid filename")

    base_dir = FILE_KIND_DIRS[kind]
    target = (base_dir / filename).resolve()
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="file not found")
    return kind, filename, target

# Serve a simple HTML index at /prediction-files/ and individual files
@app.get("/prediction-files/", response_class=HTMLResponse, include_in_schema=False)
def prediction_files_index():
    files = sorted([p.name for p in PREDICTIONS_DIR.glob("*.csv") if p.is_file()])
    html = [
        "<html><head><title>Prediction files</title></head><body>",
        "<h1>Prediction files</h1>",
        "<ul>",
    ]
    for name in files:
        ref = _build_internal_file_ref("prediction", name)
        view_url = f"/files?url={quote(ref, safe='')}"
        html.append(f'<li><a href="{view_url}">{name}</a></li>')
    html.extend(["</ul>", "</body></html>"])
    return HTMLResponse("\n".join(html))


# Serve a simple HTML index at /hotspot-files/ and individual files
@app.get("/hotspot-files/", response_class=HTMLResponse, include_in_schema=False)
def hotspot_files_index():
    files = sorted([p.name for p in HOTSPOTS_DIR.glob("*.csv") if p.is_file()])
    html = [
        "<html><head><title>Hotspot files</title></head><body>",
        "<h1>Hotspot files</h1>",
        "<ul>",
    ]
    for name in files:
        ref = _build_internal_file_ref("hotspot", name)
        view_url = f"/files?url={quote(ref, safe='')}"
        html.append(f'<li><a href="{view_url}">{name}</a></li>')
    html.extend(["</ul>", "</body></html>"])
    return HTMLResponse("\n".join(html))


@app.get("/files")
def get_hosted_file(url: str = Query(..., description="Hosted file URL reference.")):
    _, filename, target = _resolve_hosted_file_url(url)
    return FileResponse(
        path=str(target),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


@app.get("/download")
def download_hosted_file(url: str = Query(..., description="Hosted file URL reference.")):
    _, filename, target = _resolve_hosted_file_url(url)
    return FileResponse(path=str(target), media_type="text/csv", filename=filename)


class RegionCfg(BaseModel):
    name: str = "utah"
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


class DatesCfg(BaseModel):
    start: str  # "YYYY-MM-DD"
    end: str    # "YYYY-MM-DD"


class AuroraCfg(BaseModel):
    times: List[str] = Field(default_factory=lambda: ["00:00", "12:00"])
    timestamp: str = "0000-1200"
    lead_hours: int = 12
    region_tag: str = "global"


class PathsCfg(BaseModel):
    cams_download: str = "data/raw/cams_raw"
    static_pickle: str = "aurora-0.4-air-pollution-static.pickle"


class RunRequest(BaseModel):
    region: RegionCfg
    dates: DatesCfg
    paths: PathsCfg = PathsCfg()
    overwrite: bool = False
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "region": {"name": "utah", "lat_min": 37.0, "lat_max": 42.1, "lon_min": 245.9, "lon_max": 251.0},
                "dates": {"start": "2025-01-16", "end": "2025-01-16"},
                "overwrite": False,
            }
        }
    )


class DetectHotspotRequest(BaseModel):
    prediction_file: str = Field(
        ...,
        description=(
            "Prediction CSV input. Accepts hosted URL (/files?url=...), canonical reference "
            "(prediction:<filename>), file name under data/processed/predictions, or absolute path."
        ),
    )
    threshold: float = 10.0
    persistence_k: int = 12
    timestep_hours: int = 1
    threshold_field: str = Field(
        default="pm2p5_ugm3",
        description="Prediction CSV column used for threshold comparison.",
    )
    field_thresholds: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Optional multi-field thresholds: [{'field':'pm2p5_ugm3','threshold':10.0}, ...].",
    )
    top_k_hotspots: Optional[int] = Field(
        default=None,
        description="Optional cap on number of hotspot rows returned.",
    )


class SelectSensorRequest(BaseModel):
    hotspots_file: str = Field(
        ...,
        description=(
            "Hotspot CSV input. Accepts hosted URL (/files?url=...), canonical reference "
            "(hotspot:<filename>), file name under data/processed/hotspots, or absolute path."
        ),
    )
    selection_mode: str = Field(default="nearest_k", description="One of: nearest_k, radius_km")
    k: int = Field(default=1, ge=1, description="Number of nearest sensors per hotspot for nearest_k mode.")
    radius_km: float = Field(
        default=10.0,
        gt=0.0,
        description="Radius in km for radius_km mode.",
    )


class CreateStreamRequest(BaseModel):
    consumption_method_ids: List[str] = Field(
        ...,
        description="List of consumption method resource IDs used to create the stream.",
        json_schema_extra={"example": ["8332f19d-8bf6-43ce-af88-ff2bd9a9bc07"]},
    )
    filter_semantics: List[str] = Field(default_factory=list, json_schema_extra={"example": []})

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "consumption_method_ids": ["8332f19d-8bf6-43ce-af88-ff2bd9a9bc07"],
                "filter_semantics": [],
            }
        }
    )


class StartConsumeRequest(BaseModel):
    action: Literal["start", "status", "stop"] = "start"
    mode: Literal["timed", "manual"] = "timed"
    auto_close_minutes: int = Field(default=60, ge=1, le=1440)
    minutes: Optional[int] = Field(default=None, ge=1, le=1440)
    poll_interval: float = Field(default=2.0, gt=0.0)
    max_messages: int = Field(default=5000, ge=1)


STREAM_CONSUMERS: Dict[str, Dict[str, Any]] = {}


def _iso_utc(dt: Optional[datetime] = None) -> str:
    value = dt or datetime.now(timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _resolve_server(server_param: Optional[str]) -> str:
    if server_param and str(server_param).strip():
        return str(server_param).strip()
    env_server = (os.getenv("SERVER") or "").strip()
    return env_server or "local"


def _parse_terms(terms: Optional[List[str]]) -> List[str]:
    parsed: List[str] = []
    for entry in (terms or []):
        if entry is None:
            continue
        for token in str(entry).split(","):
            token = token.strip()
            if token:
                parsed.append(token)
    return parsed or ["sensor"]


def _create_clients(server: Optional[str] = None) -> tuple[str, APIClient, StreamingClient]:
    load_dotenv(override=False)
    token = os.getenv("TOKEN")
    api_url = os.getenv("API_URL")
    if not token or not api_url:
        raise HTTPException(status_code=400, detail="Missing TOKEN or API_URL in environment.")

    resolved_server = _resolve_server(server)
    client = APIClient(base_url=api_url, token=token)
    streaming = StreamingClient(client)
    return resolved_server, client, streaming


def _extract_station_fields(ds: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    extras = ds.get("extras")
    extras = extras if isinstance(extras, dict) else {}
    station_id = extras.get("station_id")
    station_name = extras.get("station_name")

    if station_id and station_name:
        return station_id, station_name

    resources = ds.get("resources")
    resources = resources if isinstance(resources, list) else []
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        cfg = resource.get("config")
        cfg = cfg if isinstance(cfg, dict) else {}
        if not station_id and cfg.get("station_id"):
            station_id = cfg.get("station_id")
        if not station_name and cfg.get("station_name"):
            station_name = cfg.get("station_name")
        if station_id and station_name:
            break
    return station_id, station_name


def _dataset_to_sensor_item(ds: Dict[str, Any]) -> Dict[str, Any]:
    extras = ds.get("extras")
    extras = extras if isinstance(extras, dict) else {}
    resources = ds.get("resources")
    resources = resources if isinstance(resources, list) else []
    station_id, station_name = _extract_station_fields(ds)

    return {
        "dataset_name": ds.get("name"),
        "owner_org": ds.get("owner_org"),
        "station_id": station_id,
        "station_name": station_name,
        "region": extras.get("region"),
        "resources_count": len(resources),
    }


def _methods_from_dataset(ds: Dict[str, Any]) -> List[Dict[str, Any]]:
    resources = ds.get("resources")
    resources = resources if isinstance(resources, list) else []

    methods: List[Dict[str, Any]] = []
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        methods.append(
            {
                "id": resource.get("id"),
                "name": resource.get("name"),
                "type": resource.get("type") or resource.get("format"),
            }
        )
    return methods


def _count_new_messages(consumer: Any, last_rows: int) -> tuple[int, int]:
    df = getattr(consumer, "dataframe", None)
    if df is None:
        return 0, last_rows
    try:
        current_rows = int(len(df))
    except Exception:
        return 0, last_rows

    if current_rows <= last_rows:
        return 0, last_rows
    return current_rows - last_rows, current_rows


def _normalize_payload_cell(cell: Any) -> List[Dict[str, Any]]:
    if isinstance(cell, dict):
        return [cell]
    if isinstance(cell, list):
        return [item for item in cell if isinstance(item, dict)]
    return []


def _consume_state_snapshot(state: Dict[str, Any]) -> Dict[str, Any]:
    started_at = state.get("started_at")
    ended_at = state.get("ended_at")
    elapsed_seconds: Optional[int] = None
    try:
        if started_at:
            start_dt = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
            end_dt = datetime.now(timezone.utc)
            if ended_at:
                end_dt = datetime.fromisoformat(str(ended_at).replace("Z", "+00:00"))
            elapsed_seconds = max(0, int((end_dt - start_dt).total_seconds()))
    except Exception:
        elapsed_seconds = None

    return {
        "topic": state.get("topic"),
        "status": state.get("status"),
        "mode": state.get("mode"),
        "started_at": state.get("started_at"),
        "planned_stop_at": state.get("planned_stop_at"),
        "ended_at": state.get("ended_at"),
        "poll_interval": state.get("poll_interval"),
        "max_messages": state.get("max_messages"),
        "message_count": state.get("message_count", 0),
        "stop_requested": bool(state.get("stop_requested")),
        "stop_reason": state.get("stop_reason"),
        "elapsed_seconds": elapsed_seconds,
        "error": state.get("error"),
        "recent_messages": state.get("recent_messages", []),
    }


async def _consume_topic_worker(
    topic: str,
    *,
    server: str,
    mode: str,
    auto_close_minutes: int,
    poll_interval: float,
    max_messages: int,
) -> None:
    state = STREAM_CONSUMERS.get(topic)
    if not state:
        return

    consumer = None
    message_count = int(state.get("message_count", 0))
    seen_rows = 0
    started_at = datetime.now(timezone.utc)
    deadline = started_at + timedelta(minutes=auto_close_minutes) if mode == "timed" else None
    recent_limit = 50

    try:
        _, _, streaming = _create_clients(server=server)
        consumer = streaming.consume_kafka_messages(topic)

        while True:
            if state.get("stop_requested"):
                state["status"] = "stopped"
                state["stop_reason"] = "manual_stop"
                break
            if deadline is not None and datetime.now(timezone.utc) >= deadline:
                state["status"] = "stopped"
                state["stop_reason"] = "auto_close_timeout"
                break
            if message_count >= max_messages:
                state["status"] = "stopped"
                state["stop_reason"] = "max_messages_reached"
                break

            df = getattr(consumer, "dataframe", None)
            if df is not None:
                try:
                    current_rows = int(len(df))
                except Exception:
                    current_rows = seen_rows
                if current_rows > seen_rows:
                    new_rows = df.iloc[seen_rows:current_rows]
                    added = 0
                    if hasattr(new_rows, "columns") and "payload" in new_rows.columns:
                        for cell in new_rows["payload"].dropna():
                            for msg in _normalize_payload_cell(cell):
                                key = json.dumps(msg, sort_keys=True, default=str)
                                seen_keys = state.setdefault("seen_keys", set())
                                if key in seen_keys:
                                    continue
                                seen_keys.add(key)
                                state.setdefault("recent_messages", []).append(msg)
                                if len(state["recent_messages"]) > recent_limit:
                                    state["recent_messages"] = state["recent_messages"][-recent_limit:]
                                added += 1
                    message_count += added if added > 0 else (current_rows - seen_rows)
                    state["message_count"] = message_count
                    seen_rows = current_rows

            await asyncio.sleep(poll_interval)

        if state.get("status") == "running":
            state["status"] = "stopped"
            state["stop_reason"] = state.get("stop_reason") or "completed"
    except Exception as ex:
        state["status"] = "failed"
        state["stop_reason"] = "error"
        state["error"] = str(ex)
    finally:
        if consumer is not None:
            try:
                consumer.stop()
            except Exception:
                pass
        state["ended_at"] = _iso_utc()
        state["message_count"] = message_count


def build_date_list(start: str, end: str) -> List[str]:
    s = pd.to_datetime(start)
    e = pd.to_datetime(end)
    if e < s:
        raise ValueError("dates.end must be >= dates.start")
    return [d.strftime("%Y-%m-%d") for d in pd.date_range(s, e, freq="D")]


def enforce_date_policy(start: str, end: str) -> None:
    try:
        dates = build_date_list(start, end)
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex

    if len(dates) > MAX_DAYS_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Only {MAX_DAYS_PER_REQUEST} day per request is allowed; got {start}..{end}",
        )


def _request_key_payload(req: RunRequest) -> Dict[str, Any]:
    return {
        "dates": {"start": str(req.dates.start), "end": str(req.dates.end)},
        "region": {
            "name": str(req.region.name),
            "lat_min": float(req.region.lat_min),
            "lat_max": float(req.region.lat_max),
            "lon_min": float(req.region.lon_min),
            "lon_max": float(req.region.lon_max),
        },
        "overwrite": bool(req.overwrite),
    }


def _build_request_key(req: RunRequest) -> str:
    payload = _request_key_payload(req)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return f"{REQUEST_KEY_PREFIX}:{digest}"


def _require_redis() -> None:
    try:
        REDIS.ping()
    except RedisError as ex:
        raise HTTPException(status_code=503, detail=f"Queue backend unavailable: {ex}") from ex


def current_queue_length() -> int:
    try:
        return int(AURORA_QUEUE.count)
    except RedisError as ex:
        raise HTTPException(status_code=503, detail=f"Queue backend unavailable: {ex}") from ex


def hourly_enqueue_count() -> int:
    now = datetime.now(timezone.utc).timestamp()
    cutoff = now - ENQUEUE_WINDOW_SECONDS
    try:
        REDIS.zremrangebyscore(ENQUEUE_EVENTS_KEY, "-inf", cutoff)
        return int(REDIS.zcard(ENQUEUE_EVENTS_KEY))
    except RedisError as ex:
        raise HTTPException(status_code=503, detail=f"Queue backend unavailable: {ex}") from ex


def can_accept_new_job() -> None:
    if current_queue_length() >= MAX_QUEUE_LENGTH:
        raise HTTPException(status_code=429, detail="Prediction queue is full. Please try again later.")
    if hourly_enqueue_count() >= MAX_ENQUEUES_PER_HOUR:
        raise HTTPException(
            status_code=429,
            detail="Prediction request budget for this hour is exhausted. Please try again later.",
        )


def _record_enqueue_event(job_id: str) -> None:
    now = datetime.now(timezone.utc).timestamp()
    member = f"{now:.6f}:{job_id}"
    try:
        REDIS.zadd(ENQUEUE_EVENTS_KEY, {member: now})
        REDIS.expire(ENQUEUE_EVENTS_KEY, ENQUEUE_WINDOW_SECONDS * 2)
    except RedisError as ex:
        raise HTTPException(status_code=503, detail=f"Queue backend unavailable: {ex}") from ex


def _job_is_active(status: str) -> bool:
    return status in {"queued", "started", "deferred", "scheduled"}


def _map_rq_status(status: str) -> str:
    return {
        "queued": "queued",
        "started": "running",
        "finished": "done",
        "failed": "failed",
        "deferred": "queued",
        "scheduled": "queued",
    }.get(status, status)


def _clear_request_mapping(request_key: Optional[str], job_id: Optional[str] = None) -> None:
    if not request_key:
        return
    try:
        current = REDIS.get(request_key)
        current_decoded = current.decode("utf-8") if isinstance(current, bytes) else current
        if job_id and current_decoded and current_decoded != job_id:
            return
        REDIS.delete(request_key)
    except RedisError:
        logger.warning("Failed to clear request mapping for %s", request_key, exc_info=True)


def _find_active_job_for_request_key(request_key: str) -> Optional[tuple[str, str]]:
    try:
        current = REDIS.get(request_key)
    except RedisError as ex:
        raise HTTPException(status_code=503, detail=f"Queue backend unavailable: {ex}") from ex

    if not current:
        return None

    job_id = current.decode("utf-8") if isinstance(current, bytes) else str(current)
    try:
        job = Job.fetch(job_id, connection=AURORA_QUEUE.connection)
        status = job.get_status(refresh=True)
    except NoSuchJobError:
        _clear_request_mapping(request_key, job_id)
        return None
    except RedisError as ex:
        raise HTTPException(status_code=503, detail=f"Queue backend unavailable: {ex}") from ex

    if _job_is_active(status):
        return job_id, _map_rq_status(status)

    _clear_request_mapping(request_key, job_id)
    return None


def _set_job_meta(job: Optional[Job], **updates: Any) -> None:
    if job is None:
        return
    meta = dict(job.meta or {})
    meta.update(updates)
    meta["updated_at"] = _iso_utc()
    job.meta = meta
    job.save_meta()


def _status_payload_from_rq_job(job: Job) -> Dict[str, Any]:
    meta = dict(job.meta or {})
    status = job.get_status(refresh=True)
    mapped_status = _map_rq_status(status)

    payload = {
        "status": mapped_status,
        "step": meta.get("step") or ("complete" if mapped_status == "done" else status),
        "created_at": meta.get("created_at"),
        "updated_at": meta.get("updated_at"),
        "time_tag": meta.get("time_tag"),
        "lead_hours": meta.get("lead_hours"),
        "region": meta.get("region"),
    }

    if meta.get("current_date") is not None:
        payload["current_date"] = meta.get("current_date")
    if meta.get("dates") is not None:
        payload["dates"] = meta.get("dates")

    if mapped_status == "done":
        payload["result"] = job.result
    elif mapped_status == "failed":
        payload["error"] = job.exc_info or meta.get("error") or "unknown"

    return payload


def build_public_prediction_url(file_name: str) -> str:
    return _build_public_file_view_url("prediction", file_name)


def build_public_hotspot_url(file_name: str) -> str:
    return _build_public_file_view_url("hotspot", file_name)


def _to_ndp_name_fragment(value: str) -> str:
    normalized = (value or "").lower().strip()
    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"[^a-z0-9_-]", "_", normalized)
    normalized = re.sub(r"[_-]{2,}", "_", normalized)
    normalized = normalized.strip("_-")
    return normalized or "unknown"


def _prediction_dates_from_paths(file_paths: List[Path]) -> tuple[Optional[str], Optional[str]]:
    dates: List[str] = []
    for fpath in file_paths:
        match = re.match(r"(?P<date>\d{4}-\d{2}-\d{2})_.+", fpath.name)
        if match:
            dates.append(match.group("date"))
    if not dates:
        return None, None
    dates.sort()
    return dates[0], dates[-1]


def register_prediction_files_ndp(
    file_paths: List[Path],
    region: str,
    *,
    date_range: Optional[Dict[str, Any]] = None,
    region_bounds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not file_paths:
        return {"registered": []}

    load_dotenv(override=False)

    token = os.getenv("TOKEN")
    api_url = os.getenv("API_URL")
    server = os.getenv("SERVER", "local")
    if not token or not api_url:
        logger.warning("Missing TOKEN or API_URL for NDP registration; skipping registration.")
        return {"registered": [{"error": "missing TOKEN or API_URL"}]}

    logger.info("Starting NDP registration: api_url=%s, server=%s, files=%d", api_url, server, len(file_paths))

    owner_org = "predictions"

    client = APIClient(base_url=api_url, token=token)
    streaming = StreamingClient(client)

    # Register a single dataset for this run and attach one method per file
    timestamp = datetime.utcnow().strftime("%Y%m%dt%H%M%S")
    safe_region = _to_ndp_name_fragment(region)
    dataset_name = _to_ndp_name_fragment(f"prediction_{safe_region}_{timestamp}")
    logger.debug("Computed dataset name for NDP: region=%r, dataset_name=%s", region, dataset_name)
    start_date, end_date = _prediction_dates_from_paths(file_paths)
    if isinstance(date_range, dict):
        start_date = str(date_range.get("start") or start_date or "")
        end_date = str(date_range.get("end") or end_date or "")

    bounds = region_bounds if isinstance(region_bounds, dict) else {}
    lat_min = bounds.get("lat_min")
    lat_max = bounds.get("lat_max")
    lon_min = bounds.get("lon_min")
    lon_max = bounds.get("lon_max")

    dataset_metadata = {
        "name": dataset_name,
        "title": f"Aurora Predictions {region} {timestamp}",
        "notes": "Forecast prediction CSVs generated by Aurora and hosted by forecast API.",
        "owner_org": owner_org,
        "extras": {
            "dataset_kind": "prediction",
            "region": region,
            "source": "aurora_forecast",
            "start_date": start_date,
            "end_date": end_date,
            "lat_min": lat_min,
            "lat_max": lat_max,
            "lon_min": lon_min,
            "lon_max": lon_max,
            "updated_at": datetime.utcnow().isoformat() + "Z",
        },
    }

    methods = []
    for fpath in file_paths:
        file_name = fpath.name
        stem = fpath.stem.lower()
        methods.append(
            {
                "type": "csv",
                "name": f"prediction_csv_{stem}",
                "description": "Hosted Aurora prediction CSV",
                "config": {"url": build_public_prediction_url(file_name)},
                "processing": {"delimiter": ",", "header_line": 0, "start_line": 1},
            }
        )

    logger.debug("Prepared dataset metadata and %d methods", len(methods))
    logger.debug("Dataset metadata preview: %s", {k: dataset_metadata.get(k) for k in ("name", "title", "owner_org")})
    try:
        # remove any existing dataset with the same name
        try:
            client.delete_resource_by_name(dataset_name, server=server)
        except Exception:
            logger.debug("No existing dataset to delete or delete failed (continuing)")

        # Prefer direct client.register_dataset if available, otherwise use streaming helper
        if hasattr(client, "register_dataset"):
            try:
                logger.debug("Calling client.register_dataset for %s", dataset_name)
                ds = client.register_dataset(dataset_metadata=dataset_metadata, methods=methods, server=server)
                logger.debug("client.register_dataset returned: %s", ds)
            except TypeError:
                logger.debug("client.register_dataset raised TypeError, falling back to streaming.register_data_source")
                ds = streaming.register_data_source(dataset_metadata=dataset_metadata, methods=methods, server=server)
                logger.debug("streaming.register_data_source returned: %s", ds)
        else:
            logger.debug("Using streaming.register_data_source for %s", dataset_name)
            ds = streaming.register_data_source(dataset_metadata=dataset_metadata, methods=methods, server=server)
            logger.debug("streaming.register_data_source returned: %s", ds)

        registered = []
        for fpath, method in zip(file_paths, methods):
            registered.append(
                {
                    "dataset_name": ds.get("name", dataset_name) if isinstance(ds, dict) else dataset_name,
                    "file": str(fpath),
                    "url": method["config"]["url"],
                    "start_date": start_date,
                    "end_date": end_date,
                    "lat_min": lat_min,
                    "lat_max": lat_max,
                    "lon_min": lon_min,
                    "lon_max": lon_max,
                }
            )
    except Exception as ex:
        logger.exception("NDP registration failed")
        registered = [{"error": str(ex)}]

    return {"registered": registered}


def register_hotspot_files_ndp(
    file_paths: List[Path],
    *,
    threshold_field: str,
    threshold: float,
    persistence_k: int,
) -> Dict[str, Any]:
    if not file_paths:
        return {"registered": []}

    load_dotenv(override=False)

    token = os.getenv("TOKEN")
    api_url = os.getenv("API_URL")
    server = os.getenv("SERVER", "local")
    if not token or not api_url:
        logger.warning("Missing TOKEN or API_URL for hotspot NDP registration; skipping registration.")
        return {"registered": [{"error": "missing TOKEN or API_URL"}]}

    owner_org = "hotspots"
    client = APIClient(base_url=api_url, token=token)
    streaming = StreamingClient(client)

    timestamp = datetime.utcnow().strftime("%Y%m%dt%H%M%S")
    dataset_name = _to_ndp_name_fragment(f"hotspots_{threshold_field}_{timestamp}")

    dataset_metadata = {
        "name": dataset_name,
        "title": f"Detected Hotspots {threshold_field} {timestamp}",
        "notes": "Hotspot CSVs generated from forecast threshold + persistence detection.",
        "owner_org": owner_org,
        "extras": {
            "dataset_kind": "hotspot",
            "source": "forecast_detection",
            "threshold_field": threshold_field,
            "threshold_value": str(threshold),
            "persistence_k": str(persistence_k),
            "updated_at": datetime.utcnow().isoformat() + "Z",
        },
    }

    methods = []
    for fpath in file_paths:
        file_name = fpath.name
        stem = fpath.stem.lower()
        methods.append(
            {
                "type": "csv",
                "name": f"hotspots_csv_{stem}",
                "description": "Hosted hotspot CSV",
                "config": {"url": build_public_hotspot_url(file_name)},
                "processing": {"delimiter": ",", "header_line": 0, "start_line": 1},
            }
        )

    try:
        try:
            client.delete_resource_by_name(dataset_name, server=server)
        except Exception:
            logger.debug("No existing hotspot dataset to delete or delete failed (continuing)")

        if hasattr(client, "register_dataset"):
            try:
                ds = client.register_dataset(dataset_metadata=dataset_metadata, methods=methods, server=server)
            except TypeError:
                ds = streaming.register_data_source(dataset_metadata=dataset_metadata, methods=methods, server=server)
        else:
            ds = streaming.register_data_source(dataset_metadata=dataset_metadata, methods=methods, server=server)

        registered = []
        for fpath, method in zip(file_paths, methods):
            registered.append(
                {
                    "dataset_name": ds.get("name", dataset_name) if isinstance(ds, dict) else dataset_name,
                    "file": str(fpath),
                    "url": method["config"]["url"],
                }
            )
    except Exception as ex:
        logger.exception("Hotspot NDP registration failed")
        registered = [{"error": str(ex)}]

    return {"registered": registered}


def _resolve_prediction_path(prediction_file: str) -> Path:
    value = (prediction_file or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="prediction file is required")

    # Hosted URL and canonical refs are first-class inputs.
    if value.startswith("prediction:") or value.startswith("hotspot:") or "/files" in value or "/download" in value:
        kind, _, target = _resolve_hosted_file_url(value)
        if kind != "prediction":
            raise HTTPException(status_code=400, detail="prediction_file must reference a prediction file")
        return target

    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = (PREDICTIONS_DIR / candidate).resolve()
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail=f"prediction file not found: {candidate}")
    return candidate


def _resolve_hotspots_path(hotspots_file: str) -> Path:
    value = (hotspots_file or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="hotspots file is required")

    # Hosted URL and canonical refs are first-class inputs.
    if value.startswith("prediction:") or value.startswith("hotspot:") or "/files" in value or "/download" in value:
        kind, _, target = _resolve_hosted_file_url(value)
        if kind != "hotspot":
            raise HTTPException(status_code=400, detail="hotspots_file must reference a hotspot file")
        return target

    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = (HOTSPOTS_DIR / candidate).resolve()
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail=f"hotspots file not found: {candidate}")
    return candidate


def _normalize_lon_deg(lon: float) -> float:
    return ((float(lon) + 180.0) % 360.0) - 180.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    lon1_n = _normalize_lon_deg(lon1)
    lon2_n = _normalize_lon_deg(lon2)
    dlon_deg = ((lon2_n - lon1_n) + 540.0) % 360.0 - 180.0

    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(dlon_deg)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _discover_sensors() -> List[Dict[str, Any]]:
    load_dotenv(override=False)

    token = os.getenv("TOKEN")
    api_url = os.getenv("API_URL")
    server = os.getenv("SERVER", "local")
    if not token or not api_url:
        raise HTTPException(
            status_code=400,
            detail="Missing TOKEN or API_URL in environment for sensor discovery.",
        )

    client = APIClient(base_url=api_url, token=token)
    sensor_datasets = client.search_datasets(
        terms=["sensor"],
        keys=["extras_dataset_kind"],
        server=server,
    )

    sensors: List[Dict[str, Any]] = []
    for ds in sensor_datasets:
        if not isinstance(ds, dict):
            continue
        extras = ds.get("extras", {})
        if not isinstance(extras, dict):
            continue

        lat = extras.get("latitude")
        lon = extras.get("longitude")
        if lat is None or lon is None:
            continue

        try:
            sensors.append(
                {
                    "name": ds.get("name"),
                    "station_id": extras.get("station_id"),
                    "lat": float(lat),
                    "lon": float(lon),
                }
            )
        except Exception:
            continue

    if not sensors:
        raise HTTPException(status_code=404, detail="No valid sensors found in catalog.")
    return sensors


WORKFLOW_SERVICE = AirQualityWorkflowService(
    project_root=PROJECT_ROOT,
    predictions_dir=PREDICTIONS_DIR,
    hotspots_dir=HOTSPOTS_DIR,
    map_dir=MAP_DIR,
    build_public_prediction_url=build_public_prediction_url,
    register_prediction_files_ndp=register_prediction_files_ndp,
    register_hotspot_files_ndp=register_hotspot_files_ndp,
    create_clients=_create_clients,
    logger=logger,
)


@app.get("/sensors")
def list_sensors(
    server: Optional[str] = Query(default=None, description="Optional server override."),
):
    resolved_server, client, _ = _create_clients(server=server)

    try:
        datasets = client.search_datasets(
            terms=["sensors"],
            server=resolved_server,
        )
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Sensor search failed: {ex}")

    items: List[Dict[str, Any]] = []
    for ds in datasets or []:
        if not isinstance(ds, dict):
            continue
        items.append(_dataset_to_sensor_item(ds))

    return {"count": len(items), "items": items}


@app.get("/sensors/{sensor_name}/consumption-method")
def get_sensor_consumption_method(
    sensor_name: str,
    server: Optional[str] = Query(default=None, description="Optional server override."),
):
    resolved_server, client, streaming = _create_clients(server=server)
    dataset = None

    # Primary path: use streaming consumption-method search.
    try:
        methods_search = streaming.search_consumption_methods(
            [sensor_name, "sensors"],
            server=resolved_server,
        )
        for ds in methods_search or []:
            if isinstance(ds, dict) and ds.get("name") == sensor_name:
                dataset = ds
                break
    except Exception:
        # Fall through to dataset lookup fallback.
        dataset = None

    # Fallback path: dataset search by exact name.
    if dataset is None:
        try:
            datasets = client.search_datasets(
                terms=[sensor_name],
                keys=["extras_dataset_kind", "extras_region"],
                server=resolved_server,
            )
        except Exception as ex:
            raise HTTPException(status_code=500, detail=f"Dataset lookup failed: {ex}")

        for ds in datasets or []:
            if isinstance(ds, dict) and ds.get("name") == sensor_name:
                dataset = ds
                break

    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Sensor dataset not found: {sensor_name}")

    methods = _methods_from_dataset(dataset)
    return {
        "sensor_name": sensor_name,
        "dataset_name": dataset.get("name"),
        "consumption_methods": methods,
        "count": len(methods),
    }


@app.post("/streams/create")
async def create_stream(
    consumption_method_ids: List[str] = Query(
        ...,
        description="Consumption method IDs. Repeat query param to add multiple values.",
    ),
    filter_semantics: Optional[List[str]] = Query(
        default=None,
        description="Optional filter semantics. Repeat query param to add multiple values.",
    ),
    server: Optional[str] = Query(default=None, description="Optional server override."),
):
    method_ids = [str(v).strip() for v in (consumption_method_ids or []) if str(v).strip()]
    if not method_ids:
        raise HTTPException(status_code=422, detail="consumption_method_ids must contain at least one ID.")

    resolved_server, _, streaming = _create_clients(server=server)
    try:
        stream_result = streaming.create_kafka_stream(
            consumption_method_ids=method_ids,
            filter_semantics=filter_semantics or [],
            server=resolved_server,
        )
        if inspect.isawaitable(stream_result):
            stream_result = await stream_result
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Stream creation failed: {ex}")

    warnings: List[str] = []
    data_stream_id = None
    if isinstance(stream_result, dict):
        data_stream_id = stream_result.get("data_stream_id") or stream_result.get("topic")
        raw_warnings = stream_result.get("warnings") or []
        if isinstance(raw_warnings, list):
            warnings = [str(w) for w in raw_warnings]
    else:
        data_stream_id = getattr(stream_result, "data_stream_id", None) or getattr(stream_result, "topic", None)
        raw_warnings = getattr(stream_result, "warnings", None)
        if isinstance(raw_warnings, list):
            warnings = [str(w) for w in raw_warnings]

    if not data_stream_id:
        raise HTTPException(status_code=500, detail="Stream created but no data_stream_id was returned.")

    return {
        "data_stream_id": data_stream_id,
        "consumption_method_ids": method_ids,
        "server": resolved_server,
        "warnings": warnings,
    }


@app.post("/streams/{topic}/consume/start")
async def start_stream_consume(topic: str, req: StartConsumeRequest):
    existing = STREAM_CONSUMERS.get(topic)
    action = req.action or "start"

    if action == "status":
        if not existing:
            return {
                "topic": topic,
                "status": "not_started",
                "mode": None,
                "started_at": None,
                "planned_stop_at": None,
                "ended_at": None,
                "poll_interval": None,
                "max_messages": None,
                "message_count": 0,
                "stop_requested": False,
                "stop_reason": None,
                "elapsed_seconds": 0,
                "error": None,
                "recent_messages": [],
            }
        return _consume_state_snapshot(existing)

    if action == "stop":
        if not existing:
            return {
                "topic": topic,
                "status": "not_started",
                "message_count": 0,
                "stop_reason": "not_started",
                "recent_messages": [],
            }
        if existing.get("status") == "running":
            existing["stop_requested"] = True
            existing["stop_reason"] = "manual_stop"
        return _consume_state_snapshot(existing)

    if existing and existing.get("status") == "running":
        snap = _consume_state_snapshot(existing)
        snap["status"] = "already_running"
        return snap

    minutes_value = req.minutes if req.minutes is not None else req.auto_close_minutes
    mode = "timed" if req.minutes is not None else req.mode
    server = _resolve_server(None)
    started_at = datetime.now(timezone.utc)
    planned_stop_at = started_at + timedelta(minutes=minutes_value) if mode == "timed" else None

    STREAM_CONSUMERS[topic] = {
        "topic": topic,
        "status": "running",
        "server": server,
        "mode": mode,
        "started_at": _iso_utc(started_at),
        "planned_stop_at": _iso_utc(planned_stop_at) if planned_stop_at else None,
        "poll_interval": req.poll_interval,
        "max_messages": req.max_messages,
        "message_count": 0,
        "stop_requested": False,
        "stop_reason": None,
        "recent_messages": [],
        "seen_keys": set(),
        "ended_at": None,
        "error": None,
    }

    STREAM_CONSUMERS[topic]["task"] = asyncio.create_task(
        _consume_topic_worker(
            topic,
            server=server,
            mode=mode,
            auto_close_minutes=minutes_value,
            poll_interval=req.poll_interval,
            max_messages=req.max_messages,
        )
    )

    snap = _consume_state_snapshot(STREAM_CONSUMERS[topic])
    snap["status"] = "started"
    return snap





def _find_matching_prediction_dataset(req: RunRequest) -> Optional[Dict[str, Any]]:
    resolved_server, client, _ = _create_clients()

    search_terms = [
        str(req.dates.start),
        str(req.dates.end),
        str(req.region.lat_max),
        str(req.region.lat_min),
        str(req.region.lon_max),
        str(req.region.lon_min),
        str(req.region.name),
    ]

    datasets = client.search_datasets(search_terms, server=resolved_server)

    expected_dates = build_date_list(req.dates.start, req.dates.end)
    time_tag = "-".join(t.replace(":", "") for t in FIXED_AURORA_CONFIG["times"])
    lead_hours = int(FIXED_AURORA_CONFIG["lead_hours"])

    expected_files = [
        f"{day}_{time_tag}_{lead_hours}h_{req.region.name}.csv"
        for day in expected_dates
    ]

    for ds in datasets or []:
        if not isinstance(ds, dict):
            continue

        extras = ds.get("extras") or {}
        if extras.get("dataset_kind") != "prediction":
            continue
        if str(extras.get("region")) != str(req.region.name):
            continue
        if str(extras.get("start_date")) != str(req.dates.start):
            continue
        if str(extras.get("end_date")) != str(req.dates.end):
            continue
        if str(extras.get("lat_min")) != str(req.region.lat_min):
            continue
        if str(extras.get("lat_max")) != str(req.region.lat_max):
            continue
        if str(extras.get("lon_min")) != str(req.region.lon_min):
            continue
        if str(extras.get("lon_max")) != str(req.region.lon_max):
            continue

        resources = ds.get("resources") or []
        matched_files = {}
        for resource in resources:
            name = str(resource.get("name") or "")
            m = re.match(r"prediction_csv_(.+)", name)
            if not m:
                continue

            file_name = m.group(1)
            if not file_name.endswith(".csv"):
                file_name = f"{file_name}.csv"

            matched_files[file_name] = resource.get("url") or build_public_prediction_url(file_name)

        if sorted(matched_files.keys()) != expected_files:
            continue

        return {
            "dataset_name": ds.get("name"),
            "result": {
                "predictions": [
                    str(Path("data/processed/predictions") / fname)
                    for fname in expected_files
                ],
                "prediction_urls": [
                    matched_files[fname] for fname in expected_files
                ],
                "predictions_dir": str(PREDICTIONS_DIR),
                "ndp": {"registered": []},
                "dates": expected_dates,
            },
        }

    return None


def run_prediction_job(req_payload: Dict[str, Any]) -> Dict[str, Any]:
    req = RunRequest.model_validate(req_payload)
    job = get_current_job(connection=AURORA_QUEUE.connection)
    request_key = None
    preexisting_files: set[str] = set()

    try:
        request_key = str((job.meta or {}).get("request_key") or "") if job else None
        _set_job_meta(job, step="init")

        cams_dir = (PROJECT_ROOT / Path("data/raw/cams_raw")).resolve()
        cams_dir.mkdir(parents=True, exist_ok=True)
        if cams_dir.exists():
            preexisting_files = {p.name for p in cams_dir.iterdir() if p.is_file()}
        _set_job_meta(job, preexisting_files=sorted(preexisting_files))

        def _progress(update: Dict[str, Any]) -> None:
            meta_update: Dict[str, Any] = {}
            if "step" in update:
                meta_update["step"] = update["step"]
            if "current_date" in update:
                meta_update["current_date"] = update["current_date"]
            if "dates" in update:
                meta_update["dates"] = update["dates"]
            if meta_update:
                _set_job_meta(job, **meta_update)

        result = WORKFLOW_SERVICE.run_model_batch(
            region=req.region.dict(),
            dates=req.dates.dict(),
            aurora=dict(FIXED_AURORA_CONFIG),
            overwrite=req.overwrite,
            on_progress=_progress,
        )

        _set_job_meta(job, step="complete")
        return result

    except Exception as ex:
        _set_job_meta(job, step="failed", error=str(ex), traceback=traceback.format_exc())
        raise
    finally:
        try:
            cams_dir = (PROJECT_ROOT / Path("data/raw/cams_raw")).resolve()
            removed: List[str] = []
            errors: List[Dict[str, str]] = []
            static_name = "aurora-0.4-air-pollution-static.pickle"
            if cams_dir.exists():
                current_files = {p.name for p in cams_dir.iterdir() if p.is_file()}
                created = current_files - preexisting_files
                for name in created:
                    if name == static_name:
                        continue
                    p = cams_dir / name
                    if p.exists() and p.is_file() and p.suffix.lower() in (".zip", ".nc"):
                        try:
                            p.unlink()
                            removed.append(str(p))
                        except Exception as cleanup_ex:
                            errors.append({"path": str(p), "error": str(cleanup_ex)})
            _set_job_meta(job, cleanup={"removed": removed, "errors": errors})
        except Exception as cleanup_ex:
            _set_job_meta(job, cleanup={"removed": [], "errors": [{"error": str(cleanup_ex)}]})
        finally:
            _clear_request_mapping(request_key, job.id if job else None)



@app.get("/health")
def health():
    return {"ok": True}


@app.post("/run-model")
def run(req: RunRequest = Body(..., examples={
    "default": {
        "summary": "Single-day Utah request",
        "value": {
            "region": {"name": "utah", "lat_min": 37.0, "lat_max": 42.1, "lon_min": 245.9, "lon_max": 251.0},
            "dates": {"start": "2025-01-16", "end": "2025-01-16"},
            "overwrite": False,
        },
    }
})):
    # Deployment policy fixes Aurora settings and ignores user-supplied overrides.
    times = list(FIXED_AURORA_CONFIG["times"])
    time_tag = "-".join([t.replace(":", "") for t in times]) if times else ""
    lead_hours = int(FIXED_AURORA_CONFIG["lead_hours"])
    enforce_date_policy(req.dates.start, req.dates.end)

    if not req.overwrite:
        existing = _find_matching_prediction_dataset(req)
        if existing:
            job_id = str(uuid.uuid4())
            JOBS[job_id] = {
                "status": "done",
                "step": "complete",
                "created_at": datetime.utcnow().isoformat() + "Z",
                "updated_at": datetime.utcnow().isoformat() + "Z",
                "time_tag": time_tag,
                "lead_hours": lead_hours,
                "region": getattr(req.region, "name", None),
                "result": existing["result"],
            }
            return {
                "job_id": job_id,
                "status": "done",
                "time_tag": time_tag,
                "lead_hours": lead_hours,
            }

    _require_redis()
    request_key = _build_request_key(req)

    try:
        with REDIS.lock(ENQUEUE_GUARD_LOCK, timeout=30, blocking_timeout=5):
            existing_job = _find_active_job_for_request_key(request_key)
            if existing_job:
                existing_job_id, existing_status = existing_job
                return {
                    "job_id": existing_job_id,
                    "status": existing_status,
                    "time_tag": time_tag,
                    "lead_hours": lead_hours,
                }

            can_accept_new_job()
            created_at = _iso_utc()
            job = AURORA_QUEUE.enqueue(
                "src.forecast_api.run_prediction_job",
                kwargs={"req_payload": req.dict()},
                job_timeout=RQ_JOB_TIMEOUT_SECONDS,
                result_ttl=RQ_JOB_TTL_SECONDS,
                ttl=RQ_JOB_TTL_SECONDS,
                meta={
                    "step": "queued",
                    "created_at": created_at,
                    "updated_at": created_at,
                    "time_tag": time_tag,
                    "lead_hours": lead_hours,
                    "region": getattr(req.region, "name", None),
                    "dates": req.dates.dict(),
                    "request_key": request_key,
                },
            )
            REDIS.set(request_key, job.id, ex=REQUEST_KEY_TTL_SECONDS)
            _record_enqueue_event(job.id)
    except HTTPException:
        raise
    except RedisError as ex:
        raise HTTPException(status_code=503, detail=f"Queue backend unavailable: {ex}") from ex

    return {"job_id": job.id, "status": "queued", "time_tag": time_tag, "lead_hours": lead_hours}


@app.get("/status/{job_id}")
def status(job_id: str):
    if job_id in JOBS:
        return JOBS[job_id]

    _require_redis()
    try:
        job = Job.fetch(job_id, connection=AURORA_QUEUE.connection)
    except NoSuchJobError as ex:
        raise HTTPException(status_code=404, detail="job not found") from ex
    except RedisError as ex:
        raise HTTPException(status_code=503, detail=f"Queue backend unavailable: {ex}") from ex

    return _status_payload_from_rq_job(job)


@app.get("/predictions")
def list_predictions():
    files = sorted([p.name for p in PREDICTIONS_DIR.glob("*.csv") if p.is_file()])
    return {
        "root": str(PREDICTIONS_DIR),
        "files": [
            {
                "name": name,
                "url": _build_public_file_view_url("prediction", name),
                "download_url": _build_public_file_download_url("prediction", name),
            }
            for name in files
        ],
    }


@app.get("/hotspots")
def list_hotspots():
    files = sorted([p.name for p in HOTSPOTS_DIR.glob("*.csv") if p.is_file()])
    return {
        "root": str(HOTSPOTS_DIR),
        "files": [
            {
                "name": name,
                "url": _build_public_file_view_url("hotspot", name),
                "download_url": _build_public_file_download_url("hotspot", name),
            }
            for name in files
        ],
    }


@app.get("/prediction-preview")
def prediction_preview(prediction_file: str, rows: int = 20):
    pred_path = _resolve_prediction_path(prediction_file)
    try:
        nrows = max(1, min(int(rows), 100))
    except Exception:
        nrows = 20

    try:
        preview_df = pd.read_csv(pred_path, nrows=nrows)
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"failed to read prediction CSV: {ex}")

    numeric_columns: List[str] = []
    for col in preview_df.columns:
        c = str(col).strip().lower()
        if c in {"timestamp", "latitude", "longitude", "lat", "lon", "cell_id"}:
            continue
        try:
            vals = pd.to_numeric(preview_df[col], errors="coerce")
            if vals.notna().any():
                numeric_columns.append(str(col))
        except Exception:
            continue

    return {
        "prediction_file": str(pred_path),
        "columns": [str(c) for c in preview_df.columns],
        "numeric_columns": numeric_columns,
        "row_count": int(len(preview_df)),
        "preview": preview_df.head(nrows).to_dict(orient="records"),
    }


@app.post("/detect-hotspot")
def detect_hotspot(req: DetectHotspotRequest):
    pred_path = _resolve_prediction_path(req.prediction_file)
    try:
        preview_df = pd.read_csv(pred_path, nrows=200)
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"failed to read prediction CSV: {ex}")

    threshold_runs: List[Dict[str, Any]] = []
    if req.field_thresholds:
        for entry in req.field_thresholds:
            if not isinstance(entry, dict):
                continue
            field = str(entry.get("field", "")).strip()
            if not field:
                continue
            try:
                threshold_value = float(entry.get("threshold"))
            except Exception:
                raise HTTPException(status_code=400, detail=f"invalid threshold value for field '{field}'")
            threshold_runs.append({"field": field, "threshold": threshold_value})
    else:
        threshold_runs.append({"field": req.threshold_field, "threshold": float(req.threshold)})

    if not threshold_runs:
        raise HTTPException(status_code=400, detail="No threshold fields selected. Select at least one threshold field.")

    for run in threshold_runs:
        if run["field"] not in preview_df.columns:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"threshold_field '{run['field']}' not found in prediction CSV columns. "
                    f"Available columns include: {list(preview_df.columns)}"
                ),
            )
        try:
            vals = pd.to_numeric(preview_df[run["field"]], errors="coerce")
        except Exception:
            vals = pd.Series([], dtype=float)
        if not vals.notna().any():
            raise HTTPException(
                status_code=400,
                detail=f"threshold_field '{run['field']}' is not numeric in the prediction CSV.",
            )

    try:
        return WORKFLOW_SERVICE.detect_hotspots(
            pred_path=pred_path,
            detection_request={
                "threshold": req.threshold,
                "persistence_k": req.persistence_k,
                "timestep_hours": req.timestep_hours,
                "threshold_field": req.threshold_field,
                "field_thresholds": threshold_runs,
                "top_k_hotspots": req.top_k_hotspots,
            },
        )
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"hotspot detection failed: {ex}")


@app.post("/select-sensor")
def select_sensor(req: SelectSensorRequest):
    hotspot_path = _resolve_hotspots_path(req.hotspots_file)
    try:
        return WORKFLOW_SERVICE.select_sensors(
            hotspot_path=hotspot_path,
            selection_request={
                "selection_mode": req.selection_mode,
                "k": req.k,
                "radius_km": req.radius_km,
            },
        )
    except ValueError as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"sensor selection failed: {ex}")
