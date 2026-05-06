import gc
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import xarray as xr
from aurora import AuroraAirPollution, Batch, Metadata


def run_aurora_forecast(
    surf_path: Path,
    atmo_path: Path,
    static_path: Path,
    out_csv: Path,
    bbox: dict,
):
    """
    Run Aurora on CAMS global files and save gridded hourly predictions.

    Parameters
    ----------
    surf_path : Path
        CAMS surface-level NetCDF (data_sfc.nc from netcdf_zip)
    atmo_path : Path
        CAMS pressure-level NetCDF (data_plev.nc from netcdf_zip)
    static_path : Path
        aurora-0.4-air-pollution-static.pickle
    out_csv : Path
        Output CSV path
    bbox : dict
        dict with keys: lat_min, lat_max, lon_min, lon_max
    """

    # ------------------ helpers ------------------
    def detect_lon_domain(ds_lon):
        lon_min = float(np.nanmin(ds_lon))
        lon_max = float(np.nanmax(ds_lon))
        return "0_360" if lon_min >= 0 and lon_max > 180 else "-180_180"

    def to_dataset_lon(lon_vals, domain):
        arr = np.asarray(lon_vals, dtype=float)
        if domain == "0_360":
            arr = arr % 360.0
            arr = np.where(arr < 0, arr + 360.0, arr)
        else:
            arr = ((arr + 180.0) % 360.0) - 180.0
        return float(arr) if np.ndim(lon_vals) == 0 else arr

    def ensure_model_orientation(ds):
        if ds.longitude.size >= 2 and not np.all(np.diff(ds.longitude.values) > 0):
            ds = ds.isel(longitude=np.argsort(ds.longitude.values))
        if ds.latitude.size >= 2 and not np.all(np.diff(ds.latitude.values) < 0):
            ds = ds.isel(latitude=slice(None, None, -1))
        return ds

    def slice_bbox(ds, bbox):
        lat = ds.latitude
        lon = ds.longitude
        domain = detect_lon_domain(lon.values)
        lon_min_ds, lon_max_ds = to_dataset_lon(
            [bbox["lon_min"], bbox["lon_max"]], domain
        )
        lat_min, lat_max = bbox["lat_min"], bbox["lat_max"]
        if lat[0] > lat[-1]:
            lat_slice = slice(lat_max, lat_min)
        else:
            lat_slice = slice(lat_min, lat_max)
        lon_slice = slice(lon_min_ds, lon_max_ds)
        return ds.sel(latitude=lat_slice, longitude=lon_slice)

    def trim_to_patch_multiple(ds, patch_size):
        h, w = ds.sizes["latitude"], ds.sizes["longitude"]
        Hc = (h // patch_size) * patch_size
        Wc = (w // patch_size) * patch_size
        if (Hc, Wc) != (h, w):
            ds = ds.isel(latitude=slice(0, Hc), longitude=slice(0, Wc))
        return ds

    # ------------------ static + datasets ------------------
    print("Loading static variables...")
    with open(static_path, "rb") as f:
        static_vars_full = pickle.load(f)

    print("Loading CAMS datasets...")
    surf_all = xr.open_dataset(surf_path, engine="netcdf4", decode_timedelta=True)
    atmo_all = xr.open_dataset(atmo_path, engine="netcdf4", decode_timedelta=True)

    if "forecast_period" in surf_all.dims:
        surf_all = surf_all.rename({"forecast_period": "time"})
    if "forecast_period" in atmo_all.dims:
        atmo_all = atmo_all.rename({"forecast_period": "time"})

    # Crop to bbox
    surf_all = ensure_model_orientation(slice_bbox(surf_all, bbox))
    atmo_all = ensure_model_orientation(slice_bbox(atmo_all, bbox))

    print("Loading Aurora model...")
    model = AuroraAirPollution()
    model.load_checkpoint()
    model.eval()
    ps = int(model.patch_size)

    surf_all = trim_to_patch_multiple(surf_all, ps)
    atmo_all = trim_to_patch_multiple(atmo_all, ps)

    # time mapping
    surf_periods = surf_all["time"].values
    atmo_periods = atmo_all["time"].values
    atmo_for_surf_idx = np.searchsorted(atmo_periods, surf_periods, side="right") - 1
    atmo_for_surf_idx = np.clip(atmo_for_surf_idx, 0, len(atmo_periods) - 1)
    print("Surface steps:", len(surf_periods), "Atmos steps:", len(atmo_periods))

    # static slice
    lat = surf_all.latitude.values
    lon = surf_all.longitude.values
    static_vars_tile = {
        k: (v[:, : len(lat), : len(lon)] if v.ndim == 3 else v[: len(lat), : len(lon)])
        for k, v in static_vars_full.items()
        if v.ndim in (2, 3)
    }

    if "forecast_reference_time" in surf_all:
        fc_ref_vals = pd.to_datetime(surf_all["forecast_reference_time"].values)

    # ------------------ main loop ------------------
    frames = []

    for fc_ref in fc_ref_vals:
        for surf_idx, atmos_idx in enumerate(atmo_for_surf_idx):
            if surf_idx == 0 or atmos_idx < 0:
                continue

            print(f"Processing hour {surf_idx}/{len(surf_periods)-1}")

            surf_prev = surf_all.isel(time=surf_idx - 1)
            surf_curr = surf_all.isel(time=surf_idx)
            atmo_prev = atmo_all.isel(time=atmos_idx - 1 if atmos_idx > 0 else 0)
            atmo_curr = atmo_all.isel(time=atmos_idx)

            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

            # ---- surface vars (1,2,H,W) ----
            # IMPORTANT: these *match your working snippet & CAMS short names*
            surf_vars = {}
            for k, var in {
                "2t": "t2m",
                "10u": "u10",
                "10v": "v10",
                "msl": "msl",
                "pm1": "pm1",
                "pm2p5": "pm2p5",
                "pm10": "pm10",
                "tcco": "tcco",
                "tc_no": "tc_no",
                "tcno2": "tcno2",
                "gtco3": "gtco3",
                "tcso2": "tcso2",
            }.items():
                a = np.asarray(surf_prev[var].values)
                b = np.asarray(surf_curr[var].values)
                if a.ndim == 3:
                    a = a[0]
                if b.ndim == 3:
                    b = b[0]
                surf_vars[k] = torch.from_numpy(
                    np.stack([a, b], axis=0)[None].astype(np.float32)
                )

            # ---- atmospheric vars (1,2,L,H,W) ----
            atmos_vars = {}
            for k, var in {
                "t": "t",
                "u": "u",
                "v": "v",
                "q": "q",
                "z": "z",
                "co": "co",
                "no": "no",
                "no2": "no2",
                "go3": "go3",
                "so2": "so2",
            }.items():
                arr_prev = np.asarray(atmo_prev[var].values)
                arr_curr = np.asarray(atmo_curr[var].values)
                if arr_prev.ndim == 4:  # (time,L,H,W)
                    arr_prev = arr_prev[0]
                    arr_curr = arr_curr[0]
                assert (
                    arr_prev.ndim == 3
                ), f"{var} expected 3D (L,H,W), got shape {arr_prev.shape}"
                atmos_vars[k] = torch.from_numpy(
                    np.stack([arr_prev, arr_curr], axis=0)[None].astype(np.float32)
                )

            # metadata
            abs_time = fc_ref + pd.to_timedelta(surf_curr.time.values)
            batch = Batch(
                surf_vars=surf_vars,
                static_vars={k: torch.from_numpy(v) for k, v in static_vars_tile.items()},
                atmos_vars=atmos_vars,
                metadata=Metadata(
                    lat=torch.from_numpy(atmo_curr.latitude.values),
                    lon=torch.from_numpy(atmo_curr.longitude.values),
                    time=(pd.Timestamp(abs_time).to_pydatetime(),),
                    atmos_levels=tuple(int(x) for x in atmo_curr.pressure_level.values),
                ),
            )

            with torch.inference_mode():
                pred = model(batch)

            # ---------------- OUTPUT EXTRACTION ----------------
            lat_out = atmo_curr.latitude.values
            lon_out = atmo_curr.longitude.values
            flip_lat = lat_out[0] > lat_out[-1]

            def get_pred(name, scale=1.0):
                arr = pred.surf_vars[name][0, 0].cpu().numpy().astype(np.float32)
                if flip_lat:
                    arr = arr[::-1, :]
                return arr * scale

            pm1 = get_pred("pm1", 1e9)
            pm25 = get_pred("pm2p5", 1e9)
            pm10 = get_pred("pm10", 1e9)
            co = get_pred("tcco")
            no = get_pred("tc_no")
            no2 = get_pred("tcno2")
            o3 = get_pred("gtco3")
            so2 = get_pred("tcso2")

            LON2D, LAT2D = np.meshgrid(lon_out, lat_out)

            frame = pd.DataFrame(
                {
                    "timestamp": pd.Timestamp(abs_time),
                    "latitude": LAT2D.ravel(),
                    "longitude": LON2D.ravel(),
                    "pm1_ugm3": pm1.ravel(),
                    "pm2p5_ugm3": pm25.ravel(),
                    "pm10_ugm3": pm10.ravel(),
                    "co": co.ravel(),
                    "no": no.ravel(),
                    "no2": no2.ravel(),
                    "o3": o3.ravel(),
                    "so2": so2.ravel(),
                }
            )

            frames.append(frame)

    # ------------------ save ------------------
    final = pd.concat(frames, ignore_index=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(out_csv, index=False)
    print(f"\nSaved hourly predictions with all surface variables to {out_csv}")
