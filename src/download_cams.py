from typing import List, Optional, Sequence, Tuple, Union
from pathlib import Path
import zipfile

import cdsapi
from huggingface_hub import hf_hub_download
import pandas as pd
import yaml


def download_cams_forecast(
    *,
    date: str,                                 # "YYYY-MM-DD"
    lead_hours: int = 24,                      # inclusive: 0..lead_hours
    times: Sequence[str] = ("00:00",),         # e.g., ("00:00",) or ("00:00","12:00")
    download_dir: Union[str, Path] = "../data/raw/cams_raw",
    overwrite: bool = False,                   # re-download even if files exist
) -> dict:
    """
    Download CAMS global atmospheric composition forecasts (surface + pressure levels)
    for one date, with possibly multiple base times (e.g. 00:00 and 12:00),
    and unpack to *one* surface-level .nc and *one* atmospheric .nc.

    Returns a dict with paths:
      {
        "static_path": <pickle>,
        "zip_path": <zip>,
        "surface_nc": <surface-level .nc>,
        "atmos_nc": <pressure-level .nc>
      }
    """

    # ----- Defaults -----
    variables = [
            # 2D / surface-esque (including total columns)
            "10m_u_component_of_wind", "10m_v_component_of_wind",
            "2m_temperature", "mean_sea_level_pressure",
            "particulate_matter_1um", "particulate_matter_2.5um",
            "particulate_matter_10um",
            "total_column_carbon_monoxide", "total_column_nitrogen_monoxide",
            "total_column_nitrogen_dioxide", "total_column_ozone",
            "total_column_sulphur_dioxide",
            # 3D / pressure-level compatible variable names (CAMS)
            "u_component_of_wind", "v_component_of_wind",
            "temperature", "geopotential", "specific_humidity",
            "carbon_monoxide", "nitrogen_dioxide", "nitrogen_monoxide",
            "ozone", "sulphur_dioxide",
    ]
    pressure_levels = [
            "50","100","150","200","250","300","400",
            "500","600","700","850","925","1000"
    ]

    # ----- Paths / filenames -----
    download_path = Path(download_dir).expanduser()
    download_path.mkdir(parents=True, exist_ok=True)

    # "00:00","12:00" -> "0000-1200"
    time_tag = "-".join(t.replace(":", "") for t in times)

    zip_name = f"{date}_{time_tag}_{lead_hours}h.nc.zip"
    sfc_name = f"{date}_{time_tag}_{lead_hours}h_surface-level.nc"
    plev_name = f"{date}_{time_tag}_{lead_hours}h_atmospheric.nc"

    zip_path = download_path / zip_name
    surface_nc = download_path / sfc_name
    atmos_nc = download_path / plev_name

    # ----- Download Aurora static variables (once) -----
    static_path = download_path / "aurora-0.4-air-pollution-static.pickle"
    if not static_path.exists():
        hf_hub_download(
            repo_id="microsoft/aurora",
            filename="aurora-0.4-air-pollution-static.pickle",
            local_dir=download_path,
        )
        print("Static variables downloaded!")
    else:
        print("Static variables already present.")

    # ----- Build CAMS request -----
    # lead_steps 0..lead_hours inclusive
    lead_steps = [str(h) for h in range(0, int(lead_hours) + 1)]

    req = {
        "type": "forecast",
        "format": "netcdf_zip",
        "date": date,
        "time": list(times),            # e.g. ["00:00","12:00"]
        "leadtime_hour": lead_steps,    # ["0","1",...,"12"]
        "variable": variables,
        "pressure_level": pressure_levels,
    }


    # ----- Retrieve (if needed / overwrite) -----
    need_zip = overwrite or not zip_path.exists()
    if need_zip:
        c = cdsapi.Client()
        c.retrieve(
            "cams-global-atmospheric-composition-forecasts",
            req,
            str(zip_path),
        )
        print(f"ZIP saved: {zip_path}")
    else:
        print(f"ZIP already exists: {zip_path}")

    if overwrite or not surface_nc.exists():
            # Error checking for zip file
            if not zip_path.exists() or zip_path.stat().st_size == 0:
                raise FileNotFoundError(f"Zip file missing or empty: {zip_path}")
            try:
                with zipfile.ZipFile(zip_path, "r") as zf, open(surface_nc, "wb") as f:
                    f.write(zf.read("data_sfc.nc"))
                print(f"Surface-level saved: {surface_nc}")
            except zipfile.BadZipFile as e:
                print(f"Error: {zip_path} is not a valid zip file: {e}")
                raise
    else:
        print(f"Surface-level already exists: {surface_nc}")

    if overwrite or not atmos_nc.exists():
            # Error checking for zip file
            if not zip_path.exists() or zip_path.stat().st_size == 0:
                raise FileNotFoundError(f"Zip file missing or empty: {zip_path}")
            try:
                with zipfile.ZipFile(zip_path, "r") as zf, open(atmos_nc, "wb") as f:
                    f.write(zf.read("data_plev.nc"))
                print(f"Atmospheric (pressure-level) saved: {atmos_nc}")
            except zipfile.BadZipFile as e:
                print(f"Error: {zip_path} is not a valid zip file: {e}")
                raise
    else:
        print(f"Atmospheric already exists: {atmos_nc}")

    return {
        "static_path": str(static_path),
        "zip_path": str(zip_path),
        "surface_nc": str(surface_nc),
        "atmos_nc": str(atmos_nc),
    }


def main():
    # when run from inside src/, config.yaml is one level up
    CONFIG_PATH = "config.yaml"
    cfg = yaml.safe_load(open(CONFIG_PATH))

    # ---- Dates from config ----
    date_cfg = cfg["dates"]
    start = pd.to_datetime(date_cfg["start"])
    end = pd.to_datetime(date_cfg["end"])
    date_list = [d.strftime("%Y-%m-%d") for d in pd.date_range(start, end, freq="D")]

    # ---- Aurora / CAMS options from config ----
    times = cfg["aurora"]["times"]             # ["00:00","12:00"]
    lead_hours = int(cfg["aurora"]["lead_hours"])   # 12
    download_dir = cfg["paths"]["cams_download"]

    # We’ll keep global_domain=True and let Aurora crop the bbox later.
    for date in date_list:
        download_cams_forecast(
            date=date,
            lead_hours=lead_hours,
            times=times,
            download_dir=download_dir,
            overwrite=False,
        )


if __name__ == "__main__":
    main()
