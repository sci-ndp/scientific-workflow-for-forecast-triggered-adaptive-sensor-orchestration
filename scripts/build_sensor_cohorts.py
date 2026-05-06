import argparse
import csv
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence


OUTPUT_COLUMNS = ["station_id", "station_name", "state_id", "lat", "lon", "elevation_m"]


@dataclass(frozen=True)
class BBox:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


@dataclass(frozen=True)
class CohortSpec:
    region: str
    source_key: str
    target_count: int
    bbox: Optional[BBox]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build deterministic station cohort CSVs for experiment sizes "
            "8,200,681,1000,2365."
        )
    )
    parser.add_argument(
        "--source-681",
        default="paper/results/stations_metadata.csv",
        help="CSV source for 681-based cohorts.",
    )
    parser.add_argument(
        "--source-2365",
        default="paper/results/stations_metadata_or.csv",
        help="CSV source for 2365-based cohorts.",
    )
    parser.add_argument(
        "--outdir",
        default="paper/results/cohorts",
        help="Output directory for cohort CSVs.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260310,
        help="Random seed for deterministic sampling.",
    )
    return parser.parse_args()


def load_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = set(reader.fieldnames or [])
        required = {"station_id", "station_name", "lat", "lon", "elevation_m"}
        missing = sorted(required - fields)
        if missing:
            raise ValueError(f"{path} missing required columns: {missing}")

        by_station: Dict[str, Dict[str, str]] = {}
        for row in reader:
            station_id = (row.get("station_id") or "").strip()
            if not station_id:
                continue

            lat = (row.get("lat") or "").strip()
            lon = (row.get("lon") or "").strip()
            # Sensor selection depends on coordinates; skip malformed rows.
            try:
                float(lat)
                float(lon)
            except Exception:
                continue

            out = {
                "station_id": station_id,
                "station_name": (row.get("station_name") or station_id).strip(),
                "state_id": (row.get("state_id") or "").strip(),
                "lat": lat,
                "lon": lon,
                "elevation_m": (row.get("elevation_m") or "").strip(),
            }
            # Keep first occurrence for stable dedupe.
            if station_id not in by_station:
                by_station[station_id] = out

    return sorted(by_station.values(), key=lambda r: r["station_id"])


def filter_bbox(rows: Iterable[Dict[str, str]], bbox: Optional[BBox]) -> List[Dict[str, str]]:
    if bbox is None:
        return list(rows)
    out: List[Dict[str, str]] = []
    for row in rows:
        lat = float(row["lat"])
        lon = float(row["lon"])
        if bbox.lat_min <= lat <= bbox.lat_max and bbox.lon_min <= lon <= bbox.lon_max:
            out.append(row)
    return out


def deterministic_sample(rows: Sequence[Dict[str, str]], k: int, seed: int) -> List[Dict[str, str]]:
    if k > len(rows):
        raise ValueError(f"Requested sample size {k} exceeds available rows {len(rows)}")
    rng = random.Random(seed)
    idx = rng.sample(range(len(rows)), k)
    sampled = [rows[i] for i in idx]
    return sorted(sampled, key=lambda r: r["station_id"])


def write_csv(path: Path, rows: Sequence[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in OUTPUT_COLUMNS})


def build_specs() -> List[CohortSpec]:
    utah_bbox = BBox(lat_min=37.0, lat_max=42.1, lon_min=-114.1, lon_max=-109.0)
    return [
        CohortSpec(
            region="utah",
            source_key="2365",
            target_count=8,
            bbox=utah_bbox,
        ),
        CohortSpec(
            region="usa",
            source_key="681",
            target_count=200,
            bbox=None,
        ),
        CohortSpec(
            region="usa",
            source_key="681",
            target_count=681,
            bbox=None,
        ),
        CohortSpec(
            region="usa",
            source_key="2365",
            target_count=1000,
            bbox=None,
        ),
        CohortSpec(
            region="usa",
            source_key="2365",
            target_count=2365,
            bbox=None,
        ),
    ]


def main() -> None:
    args = parse_args()

    source_681 = Path(args.source_681).resolve()
    source_2365 = Path(args.source_2365).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    sources = {
        "681": load_rows(source_681),
        "2365": load_rows(source_2365),
    }
    specs = build_specs()
    for i, spec in enumerate(specs):
        source_rows = sources[spec.source_key]
        filtered = filter_bbox(source_rows, spec.bbox)
        if spec.target_count == len(filtered):
            selected = list(filtered)
        else:
            # Use a stable seed offset per cohort to avoid accidental identical subsets across sizes.
            selected = deterministic_sample(filtered, spec.target_count, args.seed + i)

        filename = f"{spec.region}_sensors_{spec.target_count}.csv"
        output_path = outdir / filename
        write_csv(output_path, selected)
        print(f"[ok] {filename}: {len(selected)} rows")


if __name__ == "__main__":
    main()
