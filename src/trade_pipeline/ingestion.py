import shutil
import zipfile
from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import PipelineConfig

CANONICAL_COLUMNS = [
    "document_date", "buyer_name", "document_type", "reference_no", "seller_name",
    "product_category", "quantity", "item_description", "invoice_year", "unit_price", "amount",
]
# Some client exports may omit unit_price; it can be recomputed from amount / quantity.
REQUIRED_COLUMNS = [c for c in CANONICAL_COLUMNS if c != "unit_price"]


def extract_zip(input_zip: Path, extraction_dir: Path) -> list[Path]:
    if not zipfile.is_zipfile(input_zip):
        raise ValueError(f"Input is not a valid ZIP archive: {input_zip}")
    with zipfile.ZipFile(input_zip) as archive:
        archive.extractall(extraction_dir)
    return sorted(extraction_dir.rglob("*.csv"))


def discover_csv_files(input_path: Path, extraction_dir: Path | None = None) -> list[Path]:
    if not input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")
    if input_path.is_dir():
        files = sorted(input_path.rglob("*.csv"))
        if not files:
            raise FileNotFoundError(f"No CSV files found in directory: {input_path}")
        return files
    if input_path.suffix.lower() == ".csv":
        return [input_path]
    if input_path.suffix.lower() == ".zip":
        if extraction_dir is None:
            raise ValueError("extraction_dir is required for ZIP input")
        return extract_zip(input_path, extraction_dir)
    raise ValueError("Expected a ZIP file, CSV directory, or single CSV file")


def select_source_files(csv_files: Iterable[Path], start_date: str, end_date: str) -> list[Path]:
    selected = []
    start, end = pd.Timestamp(start_date), pd.Timestamp(end_date)
    for path in csv_files:
        dates = pd.to_datetime(pd.read_csv(path, usecols=["document_date"])["document_date"], errors="coerce")
        if dates.between(start, end).any():
            selected.append(path)
    return selected


def copy_raw_files(source_files: Iterable[Path], raw_dir: Path) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    for path in source_files:
        shutil.copy2(path, raw_dir / path.name)


def load_master(source_files: Iterable[Path], config: PipelineConfig) -> pd.DataFrame:
    frames = []
    for path in source_files:
        frame = pd.read_csv(path, dtype={"reference_no": "string"}, low_memory=False)
        missing = set(REQUIRED_COLUMNS) - set(frame.columns)
        if missing:
            raise ValueError(f"{path.name} is missing required columns: {sorted(missing)}")
        for column in CANONICAL_COLUMNS:
            if column not in frame.columns:
                frame[column] = pd.NA
        frame = frame[CANONICAL_COLUMNS].copy()
        frame["source_file"] = path.name
        frame["source_row_number"] = range(2, len(frame) + 2)
        frames.append(frame)
    master = pd.concat(frames, ignore_index=True, sort=False)
    dt = pd.to_datetime(master["document_date"], errors="coerce")
    in_period = dt.between(pd.Timestamp(config.start_date), pd.Timestamp(config.end_date))
    target_type = master["document_type"].astype("string").str.strip().str.upper().eq(config.target_document_type.upper())
    # Keep malformed dates for validation, but exclude valid rows outside the assessment period and non-target types.
    return master.loc[(in_period | dt.isna()) & target_type].reset_index(drop=True)
