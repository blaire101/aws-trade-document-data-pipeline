from pathlib import Path
from tempfile import TemporaryDirectory
from .config import PipelineConfig
from .data_quality import deduplicate, flag_price_anomalies, profile_dataframe, validate_and_standardize
from .ingestion import copy_raw_files, discover_csv_files, load_master, select_source_files
from .output import write_pipeline_outputs
from .transformation import build_trade_identifier, hash_identifiers


def prepare_master(source_files, output_dir, config):
    if not source_files: raise ValueError("No source files contain rows in the configured assessment period")
    copy_raw_files(source_files, output_dir / "raw")
    return load_master(source_files, config)

def run_pipeline(input_path: Path, output_dir: Path, config: PipelineConfig) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    if input_path.suffix.lower()==".zip":
        with TemporaryDirectory(prefix="trade_pipeline_") as temp_dir:
            files=discover_csv_files(input_path,Path(temp_dir)); source_files=select_source_files(files,config.start_date,config.end_date); master=prepare_master(source_files,output_dir,config)
    else:
        files=discover_csv_files(input_path); source_files=select_source_files(files,config.start_date,config.end_date); master=prepare_master(source_files,output_dir,config)
    profile_dataframe(master, output_dir/"profiling")
    valid, validation_failed = validate_and_standardize(master, config)
    deduped, duplicate_failed = deduplicate(valid)
    cleaned, review = flag_price_anomalies(deduped, config)
    transformed = build_trade_identifier(cleaned)
    hashed = hash_identifiers(transformed)
    return write_pipeline_outputs(output_dir,config,source_files,master,validation_failed,duplicate_failed,cleaned,review,transformed,hashed)
