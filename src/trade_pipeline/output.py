import json
from dataclasses import asdict
from pathlib import Path
import pandas as pd
from .config import PipelineConfig
from .data_quality import deduplicate


def write_csv(df, path):
    path.parent.mkdir(parents=True, exist_ok=True); df.to_csv(path, index=False)


def write_pipeline_outputs(output_dir: Path, config: PipelineConfig, source_files, master, validation_failed, duplicate_failed, cleaned, anomaly_review, transformed, hashed) -> dict:
    failed = pd.concat([validation_failed, duplicate_failed], ignore_index=True)
    hashed_output = hashed.drop(columns=["trade_identifier"])
    write_csv(master, output_dir / "staging/master_union_in_scope.csv")
    write_csv(cleaned, output_dir / "cleaned/trade_invoice_cleaned.csv")
    write_csv(transformed, output_dir / "transformed/trade_invoice_transformed.csv")
    write_csv(failed, output_dir / "failed/trade_invoice_failed.csv")
    write_csv(anomaly_review, output_dir / "review/potential_unit_price_anomalies.csv")
    write_csv(hashed_output, output_dir / "hashed/trade_invoice_hashed.csv")
    manifest = {
        "configuration": asdict(config),
        "source_files": [p.name for p in source_files],
        "row_counts": {"master":len(master),"validation_failed":len(validation_failed),"duplicate_failed":len(duplicate_failed),"anomaly_review":len(anomaly_review),"cleaned":len(cleaned),"failed_total":len(failed)},
        "quality_checks": {
            "reconciled": len(master) == len(cleaned) + len(failed),
            "anomalies_retained_in_cleaned": int(cleaned["is_anomalous_price"].sum()) == len(anomaly_review),
            "hash_cardinality_preserved": transformed["trade_identifier"].nunique() == hashed["hashed_trade_identifier"].nunique(),
            "cleaned_has_no_duplicate_keys": len(deduplicate(cleaned)[1]) == 0,
        },
    }
    (output_dir/"run_manifest.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
    return manifest
