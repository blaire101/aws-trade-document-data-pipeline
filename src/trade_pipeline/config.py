from dataclasses import dataclass


@dataclass
class PipelineConfig:
    """Configuration for the trade-invoice post-extraction ETL pipeline."""

    start_date: str = "2020-01-01"
    end_date: str = "2022-12-31"
    target_document_type: str = "CINV"
    minimum_category_frequency: int = 2
    anomaly_iqr_multiplier: float = 1.5
