from pathlib import Path
import pandas as pd
from .config import PipelineConfig
from .ingestion import CANONICAL_COLUMNS


def normalize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)


def profile_dataframe(df: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    profile = pd.DataFrame({
        "column": df.columns,
        "dtype": [str(df[c].dtype) for c in df.columns],
        "row_count": [len(df)] * len(df.columns),
        "null_count": [int(df[c].isna().sum()) for c in df.columns],
        "null_pct": [round(float(df[c].isna().mean() * 100), 4) for c in df.columns],
        "distinct_count": [int(df[c].nunique(dropna=True)) for c in df.columns],
    })
    profile.to_csv(output_dir / "column_profile.csv", index=False)
    for column in ["buyer_name", "document_type", "product_category", "seller_name"]:
        df[column].value_counts(dropna=False).rename_axis(column).reset_index(name="row_count").to_csv(
            output_dir / f"{column}_distribution.csv", index=False
        )


def validate_and_standardize(master: pd.DataFrame, config: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = master.copy()
    text_columns = ["document_date", "buyer_name", "document_type", "reference_no", "seller_name", "product_category", "item_description"]
    for c in text_columns:
        df[c] = normalize_text(df[c])
    for c in ["buyer_name", "document_type", "seller_name", "product_category"]:
        df[c] = df[c].str.upper()

    df["document_date_parsed"] = pd.to_datetime(df["document_date"], errors="coerce")
    for c in ["quantity", "invoice_year", "unit_price", "amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    failures = pd.Series("", index=df.index, dtype="string")
    def add_failure(mask, code):
        failures.loc[mask] = failures.loc[mask].apply(lambda v: code if v == "" else f"{v}|{code}")

    start, end = pd.Timestamp(config.start_date), pd.Timestamp(config.end_date)
    add_failure(df["document_date_parsed"].isna(), "INVALID_DOCUMENT_DATE")
    add_failure(df["document_date_parsed"].notna() & ~df["document_date_parsed"].between(start, end), "DATE_OUT_OF_SCOPE")
    add_failure(df["document_type"].ne(config.target_document_type.upper()), "INVALID_DOCUMENT_TYPE")

    for column, code in [("buyer_name", "INVALID_BUYER"), ("document_type", "INVALID_DOCUMENT_TYPE"), ("product_category", "INVALID_PRODUCT_CATEGORY")]:
        counts = df[column].value_counts(dropna=True)
        valid_values = counts[counts >= config.minimum_category_frequency].index
        add_failure(df[column].isna() | ~df[column].isin(valid_values), code)

    add_failure(df["reference_no"].isna() | df["reference_no"].eq(""), "MISSING_REFERENCE_NO")
    add_failure(df["seller_name"].isna() | df["seller_name"].eq(""), "MISSING_SELLER_NAME")
    add_failure(df["item_description"].isna() | df["item_description"].eq(""), "MISSING_ITEM_DESCRIPTION")
    add_failure(df["quantity"].isna() | (df["quantity"] <= 0), "INVALID_QUANTITY")
    add_failure(df["amount"].isna() | (df["amount"] <= 0), "INVALID_AMOUNT")
    add_failure(df["invoice_year"].isna() | (df["invoice_year"] <= 0), "INVALID_INVOICE_YEAR")
    add_failure(df["document_date_parsed"].notna() & df["invoice_year"].notna() & (df["invoice_year"] != df["document_date_parsed"].dt.year), "INVOICE_YEAR_MISMATCH")
    add_failure(df["unit_price"].notna() & (df["unit_price"] <= 0), "INVALID_UNIT_PRICE")

    failed = df.loc[failures.ne("")].copy(); failed["failure_reason"] = failures.loc[failures.ne("")]
    valid = df.loc[failures.eq("")].copy()
    valid["unit_price_recomputed"] = valid["amount"] / valid["quantity"]
    valid["unit_price_variance"] = valid["unit_price"] - valid["unit_price_recomputed"]
    valid["unit_price"] = valid["unit_price"].fillna(valid["unit_price_recomputed"])
    return valid.reset_index(drop=True), failed.reset_index(drop=True)


def deduplicate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Preserve the original case-study rule: all business columns except the monetary winner field.
    key_columns = [c for c in CANONICAL_COLUMNS if c != "amount"]
    ranked = df.sort_values(key_columns + ["amount"], ascending=[True]*len(key_columns)+[False], kind="mergesort")
    duplicate_mask = ranked.duplicated(subset=key_columns, keep="first")
    failed = ranked.loc[duplicate_mask].copy(); failed["failure_reason"] = "DUPLICATE_LOWER_AMOUNT"
    kept = ranked.loc[~duplicate_mask].copy().sort_index()
    return kept.reset_index(drop=True), failed.reset_index(drop=True)


def flag_price_anomalies(df: pd.DataFrame, config: PipelineConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = df.copy()
    out["analysis_unit_price"] = out["amount"] / out["quantity"]
    out["year"] = out["document_date_parsed"].dt.year
    group_columns = ["year", "buyer_name", "product_category"]
    q1 = out.groupby(group_columns)["analysis_unit_price"].transform("quantile", 0.25)
    q3 = out.groupby(group_columns)["analysis_unit_price"].transform("quantile", 0.75)
    iqr = q3 - q1
    lower = q1 - config.anomaly_iqr_multiplier * iqr
    upper = q3 + config.anomaly_iqr_multiplier * iqr
    out["is_anomalous_price"] = ((out["analysis_unit_price"] < lower) | (out["analysis_unit_price"] > upper)).fillna(False)
    out["anomaly_reason"] = pd.Series(pd.NA, index=out.index, dtype="string")
    out.loc[out["is_anomalous_price"], "anomaly_reason"] = "POTENTIAL_UNIT_PRICE_ANOMALY_IQR"
    return out.reset_index(drop=True), out.loc[out["is_anomalous_price"]].reset_index(drop=True)
