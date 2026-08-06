import hashlib
import pandas as pd


def build_trade_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """Build a compact traceable business identifier for each invoice line."""
    out = df.copy()
    avg_amount = out.groupby([out["document_date_parsed"].dt.to_period("M"), "buyer_name", "product_category"])["amount"].transform("mean")
    ref3 = out["reference_no"].str.replace(r"\D", "", regex=True).str[-3:].str.zfill(3)
    avg2 = avg_amount.round().astype("Int64").astype("string").str.zfill(2).str[:2]
    month2 = out["document_date_parsed"].dt.strftime("%m")
    buyer1 = out["buyer_name"].str[0]
    out["trade_identifier"] = "T" + ref3 + avg2 + month2 + buyer1
    return out


def hash_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["hashed_trade_identifier"] = out["trade_identifier"].map(lambda v: hashlib.sha256(v.encode("utf-8")).hexdigest())
    if out["trade_identifier"].nunique() != out["hashed_trade_identifier"].nunique():
        raise RuntimeError("Hash uniqueness was not preserved")
    return out
