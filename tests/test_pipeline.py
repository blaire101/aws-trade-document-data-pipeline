import pandas as pd
from trade_pipeline.config import PipelineConfig
from trade_pipeline.data_quality import deduplicate, flag_price_anomalies
from trade_pipeline.transformation import build_trade_identifier, hash_identifiers

def sample_row(amount=85.0):
    return {"document_date":"2020-01-05","buyer_name":"SAKAE KYO PTE LTD","document_type":"CINV","reference_no":"INV-1001","seller_name":"YOCORN FOOD ENTERPRISE PTE LTD","product_category":"FOOD_PRODUCT","quantity":10.0,"item_description":"Nori Wrapper Green","invoice_year":2020,"unit_price":8.5,"amount":amount,"document_date_parsed":pd.Timestamp("2020-01-05"),"unit_price_recomputed":amount/10,"unit_price_variance":8.5-amount/10}

def test_deduplicate_keeps_higher_amount():
    kept, failed = deduplicate(pd.DataFrame([sample_row(80),sample_row(85)]))
    assert kept.iloc[0]["amount"]==85
    assert failed.iloc[0]["amount"]==80

def test_identifier_and_hash_cardinality():
    transformed=build_trade_identifier(pd.DataFrame([sample_row()]))
    hashed=hash_identifiers(transformed)
    assert transformed.iloc[0]["trade_identifier"].startswith("T001")
    assert transformed["trade_identifier"].nunique()==hashed["hashed_trade_identifier"].nunique()
