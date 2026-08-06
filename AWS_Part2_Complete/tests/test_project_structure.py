from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_expected_files_exist():
    expected = [
        "infrastructure/foundation.yaml",
        "infrastructure/ingestion.yaml",
        "infrastructure/exploitation.yaml",
        "ingestion/downloader/app.py",
        "ingestion/glue/trade_invoice_etl.py",
        "ingestion/glue/sql/01_staging.sql",
        "ingestion/glue/sql/09_review.sql",
        "exploitation/athena/sample_queries.sql",
    ]
    for path in expected:
        assert (ROOT / path).exists(), path


def test_glue_sql_is_split_into_small_steps():
    sql_dir = ROOT / "ingestion" / "glue" / "sql"
    names = sorted(p.name for p in sql_dir.glob("*.sql"))
    assert len(names) == 9
    assert names[0] == "01_staging.sql"
    assert names[-1] == "09_review.sql"
