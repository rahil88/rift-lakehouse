import pytest

from consumer.bronze import BRONZE_TABLES, copy_into_statement, load_bronze, setup_statements
from consumer.databricks import DatabricksSettings
from consumer.upload import upload_landing

VOLUME = "/Volumes/workspace/rift_landing/files"


def test_setup_is_idempotent_ddl():
    statements = setup_statements("workspace")
    assert all("IF NOT EXISTS" in s for s in statements)
    assert "CREATE VOLUME IF NOT EXISTS workspace.rift_landing.files" in statements[1]
    assert len(statements) == 3 + len(BRONZE_TABLES)


def test_every_bronze_table_keeps_ingestion_metadata():
    for table in BRONZE_TABLES:
        assert "_ingested_at" in table.columns and "_source_file" in table.columns
        assert "_metadata.file_path" in table.select
    event_tables = [t for t in BRONZE_TABLES if t.name in ("matches", "timeline_events")]
    assert all("_schema_version" in t.columns and "payload" in t.columns for t in event_tables)


def test_copy_into_reads_the_landing_subdir_with_a_pattern():
    matches = next(t for t in BRONZE_TABLES if t.name == "matches")
    sql = copy_into_statement(matches, "workspace", VOLUME)
    assert sql.startswith("COPY INTO workspace.rift_bronze.matches")
    assert f"FROM '{VOLUME}/match'" in sql
    assert "PATTERN = 'ingest_date=*/*.jsonl'" in sql
    assert "FILEFORMAT = JSON" in sql


def test_load_skips_tables_without_files():
    executed = []

    def run_sql(sql):
        executed.append(sql)
        return [{"num_affected_rows": "3", "num_inserted_rows": "3"}]

    results = load_bronze(run_sql, lambda path: path.endswith("/match"), "workspace", VOLUME)
    assert results["matches"] == {"num_affected_rows": "3", "num_inserted_rows": "3"}
    assert results["timeline_events"] == {"skipped": "no files"}
    assert len(executed) == 1


def test_upload_moves_files_to_landed_after_success(tmp_path):
    landing, landed = tmp_path / "landing", tmp_path / "landed"
    f = landing / "match" / "ingest_date=2026-11-11" / "x.jsonl"
    f.parent.mkdir(parents=True)
    f.write_text('{"a":1}\n')

    class FakeFiles:
        uploads = []

        def upload(self, path, fh, overwrite):
            self.uploads.append((path, fh.read(), overwrite))

    files = FakeFiles()
    report = upload_landing(files, landing, landed, VOLUME)
    assert files.uploads == [(f"{VOLUME}/match/ingest_date=2026-11-11/x.jsonl", b'{"a":1}\n', True)]
    assert not f.exists() and (landed / "match" / "ingest_date=2026-11-11" / "x.jsonl").exists()
    assert report.files == 1
    assert upload_landing(files, landing, landed, VOLUME).files == 0  # nothing left to upload


def test_upload_failure_leaves_file_in_landing(tmp_path):
    landing = tmp_path / "landing"
    (landing / "match").mkdir(parents=True)
    (landing / "match" / "x.jsonl").write_text("{}\n")

    class Failing:
        def upload(self, path, fh, overwrite):
            raise ConnectionError("network")

    with pytest.raises(ConnectionError):
        upload_landing(Failing(), landing, tmp_path / "landed", VOLUME)
    assert (landing / "match" / "x.jsonl").exists()  # retried next run


def test_settings_parse_warehouse_and_hide_token():
    s = DatabricksSettings.from_env(
        {
            "DATABRICKS_HOST": "https://dbc-1.cloud.databricks.com/",
            "DATABRICKS_TOKEN": "dapi-secret",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc123",
        }
    )
    assert s.warehouse_id == "abc123"
    assert s.volume_root == VOLUME
    assert "dapi-secret" not in repr(s)


def test_ddl_comments_are_escaped():
    """Regression: "producer's" in the DLQ table comment broke the CREATE TABLE statement."""
    from consumer.databricks import sql_string

    assert sql_string("producer's \\ x") == "'producer\\'s \\\\ x'"
    dlq = next(s for s in setup_statements("workspace") if ".dlq_events " in s)
    assert "producer\\'s contract check" in dlq


def test_upload_skips_file_taken_by_concurrent_uploader(tmp_path):
    """Regression: a backfill run and a scheduled run uploaded at once; one crashed with
    FileNotFoundError when the other archived the file first."""
    landing, landed = tmp_path / "landing", tmp_path / "landed"
    for name in ("a.jsonl", "b.jsonl"):
        (landing / "match").mkdir(parents=True, exist_ok=True)
        (landing / "match" / name).write_text("{}\n")

    class RacingFiles:
        def upload(self, path, fh, overwrite):
            if path.endswith("a.jsonl"):  # the other uploader archives b while we upload a
                (landing / "match" / "b.jsonl").unlink()

    report = upload_landing(RacingFiles(), landing, landed, VOLUME)
    assert (report.files, report.skipped) == (1, 1)
