"""
Testy projektu zaliczeniowego M4 — ecommerce_daily_pipeline
===========================================================
Autor: Aleksander Ortonowski

1. Struktura DAG (DagBag) — import, schedule, task_ids, zależności, retry, SLA.
2. Logika biznesowa na małych DataFrame'ach — bez bazy danych:
   calculate_rfm_segment, compute_rfm, build_sales_daily, build_quality_report.

Uruchomienie (z katalogu zaliczenie/):
    pytest tests/ -v
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from airflow.models import DagBag

DAG_FOLDER = Path(__file__).resolve().parent.parent / "dags"
sys.path.insert(0, str(DAG_FOLDER))

import ecommerce_daily_pipeline as pipeline  # noqa: E402

DAG_ID = "ecommerce_daily_pipeline"
REQUIRED_TASK_IDS = {
    "check_group.check_orders_exist",
    "check_group.check_data_freshness",
    "extract_group.extract_orders",
    "extract_group.extract_order_items",
    "extract_group.extract_returns",
    "transform_group.transform_sales_daily",
    "transform_group.transform_rfm",
    "load_to_warehouse",
    "notify_pipeline_complete",
}
EXTRACT_TASKS = {t for t in REQUIRED_TASK_IDS if t.startswith("extract_group.")}


# ===========================================================================
# FIXTURES
# ===========================================================================

@pytest.fixture(scope="module")
def dagbag():
    """DagBag z folderu dags/ projektu (bez przykładów Airflow)."""
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    """Główny DAG projektu."""
    dag = dagbag.get_dag(DAG_ID)
    assert dag is not None
    return dag


# ===========================================================================
# 1. STRUKTURA DAG
# ===========================================================================

def test_dag_loads_without_import_errors(dagbag):
    """Wszystkie DAGi z folderu (też extra Databricks) ładują się bez błędów."""
    assert dagbag.import_errors == {}, dagbag.import_errors
    assert {DAG_ID, "ecommerce_databricks_transform"} <= set(dagbag.dag_ids)


def test_schedule(dag):
    """Codziennie o 06:00 UTC, bez catchup."""
    assert dag.schedule_interval == "0 6 * * *"
    assert dag.catchup is False


def test_task_count(dag):
    """Co najmniej 6 tasków."""
    assert len(dag.tasks) >= 6


def test_required_task_ids_present(dag):
    """Wszystkie wymagane task_ids istnieją."""
    missing = REQUIRED_TASK_IDS - set(dag.task_ids)
    assert not missing, f"Brakujące taski: {missing}"


def test_extract_tasks_run_in_parallel(dag):
    """Brak zależności pomiędzy taskami ekstrakcji; każdy zależy od obu checków."""
    checks = {"check_group.check_orders_exist", "check_group.check_data_freshness"}
    for task_id in EXTRACT_TASKS:
        task = dag.get_task(task_id)
        assert task.upstream_task_ids == checks
        assert not (task.downstream_task_ids & EXTRACT_TASKS)


def test_pipeline_order(dag):
    """check → extract → transform → load → notify (+ quality report przed notify)."""
    transforms = {"transform_group.transform_sales_daily", "transform_group.transform_rfm"}
    for task_id in transforms:
        assert dag.get_task(task_id).upstream_task_ids == EXTRACT_TASKS
    assert dag.get_task("load_to_warehouse").upstream_task_ids == transforms
    assert dag.get_task("generate_quality_report").upstream_task_ids == transforms
    assert dag.get_task("notify_pipeline_complete").upstream_task_ids == {
        "load_to_warehouse",
        "generate_quality_report",
    }


def test_retries_backoff_and_callback_on_every_task(dag):
    """Każdy task: retries=3, exponential backoff, on_failure_callback."""
    for task in dag.tasks:
        assert task.retries == 3, task.task_id
        assert task.retry_exponential_backoff is True, task.task_id
        assert task.on_failure_callback, task.task_id


def test_sla_on_critical_tasks(dag):
    """SLA na transformacjach i load_to_warehouse + callback SLA na DAG."""
    assert dag.get_task("transform_group.transform_sales_daily").sla == timedelta(hours=1)
    assert dag.get_task("transform_group.transform_rfm").sla == timedelta(hours=1)
    assert dag.get_task("load_to_warehouse").sla is not None
    assert dag.sla_miss_callback is not None


def test_no_print_in_dag_code():
    """Logowanie tylko przez logging."""
    for path in DAG_FOLDER.glob("*.py"):
        assert "print(" not in path.read_text(encoding="utf-8"), path.name


# ===========================================================================
# 2. LOGIKA BIZNESOWA
# ===========================================================================

@pytest.mark.parametrize(
    ("recency", "frequency", "monetary", "expected"),
    [
        (3, 12, 4500.0, "Champions"),
        (7, 5, 1000.0, "Champions"),            # granice włącznie
        (8, 5, 1000.0, "Loyal Customers"),      # recency > 7 → już nie Champion
        (5, 5, 999.99, "Loyal Customers"),      # za mało monetary na Championa
        (30, 3, 500.0, "Loyal Customers"),
        (145, 1, 49.0, "Lost"),
        (121, 10, 9000.0, "Lost"),              # Lost przed At Risk
        (120, 2, 100.0, "At Risk"),             # 120 to jeszcze nie Lost
        (75, 3, 890.0, "At Risk"),
        (61, 1, 50.0, "Others"),                # At Risk wymaga frequency >= 2
        (10, 1, 50.0, "New Customers"),
        (31, 1, 50.0, "Others"),
        (20, 2, 300.0, "Others"),
    ],
)
def test_calculate_rfm_segment(recency, frequency, monetary, expected):
    """Segmenty RFM zgodne ze specyfikacją (z przypadkami granicznymi)."""
    assert pipeline.calculate_rfm_segment(recency, frequency, monetary) == expected


def test_compute_rfm_columns_scores_and_segments():
    """compute_rfm: kolumny, recency, zakres score'ów 1–5, segmenty."""
    ref = date(2024, 1, 15)
    customers = pd.DataFrame(
        {
            "customer_id": [1, 2, 3, 4, 5, 6],
            "last_order_date": pd.to_datetime(
                ["2024-01-12 10:00", "2023-11-01 00:00", "2023-09-01 00:00",
                 "2024-01-10 00:00", "2024-01-14 23:59", "2023-12-01 00:00"]
            ),
            "frequency": [12, 3, 1, 1, 4, 2],
            "monetary": [4500.0, 890.0, 49.0, 120.0, 800.0, 300.0],
        }
    )
    rfm = pipeline.compute_rfm(customers, ref)

    assert list(rfm.columns) == pipeline.RFM_COLUMNS
    by_id = rfm.set_index("customer_id")
    assert by_id.loc[1, "recency_days"] == 3
    assert by_id.loc[5, "recency_days"] == 1          # liczymy dni kalendarzowe, nie godziny
    assert by_id.loc[1, "segment"] == "Champions"
    assert by_id.loc[2, "segment"] == "At Risk"
    assert by_id.loc[3, "segment"] == "Lost"
    assert by_id.loc[4, "segment"] == "New Customers"
    assert by_id.loc[5, "segment"] == "Loyal Customers"

    assert rfm["rfm_score"].str.fullmatch(r"[1-5]{3}").all()
    # Najlepszy klient: najświeższy z top-2 recency, top F i top M
    assert by_id.loc[1, "rfm_score"][1:] == "55"
    assert by_id.loc[5, "rfm_score"][0] == "5"        # najmniejsza recency → R=5


def test_compute_rfm_handles_small_and_empty_input():
    """Mniej niż 5 klientów i pusty wynik nie wywalają transformacji."""
    one = pd.DataFrame(
        {"customer_id": [7], "last_order_date": [pd.Timestamp("2024-01-14")], "frequency": [1], "monetary": [10]}
    )
    assert len(pipeline.compute_rfm(one, date(2024, 1, 15))) == 1

    empty = pipeline.compute_rfm(
        pd.DataFrame(columns=["customer_id", "last_order_date", "frequency", "monetary"]),
        date(2024, 1, 15),
    )
    assert empty.empty and list(empty.columns) == pipeline.RFM_COLUMNS


@pytest.fixture
def sample_day():
    """Dane jak w schema.sql (zamówienia 1–5 z 'wczoraj') + dodatkowy zwrot i refund."""
    orders = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5, 6],
            "customer_id": [1, 2, 3, 4, 5, 1],
            "order_date": pd.to_datetime(["2024-01-14"] * 6),
            "status": ["completed", "completed", "processing", "completed", "cancelled", "refunded"],
            "total_amount": [899.0, 189.5, 1250.0, 45.0, 350.0, 100.0],
            "customer_country": ["PL", "PL", "DE", "FR", "GB", "PL"],
        }
    )
    items = pd.DataFrame(
        {
            "item_id": [1, 2, 3, 4, 5, 6, 7, 8],
            "order_id": [1, 1, 1, 2, 3, 4, 5, 6],
            "product_id": [1, 3, 4, 7, 5, 10, 1, 8],
            "quantity": [1, 2, 1, 1, 1, 1, 1, 2],
            "unit_price": [549.0, 12.0, 35.0, 189.5, 1250.0, 45.0, 350.0, 50.0],
            "category": ["Elektronika", "Akcesoria", "Akcesoria", "Audio", "Komputery", "Gaming", "Elektronika", "Audio"],
        }
    )
    returns = pd.DataFrame(
        {"return_id": [1], "order_id": [2], "return_date": ["2024-01-15"], "reason": ["Uszkodzony"]}
    )
    return orders, items, returns


def test_build_sales_daily(sample_day):
    """Agregacja: kolumny, filtr cancelled, przychód tylko completed/processing, zwroty."""
    orders, items, returns = sample_day
    sales = pipeline.build_sales_daily(orders, items, returns, date(2024, 1, 14))

    assert list(sales.columns) == pipeline.SALES_COLUMNS
    assert (sales["date"] == "2024-01-14").all()
    assert "GB" not in set(sales["country"])                    # jedyne zamówienie GB anulowane

    # 5 nieanulowanych zamówień, każde liczone dokładnie raz
    assert sales["orders_count"].sum() == 5
    # refunded (100) liczy się do orders_count, ale nie do przychodu
    assert sales["gross_revenue"].sum() == pytest.approx(899.0 + 189.5 + 1250.0 + 45.0)

    row = sales.set_index(["category", "country"])
    assert row.loc[("Elektronika", "PL"), "gross_revenue"] == 899.0   # kategoria wg najdroższej pozycji
    assert row.loc[("Audio", "PL"), "orders_count"] == 2               # zamówienie 2 + refund 6
    assert row.loc[("Audio", "PL"), "returns_count"] == 1
    assert row.loc[("Audio", "PL"), "avg_order_value"] == pytest.approx(94.75)
    assert row.loc[("Komputery", "DE"), "avg_order_value"] == 1250.0
    assert sales["returns_count"].sum() == 1


def test_build_sales_daily_without_returns(sample_day):
    """Pusty CSV zwrotów (same nagłówki) → returns_count = 0, bez błędu."""
    orders, items, _ = sample_day
    empty_returns = pd.DataFrame(columns=pipeline.RETURNS_COLUMNS)
    sales = pipeline.build_sales_daily(orders, items, empty_returns, date(2024, 1, 14))
    assert sales["returns_count"].sum() == 0
    assert sales["returns_count"].dtype.kind == "i"


def test_build_quality_report(sample_day):
    """Raport jakości: wymagane pola i poprawne wartości."""
    orders, _, _ = sample_day
    orders.loc[0, "customer_id"] = None
    report = pipeline.build_quality_report(orders, date(2024, 1, 14))

    assert {
        "execution_date", "orders_processed", "null_customer_ids",
        "negative_amounts", "cancelled_rate", "data_quality_score",
    } <= report.keys()
    assert report["orders_processed"] == 6
    assert report["null_customer_ids"] == 1
    assert report["negative_amounts"] == 0
    assert report["cancelled_rate"] == pytest.approx(1 / 6, abs=1e-4)
    assert report["data_quality_score"] == pytest.approx(5 / 6, abs=1e-4)
