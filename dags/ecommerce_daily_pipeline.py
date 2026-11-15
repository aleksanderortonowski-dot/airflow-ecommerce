"""
PROJEKT ZALICZENIOWY — Miesiąc 4: DataWolves Shop ETL
=====================================================
Autor: Aleksander Ortonowski

Produkcyjny pipeline e-commerce uruchamiany codziennie o 06:00 UTC.
Przetwarza dane transakcyjne z poprzedniego dnia (execution_date - 1 dzień).

    check_group      ─► extract_group (równolegle)       ─► transform_group      ─► load_to_warehouse ─► notify
      check_orders_exist   extract_orders                      transform_sales_daily                     ▲
      check_data_freshness extract_order_items                 transform_rfm                             │
                           extract_returns                         └─► generate_quality_report ──────────┘

Wyniki:
    {DATA_PATH}/raw/{orders,order_items,returns}/dt=YYYY-MM-DD/*.csv
    {DATA_PATH}/processed/sales_daily/dt=YYYY-MM-DD/sales.parquet
    {DATA_PATH}/processed/rfm/dt=YYYY-MM-DD/rfm.parquet
    {DATA_PATH}/quality/dt=YYYY-MM-DD/report.json
    {DATA_PATH}/warehouse/{sales_daily,rfm}/dt=YYYY-MM-DD/*.parquet

Logika biznesowa (agregacja sprzedaży, RFM, raport jakości) jest w czystych
funkcjach na DataFrame'ach (`build_sales_daily`, `compute_rfm`,
`build_quality_report`). Callable'e tasków robią tylko I/O, więc logikę da się
testować bez bazy danych (patrz tests/test_dag.py).

Jak uruchomić:
    cp dags/ecommerce_daily_pipeline.py ../../miesiac_04_airflow/dags/
    UI → ecommerce_daily_pipeline → toggle ON → Trigger DAG
"""

import json
import logging
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ===========================================================================
# KONFIGURACJA — Airflow Variables (Admin → Variables), bez hardkodowania
# ===========================================================================
DATA_PATH = Variable.get("DATA_PATH", default_var="/opt/airflow/data")
MIN_ORDERS = int(Variable.get("MIN_ORDERS_THRESHOLD", default_var="10"))
SLACK_CHANNEL = Variable.get("SLACK_CHANNEL", default_var="#data-alerts-test")
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_ecommerce")
SLACK_CONN_ID = Variable.get("SLACK_CONN_ID", default_var="slack_default")

MAX_DATA_AGE_DAYS = 2      # check_data_freshness: dane starsze niż 2 dni → błąd
RFM_WINDOW_DAYS = 365      # okno RFM
REVENUE_STATUSES = ("completed", "processing")

ORDERS_COLUMNS = ["order_id", "customer_id", "order_date", "status", "total_amount", "customer_country"]
ITEMS_COLUMNS = ["item_id", "order_id", "product_id", "quantity", "unit_price", "category"]
RETURNS_COLUMNS = ["return_id", "order_id", "return_date", "reason"]
SALES_COLUMNS = [
    "date", "category", "country", "orders_count",
    "gross_revenue", "avg_order_value", "returns_count",
]
RFM_COLUMNS = ["customer_id", "recency_days", "frequency", "monetary", "rfm_score", "segment"]


# ===========================================================================
# HELPERY
# ===========================================================================

def _target_date(execution_date: datetime) -> date:
    """Zwraca datę przetwarzanych danych — dzień przed execution_date."""
    return execution_date.date() - timedelta(days=1)


def _partition(layer: str, dataset: str, day: date) -> Path:
    """Buduje ścieżkę partycji: {DATA_PATH}/{layer}/{dataset}/dt={day}."""
    return Path(DATA_PATH) / layer / dataset / f"dt={day}"


def _send_slack(text: str) -> None:
    """
    Wysyła wiadomość na Slack przez SlackWebhookHook.

    Błąd Slacka (np. mockowy webhook w środowisku lokalnym) jest tylko logowany —
    nie może wywalić taska ani przykryć oryginalnego błędu w callbacku.
    """
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send(text=text, channel=SLACK_CHANNEL)
        log.info("Wysłano wiadomość Slack na %s", SLACK_CHANNEL)
    except Exception as exc:  # noqa: BLE001
        log.warning("Nie udało się wysłać wiadomości Slack: %s", exc)


# ===========================================================================
# CALLBACKI
# ===========================================================================

def on_failure_alert(context: dict[str, Any]) -> None:
    """
    Callback wywoływany, gdy task ostatecznie się nie udał (po wszystkich retry).

    Loguje dag_id, task_id, datę wykonania, błąd i link do logów,
    a następnie próbuje wysłać alert na Slack.
    """
    dag_id = context["dag"].dag_id
    ti = context["task_instance"]
    execution_date = context.get("execution_date")
    exception = str(context.get("exception", "Nieznany błąd"))[:300]

    log.error(
        "FAILURE | DAG: %s | Task: %s | Date: %s | Error: %s | Logs: %s",
        dag_id, ti.task_id, execution_date, exception, ti.log_url,
    )
    _send_slack(
        f"❌ *Pipeline FAILED*\n*DAG:* `{dag_id}`\n*Task:* `{ti.task_id}`\n"
        f"*Data:* `{execution_date}`\n*Błąd:* `{exception}`\n*Logi:* {ti.log_url}"
    )


def on_sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Callback przekroczenia SLA (ustawiany na poziomie DAG) — log + Slack."""
    log.warning("SLA MISS | DAG: %s | Taski: %s", dag.dag_id, task_list)
    _send_slack(f"⚠️ *SLA MISS* w `{dag.dag_id}`: {task_list}. Pipeline działa, ale jest spóźniony.")


# ===========================================================================
# CHECK GROUP
# ===========================================================================

def check_orders_exist(execution_date: datetime, **context) -> int:
    """
    Sprawdza czy w tabeli orders istnieją dane za poprzedni dzień.

    COUNT(*) == 0       → AirflowSkipException (brak pracy, to nie awaria),
    COUNT(*) < MIN_ORDERS → ostrzeżenie w logu, ale pipeline działa dalej,
    w przeciwnym razie → log z liczbą zamówień.
    Zwraca liczbę zamówień (XCom).
    """
    target_date = _target_date(execution_date)
    log.info("Sprawdzam dane dla daty: %s", target_date)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    result = hook.get_first(
        "SELECT COUNT(*) FROM orders WHERE DATE(order_date) = %(target_date)s",
        parameters={"target_date": target_date},
    )
    count = int(result[0]) if result and result[0] is not None else 0

    if count == 0:
        raise AirflowSkipException(f"Brak zamówień na {target_date} — pomijam pipeline.")

    if count < MIN_ORDERS:
        log.warning(
            "Niski wolumen: %d zamówień dla %s (próg MIN_ORDERS_THRESHOLD=%d)",
            count, target_date, MIN_ORDERS,
        )
    log.info("Znaleziono %d zamówień dla daty %s", count, target_date)
    return count


def check_data_freshness(execution_date: datetime, **context) -> str:
    """
    Sprawdza czy dane w bazie nie są starsze niż 2 dni.

    Pobiera MAX(order_date) z tabeli orders. Jeśli najświeższy rekord jest starszy
    niż execution_date - 2 dni (albo tabela jest pusta) → ValueError.
    Zwraca najświeższą datę jako string (XCom).
    """
    reference_date = execution_date.date()
    log.info("Sprawdzam świeżość danych. Reference date: %s", reference_date)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    result = hook.get_first("SELECT MAX(order_date) FROM orders")

    if not result or result[0] is None:
        raise ValueError("Tabela orders jest pusta — brak jakichkolwiek danych.")

    max_date = pd.Timestamp(result[0]).date()
    age_days = (reference_date - max_date).days

    if max_date < reference_date - timedelta(days=MAX_DATA_AGE_DAYS):
        raise ValueError(
            f"Dane starsze niż {MAX_DATA_AGE_DAYS} dni: najświeższe zamówienie z {max_date} "
            f"({age_days} dni przed {reference_date}). Sprawdź proces zasilania bazy."
        )

    log.info("Dane świeże — najnowsze zamówienie: %s (%d dni temu)", max_date, age_days)
    return str(max_date)


# ===========================================================================
# EXTRACT GROUP — trzy taski równoległe
# ===========================================================================

def _extract_to_csv(sql: str, target_date: date, output_path: Path, columns: list[str]) -> str:
    """
    Wykonuje parametryzowane zapytanie i zapisuje wynik do CSV.

    Pusty wynik → CSV z samymi nagłówkami (kolejne taski nie muszą obsługiwać
    brakującego pliku). Zwraca ścieżkę do pliku.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = hook.get_pandas_df(sql, parameters={"target_date": target_date})
    if df.empty:
        df = pd.DataFrame(columns=columns)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    log.info("Zapisano %d rekordów → %s", len(df), output_path)
    return str(output_path)


def extract_orders(execution_date: datetime, **context) -> str:
    """Pobiera zamówienia z poprzedniego dnia (z krajem klienta) i zapisuje jako CSV."""
    target_date = _target_date(execution_date)
    log.info("Ekstrakcja zamówień dla daty: %s", target_date)
    sql = """
        SELECT
            o.id            AS order_id,
            o.customer_id,
            o.order_date,
            o.status,
            o.total_amount,
            c.country       AS customer_country
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE DATE(o.order_date) = %(target_date)s
    """
    output_path = _partition("raw", "orders", target_date) / "orders.csv"
    return _extract_to_csv(sql, target_date, output_path, ORDERS_COLUMNS)


def extract_order_items(execution_date: datetime, **context) -> str:
    """Pobiera pozycje zamówień z poprzedniego dnia (z kategorią produktu) i zapisuje jako CSV."""
    target_date = _target_date(execution_date)
    log.info("Ekstrakcja pozycji zamówień dla daty: %s", target_date)
    sql = """
        SELECT
            oi.id           AS item_id,
            oi.order_id,
            oi.product_id,
            oi.quantity,
            oi.unit_price,
            p.category
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.id
        JOIN products p ON oi.product_id = p.id
        WHERE DATE(o.order_date) = %(target_date)s
    """
    output_path = _partition("raw", "order_items", target_date) / "items.csv"
    return _extract_to_csv(sql, target_date, output_path, ITEMS_COLUMNS)


def extract_returns(execution_date: datetime, **context) -> str:
    """
    Pobiera zwroty dla zamówień z poprzedniego dnia i zapisuje jako CSV.

    Brak zwrotów to normalna sytuacja → pusty CSV z nagłówkami, bez błędu.
    """
    target_date = _target_date(execution_date)
    log.info("Ekstrakcja zwrotów dla daty: %s", target_date)
    sql = """
        SELECT
            r.id            AS return_id,
            r.order_id,
            r.return_date,
            r.reason
        FROM returns r
        JOIN orders o ON r.order_id = o.id
        WHERE DATE(o.order_date) = %(target_date)s
    """
    output_path = _partition("raw", "returns", target_date) / "returns.csv"
    return _extract_to_csv(sql, target_date, output_path, RETURNS_COLUMNS)


# ===========================================================================
# LOGIKA BIZNESOWA — czyste funkcje (testowalne bez Airflow/bazy)
# ===========================================================================

def build_sales_daily(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    returns: pd.DataFrame,
    target_date: date,
) -> pd.DataFrame:
    """
    Agreguje sprzedaż dzienną per kategoria produktu i kraj klienta.

    Reguły:
        - zamówienia "cancelled" są odfiltrowane przed agregacją,
        - kategoria zamówienia = kategoria pozycji o najwyższej wartości
          (quantity * unit_price), żeby każde zamówienie było liczone raz
          i przychód się nie dublował; zamówienia bez pozycji → "Nieznana",
        - gross_revenue = suma total_amount tylko dla statusów completed / processing,
        - orders_count = liczba unikalnych zamówień (bez anulowanych),
        - avg_order_value = gross_revenue / orders_count (2 miejsca po przecinku),
        - returns_count = liczba zwrotów zamówień z danej grupy.
    """
    valid = orders.loc[orders["status"] != "cancelled"].copy()
    valid["order_id"] = valid["order_id"].astype("int64")
    valid["total_amount"] = valid["total_amount"].astype(float)

    items = items.copy()
    items["order_id"] = items["order_id"].astype("int64")
    items["line_value"] = items["quantity"].astype(float) * items["unit_price"].astype(float)
    main_category = (
        items.sort_values(["order_id", "line_value"], ascending=[True, False])
        .drop_duplicates("order_id")[["order_id", "category"]]
    )

    returns_per_order = (
        returns.assign(order_id=returns["order_id"].astype("int64"))
        .groupby("order_id")
        .size()
        .rename("returns")
        .reset_index()
    )

    valid = valid.merge(main_category, on="order_id", how="left")
    valid["category"] = valid["category"].fillna("Nieznana")
    valid = valid.merge(returns_per_order, on="order_id", how="left")
    valid["returns"] = valid["returns"].fillna(0).astype(int)
    valid["revenue"] = valid["total_amount"].where(valid["status"].isin(REVENUE_STATUSES), 0.0)

    sales = (
        valid.groupby(["category", "customer_country"], as_index=False)
        .agg(
            orders_count=("order_id", "nunique"),
            gross_revenue=("revenue", "sum"),
            returns_count=("returns", "sum"),
        )
        .rename(columns={"customer_country": "country"})
    )
    sales["orders_count"] = sales["orders_count"].astype(int)
    sales["returns_count"] = sales["returns_count"].astype(int)
    sales["gross_revenue"] = sales["gross_revenue"].astype(float).round(2)
    sales["avg_order_value"] = (sales["gross_revenue"] / sales["orders_count"]).round(2)
    sales["date"] = str(target_date)

    return (
        sales[SALES_COLUMNS]
        .sort_values(["gross_revenue", "category", "country"], ascending=[False, True, True])
        .reset_index(drop=True)
    )


def calculate_rfm_segment(recency_days: int, frequency: int, monetary: float) -> str:
    """
    Oblicza segment RFM na podstawie trzech metryk.

    Kolejność warunków ma znaczenie — sprawdzamy od najbardziej wartościowych:
        Champions:       recency <= 7,   frequency >= 5, monetary >= 1000
        Loyal Customers: recency <= 30,  frequency >= 3, monetary >= 500
        Lost:            recency > 120
        At Risk:         recency > 60,   frequency >= 2
        New Customers:   frequency == 1, recency <= 30
        Others:          wszystko inne
    """
    if recency_days <= 7 and frequency >= 5 and monetary >= 1000:
        return "Champions"
    if recency_days <= 30 and frequency >= 3 and monetary >= 500:
        return "Loyal Customers"
    if recency_days > 120:
        return "Lost"
    if recency_days > 60 and frequency >= 2:
        return "At Risk"
    if frequency == 1 and recency_days <= 30:
        return "New Customers"
    return "Others"


def quintile_score(values: pd.Series, higher_is_better: bool = True) -> pd.Series:
    """
    Zamienia metrykę na score 1–5 wg kwintyli rangi.

    Używamy rank(method="first", pct=True) zamiast pd.qcut — działa dla dowolnej
    liczby klientów (także < 5) i przy wielu remisach (np. frequency == 1),
    gdzie qcut wywala się na nieunikalnych granicach przedziałów.
    """
    if values.empty:
        return pd.Series(dtype=int, index=values.index)
    ranked = values.rank(method="first", ascending=higher_is_better, pct=True)
    return np.ceil(ranked * 5).clip(1, 5).astype(int)


def compute_rfm(customers: pd.DataFrame, reference_date: date) -> pd.DataFrame:
    """
    Liczy RFM dla klientów z kolumnami: customer_id, last_order_date, frequency, monetary.

    recency_days = (reference_date - last_order_date.date()).days
    rfm_score    = "RFM", np. "524" — R: mniejsza recency → wyższy score,
                   F i M: wyższa wartość → wyższy score.
    segment      = calculate_rfm_segment(recency_days, frequency, monetary)
    """
    if customers.empty:
        return pd.DataFrame(columns=RFM_COLUMNS)

    df = customers.copy()
    df["frequency"] = df["frequency"].astype(int)
    df["monetary"] = df["monetary"].astype(float).round(2)
    last_day = pd.to_datetime(df["last_order_date"]).dt.normalize()
    df["recency_days"] = (pd.Timestamp(reference_date) - last_day).dt.days.clip(lower=0).astype(int)

    r_score = quintile_score(df["recency_days"], higher_is_better=False)
    f_score = quintile_score(df["frequency"])
    m_score = quintile_score(df["monetary"])
    df["rfm_score"] = r_score.astype(str) + f_score.astype(str) + m_score.astype(str)

    df["segment"] = [
        calculate_rfm_segment(r, f, m)
        for r, f, m in zip(df["recency_days"], df["frequency"], df["monetary"])
    ]
    return df[RFM_COLUMNS].sort_values("customer_id").reset_index(drop=True)


def build_quality_report(orders: pd.DataFrame, target_date: date) -> dict[str, Any]:
    """
    Buduje raport jakości danych dla surowych zamówień z danego dnia.

    data_quality_score = odsetek rekordów bez problemów w kluczowych polach
    (brak NULL w order_id / customer_id / total_amount / customer_country
    oraz nieujemna kwota).
    """
    total = len(orders)
    key_fields = ["order_id", "customer_id", "total_amount", "customer_country"]
    null_customer_ids = int(orders["customer_id"].isna().sum())
    amounts = pd.to_numeric(orders["total_amount"], errors="coerce")
    negative_amounts = int((amounts < 0).sum())
    cancelled = int((orders["status"] == "cancelled").sum())

    bad_rows = orders[key_fields].isna().any(axis=1) | (amounts < 0)
    quality_score = 1.0 if total == 0 else 1 - bad_rows.sum() / total

    return {
        "execution_date": str(target_date),
        "orders_processed": total,
        "null_customer_ids": null_customer_ids,
        "null_key_fields": int(orders[key_fields].isna().sum().sum()),
        "negative_amounts": negative_amounts,
        "cancelled_rate": round(cancelled / total, 4) if total else 0.0,
        "data_quality_score": round(float(quality_score), 4),
    }


# ===========================================================================
# TRANSFORM GROUP
# ===========================================================================

def transform_sales_daily(execution_date: datetime, **context) -> str:
    """
    Agregacja sprzedaży dziennej per kategoria produktu i kraj klienta.

    Wejście: ścieżki CSV z XCom tasków extract_group.
    Wyjście: {DATA_PATH}/processed/sales_daily/dt={date}/sales.parquet (ścieżka → XCom).
    """
    target_date = _target_date(execution_date)
    ti = context["task_instance"]

    orders_path = ti.xcom_pull(task_ids="extract_group.extract_orders")
    items_path = ti.xcom_pull(task_ids="extract_group.extract_order_items")
    returns_path = ti.xcom_pull(task_ids="extract_group.extract_returns")
    log.info("Transformacja sprzedaży dla daty: %s", target_date)

    orders = pd.read_csv(orders_path, parse_dates=["order_date"])
    items = pd.read_csv(items_path)
    returns = pd.read_csv(returns_path)
    log.info("Wejście: %d zamówień, %d pozycji, %d zwrotów", len(orders), len(items), len(returns))

    sales = build_sales_daily(orders, items, returns, target_date)

    output_path = _partition("processed", "sales_daily", target_date) / "sales.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sales.to_parquet(output_path, index=False)

    log.info(
        "sales_daily: %d grup, %d zamówień, przychód %.2f PLN → %s",
        len(sales), sales["orders_count"].sum(), sales["gross_revenue"].sum(), output_path,
    )
    return str(output_path)


def transform_rfm(execution_date: datetime, **context) -> str:
    """
    Segmentacja RFM klientów aktywnych w ostatnich 365 dniach.

    Reference date = execution_date.date(). Brane są zamówienia completed/processing
    z okna [reference_date - 365 dni, reference_date] — górna granica chroni
    przed ujemną recency przy re-runach historycznych dat.
    Wyjście: {DATA_PATH}/processed/rfm/dt={reference_date}/rfm.parquet (ścieżka → XCom).
    """
    reference_date = execution_date.date()
    start_date = reference_date - timedelta(days=RFM_WINDOW_DAYS)
    end_date = reference_date + timedelta(days=1)
    log.info("RFM: reference_date=%s, start_date=%s", reference_date, start_date)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    customers = hook.get_pandas_df(
        """
        SELECT
            o.customer_id,
            MAX(o.order_date)   AS last_order_date,
            COUNT(o.id)         AS frequency,
            SUM(o.total_amount) AS monetary
        FROM orders o
        WHERE o.status IN ('completed', 'processing')
          AND o.order_date >= %(start_date)s
          AND o.order_date <  %(end_date)s
        GROUP BY o.customer_id
        """,
        parameters={"start_date": start_date, "end_date": end_date},
    )
    log.info("Pobrano %d aktywnych klientów", len(customers))

    rfm = compute_rfm(customers, reference_date)

    output_path = _partition("processed", "rfm", reference_date) / "rfm.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rfm.to_parquet(output_path, index=False)

    log.info("Rozkład segmentów RFM:\n%s", rfm["segment"].value_counts().to_string())
    log.info("Zapisano RFM (%d klientów) → %s", len(rfm), output_path)
    return str(output_path)


def generate_quality_report(execution_date: datetime, **context) -> str:
    """
    Generuje raport jakości danych (JSON) dla surowych zamówień z dnia.

    Wyjście: {DATA_PATH}/quality/dt={date}/report.json (ścieżka → XCom).
    """
    target_date = _target_date(execution_date)
    ti = context["task_instance"]
    orders = pd.read_csv(ti.xcom_pull(task_ids="extract_group.extract_orders"))

    report = build_quality_report(orders, target_date)

    output_path = Path(DATA_PATH) / "quality" / f"dt={target_date}" / "report.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    log.info("Raport jakości: %s → %s", report, output_path)
    return str(output_path)


# ===========================================================================
# LOAD & NOTIFY
# ===========================================================================

def load_to_warehouse(execution_date: datetime, **context) -> dict[str, int]:
    """
    Kopiuje przetworzone pliki Parquet do katalogu warehouse (symulacja DWH).

    Kopiuje:
        sales.parquet → {DATA_PATH}/warehouse/sales_daily/dt={date}/
        rfm.parquet   → {DATA_PATH}/warehouse/rfm/dt={date}/
    i weryfikuje liczbę rekordów po załadowaniu. Zwraca {dataset: liczba_rekordów}.
    """
    target_date = _target_date(execution_date)
    ti = context["task_instance"]
    sources = {
        "sales_daily": ti.xcom_pull(task_ids="transform_group.transform_sales_daily"),
        "rfm": ti.xcom_pull(task_ids="transform_group.transform_rfm"),
    }
    log.info("Ładuję dane do warehouse dla daty: %s", target_date)

    loaded = {}
    for dataset, src in sources.items():
        src_path = Path(src)
        dst_dir = _partition("warehouse", dataset, target_date)
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst_path = dst_dir / src_path.name
        shutil.copy2(src_path, dst_path)

        rows = len(pd.read_parquet(dst_path))
        if rows != len(pd.read_parquet(src_path)):
            raise ValueError(f"Niezgodna liczba rekordów po kopiowaniu {dataset}: {dst_path}")
        loaded[dataset] = rows
        log.info("Załadowano %s: %d rekordów → %s", dataset, rows, dst_path)

    return loaded


def notify_pipeline_complete(execution_date: datetime, **context) -> str:
    """
    Wysyła podsumowanie zakończonego pipeline'u (log + opcjonalnie Slack).

    Zawartość: data przetwarzania, liczba zamówień, przychód, zwroty,
    top kategoria, segmenty RFM, wynik jakości danych, status SUCCESS ✅.
    """
    target_date = _target_date(execution_date)
    ti = context["task_instance"]

    sales = pd.read_parquet(ti.xcom_pull(task_ids="transform_group.transform_sales_daily"))
    rfm = pd.read_parquet(ti.xcom_pull(task_ids="transform_group.transform_rfm"))
    quality_path = ti.xcom_pull(task_ids="generate_quality_report")
    quality = json.loads(Path(quality_path).read_text(encoding="utf-8")) if quality_path else {}

    orders_count = int(sales["orders_count"].sum())
    revenue = float(sales["gross_revenue"].sum())
    returns_count = int(sales["returns_count"].sum())
    returns_rate = returns_count / orders_count * 100 if orders_count else 0.0
    if sales.empty:
        top_category, top_revenue = "—", 0.0
    else:
        by_category = sales.groupby("category")["gross_revenue"].sum()
        top_category, top_revenue = by_category.idxmax(), float(by_category.max())
    segments = rfm["segment"].value_counts()

    lines = [
        "✅ *Pipeline zakończony sukcesem* — status: SUCCESS",
        f"📅 Data: {target_date}",
        f"📦 Zamówienia: {orders_count}",
        f"💰 Przychód: {revenue:,.2f} PLN",
        f"⚠️ Zwroty: {returns_count} ({returns_rate:.1f}%)",
        f"🏆 Top kategoria: {top_category} ({top_revenue:,.2f} PLN)",
        f"👥 Segmenty RFM ({segments.size}): "
        + ", ".join(f"{name}={cnt}" for name, cnt in segments.items()),
        f"🧪 Data quality score: {quality.get('data_quality_score', 'n/a')}",
    ]
    message = "\n".join(lines)

    log.info("=" * 60)
    for line in lines:
        log.info(line)
    log.info("=" * 60)

    _send_slack(message)
    return message


# ===========================================================================
# DEFINICJA DAG — dag_id, schedule_interval i task_ids zgodne ze specyfikacją
# ===========================================================================

DEFAULT_ARGS = {
    "owner": "aleksander-ortonowski",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
    "on_failure_callback": on_failure_alert,
}

with DAG(
    dag_id="ecommerce_daily_pipeline",
    description="Projekt zaliczeniowy M4 — DataWolves Shop ETL",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=on_sla_miss_alert,
    tags=["ecommerce", "zaliczenie", "miesiac-4"],
) as dag:

    with TaskGroup("check_group") as check_group:
        check_orders = PythonOperator(
            task_id="check_orders_exist",
            python_callable=check_orders_exist,
        )
        check_freshness = PythonOperator(
            task_id="check_data_freshness",
            python_callable=check_data_freshness,
        )

    with TaskGroup("extract_group") as extract_group:
        ext_orders = PythonOperator(
            task_id="extract_orders",
            python_callable=extract_orders,
        )
        ext_items = PythonOperator(
            task_id="extract_order_items",
            python_callable=extract_order_items,
        )
        ext_returns = PythonOperator(
            task_id="extract_returns",
            python_callable=extract_returns,
        )

    with TaskGroup("transform_group") as transform_group:
        tr_sales = PythonOperator(
            task_id="transform_sales_daily",
            python_callable=transform_sales_daily,
            sla=timedelta(hours=1),
        )
        tr_rfm = PythonOperator(
            task_id="transform_rfm",
            python_callable=transform_rfm,
            sla=timedelta(hours=1),
        )

    quality_task = PythonOperator(
        task_id="generate_quality_report",
        python_callable=generate_quality_report,
    )

    load_task = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
        sla=timedelta(hours=1, minutes=30),
    )

    notify_task = PythonOperator(
        task_id="notify_pipeline_complete",
        python_callable=notify_pipeline_complete,
    )

    # Główny łańcuch (zgodny ze starterem)
    check_group >> extract_group >> transform_group >> load_task >> notify_task
    # Extra C: raport jakości po transformacji, przed powiadomieniem
    transform_group >> quality_task >> notify_task
