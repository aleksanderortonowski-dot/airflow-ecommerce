# DataWolves Shop: `ecommerce_daily_pipeline`

Projekt zaliczeniowy, Miesiąc 4 (Apache Airflow) · Aleksander Ortonowski

Produkcyjny DAG, który codziennie o **06:00 UTC** przetwarza dane e-commerce z poprzedniego dnia. Wynikiem są agregacja sprzedaży, segmentacja RFM, raport jakości danych i podsumowanie (log + Slack).

```
check_group ──► extract_group ──────────► transform_group ─────────► load_to_warehouse ──► notify_pipeline_complete
  check_orders_exist     extract_orders          transform_sales_daily                           ▲
  check_data_freshness   extract_order_items     transform_rfm                                   │
                         extract_returns              └──► generate_quality_report ──────────────┘
```

## Struktura

```
zaliczenie/
├── README.md
├── dags/
│   ├── ecommerce_daily_pipeline.py        ← główny DAG (10 tasków)
│   └── ecommerce_databricks_transform.py  ← extra B: DatabricksSubmitRunOperator (stub, schedule=None)
└── tests/
    └── test_dag.py                        ← 27 testów: struktura DAG + logika biznesowa
```

## Uruchomienie

```bash
cd miesiac_04_airflow
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init && docker compose up -d
cp ../airflow_zadania_domowe/zaliczenie/dags/*.py dags/
```

W UI (http://localhost:8080, `airflow`/`airflow`):

1. **Connections**: `postgres_ecommerce` (Postgres, host `postgres`, schema `ecommerce`, `airflow`/`airflow`, port 5432). Opcjonalnie `slack_default`. Gdy go brak, powiadomienie zostanie tylko zalogowane.
2. **Variables**: *Import Variables* z pliku `miesiac_04_airflow/variables.json`.
3. `ecommerce_daily_pipeline`: toggle ON, potem **Trigger DAG**.

Wyniki znajdziesz w `miesiac_04_airflow/data/`:

| Warstwa | Ścieżka |
|---------|---------|
| raw | `raw/{orders,order_items,returns}/dt=YYYY-MM-DD/*.csv` |
| processed | `processed/sales_daily/dt=…/sales.parquet`, `processed/rfm/dt=…/rfm.parquet` |
| quality | `quality/dt=…/report.json` |
| warehouse | `warehouse/{sales_daily,rfm}/dt=…/*.parquet` |

## Testy

```bash
pip install -r ../miesiac_04_airflow/requirements.txt
cd airflow_zadania_domowe/zaliczenie
airflow db migrate          # jednorazowo, bo Variables w DAG potrzebują metadanych
pytest tests/ -v
```

## Decyzje projektowe

- **`check_orders_exist`**: przy `COUNT == 0` rzuca `AirflowSkipException`. Gdy zamówień jest mniej niż `MIN_ORDERS_THRESHOLD`, w logu pojawia się tylko ostrzeżenie, bo dane startowe mają około 5 zamówień dziennie.
- **`check_data_freshness`**: rzuca `ValueError`, gdy `MAX(order_date)` jest starsze niż `execution_date − 2 dni` albo tabela jest pusta.
- **Kategoria zamówienia w `sales_daily`**: to kategoria najdroższej pozycji. Dzięki temu każde zamówienie i jego `total_amount` liczą się dokładnie raz, a przychód się nie dubluje.
- **`gross_revenue`**: liczony tylko ze statusów `completed` i `processing`. Zamówienia `refunded` wliczają się do `orders_count`, ale nie do przychodu. `cancelled` są odfiltrowane.
- **`rfm_score`**: kwintyle rangi (`rank(pct=True)`) zamiast `pd.qcut`. Działa dla każdej liczby klientów i przy remisach, np. wielu klientach z `frequency == 1`.
- **Okno RFM**: ma też górną granicę `< reference_date + 1`, więc re-run historycznej daty nie daje ujemnej recency.
- **Logika w czystych funkcjach**: `build_sales_daily`, `compute_rfm` i `build_quality_report` działają na DataFrame'ach i są testowane bez bazy.
- **Błędy**: `retries=3` z exponential backoff (max 1h), `on_failure_callback` (log + Slack), `sla_miss_callback` oraz SLA na transformacjach (1h) i load (1h30).

## Extra

| Extra | Status |
|-------|--------|
| A: testy pytest | ✅ `tests/test_dag.py` |
| B: DatabricksSubmitRunOperator | ✅ osobny DAG `ecommerce_databricks_transform` (stub, wymaga `databricks_default`) |
| C: Data Quality Report | ✅ task `generate_quality_report` → `quality/dt=…/report.json` |
| D: ExternalTaskSensor | zrobione w zadaniu domowym 3 (`ecommerce_extract_dag` + `ecommerce_transform_dag`) |
