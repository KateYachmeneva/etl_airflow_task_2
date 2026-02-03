from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import logging
import os

log =logging.getLogger(__name__)

DATA_PATH = "/opt/airflow/data/IOT-temp.csv"
OUT_DIR = "/opt/airflow/output"

def temp_etl():
    os.makedirs(OUT_DIR, exist_ok=True)
    df = pd.read_csv(DATA_PATH)
    log.info("Rows total: %s", len(df))
    log.info("Columns: %s", list(df.columns))

    # 1) отфильтруйте out/in = In
    df = df[df["out/in"] == "In"]
    log.info("Rows after out/in=in filte: %s", len(df))

    # 2) поле noted_date переведите в формат ‘yyyy-MM-dd’ с типом данных date
    df["noted_date"] = pd.to_datetime(df["noted_date"],errors="coerce")

    #Очистка мусора
    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df = df.dropna(subset=["noted_date", "temp"])

    df["date"] = df["noted_date"].dt.date

    # 3) очистите температуру по 5-му и 95-му процентилю
    p5 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    log.info("p5: %s, p95: %s", p5, p95)

    df_clean = df[(df["temp"] > p5) & (df["temp"] < p95)].copy()
    log.info("Rows after cleaning: %s", len(df_clean))
    # 4) вычислите 5 самых жарких и самых холодных дней за год
    # Считаем по ДНЮ: например, средняя температура за день (логично для "дней")
    by_day = (
        df_clean.groupby("date", as_index=False)
        .agg(avg_temp=("temp", "mean"), min_temp=("temp", "min"), max_temp=("temp", "max"), cnt=("temp", "count"))
    )

    top_hot_days = by_day.sort_values("avg_temp", ascending=False).head(5)
    top_cold_days = by_day.sort_values("avg_temp", ascending=True).head(5)

    # Сохраняем
    # noted_date в формате yyyy-MM-dd (для файла cleaned сделаем отдельную колонку)
    df_clean["noted_date"] = df_clean["noted_date"].dt.strftime("%Y-%m-%d")

    df_clean.to_csv(f"{OUT_DIR}/cleaned.csv", index=False)
    top_hot_days.to_csv(f"{OUT_DIR}/top_hot_days.csv", index=False)
    top_cold_days.to_csv(f"{OUT_DIR}/top_cold_days.csv", index=False)

    log.info("Saved: cleaned.csv, top_hot_days.csv, top_cold_days.csv")

with DAG(
        dag_id="temp_etl",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["etl", "homework"],
) as dag:
    PythonOperator(
        task_id="run_etl",
        python_callable=temp_etl
    )
