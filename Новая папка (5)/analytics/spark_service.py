from __future__ import annotations

import shutil
import os
import json
import subprocess
import uuid
from typing import Any, Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType


EXPECTED_COLUMNS = ("ts", "category", "value")

_COLUMN_RU_MAP = {
    # базовые
    "ts": "Дата/время",
    "category": "Категория",
    "value": "Значение",
    # частые доменные
    "patient_id": "ID пациента",
    "patientid": "ID пациента",
    "age": "Возраст",
    "gender": "Пол",
    "sex": "Пол",
    "diagnosis": "Диагноз",
    "diagnosis_code": "Код диагноза",
    "id": "ID",
    "date": "Дата",
    "time": "Время",
}

_TOKEN_RU_MAP = {
    "patient": "Пациент",
    "id": "ID",
    "age": "Возраст",
    "gender": "Пол",
    "sex": "Пол",
    "diagnosis": "Диагноз",
    "code": "Код",
    "date": "Дата",
    "time": "Время",
    "status": "Статус",
    "type": "Тип",
    "group": "Группа",
    "value": "Значение",
    "category": "Категория",
    "count": "Кол-во",
}


def _to_ru_column_name(name: Any) -> str:
    """Heuristic mapping of column names to Russian labels."""
    if name is None:
        return ""
    s = str(name).strip()
    if not s:
        return s

    s_lower = s.lower()
    if s_lower in _COLUMN_RU_MAP:
        return _COLUMN_RU_MAP[s_lower]

    # Если это уже кириллица — возвращаем как есть.
    if any("\u0400" <= ch <= "\u04FF" for ch in s):
        return s

    parts = s_lower.replace("-", "_").split("_")
    out: List[str] = []
    for p in parts:
        if not p:
            continue
        if p in _TOKEN_RU_MAP:
            out.append(_TOKEN_RU_MAP[p])
        elif p.isdigit():
            out.append(p)
        else:
            out.append(p.capitalize())
    return " ".join(out) if out else s


class SparkAnalysisError(Exception):
    """Базовая ошибка анализа Spark."""


class SparkDockerUnavailableError(SparkAnalysisError):
    """Docker недоступен."""


class SparkInvalidCsvError(SparkAnalysisError):
    """Некорректный CSV для анализа."""


class SparkExecutionTimeoutError(SparkAnalysisError):
    """Таймаут выполнения Spark-задачи."""


def _guess_delimiter(csv_path: str) -> str:
    # Пробуем угадать разделитель по первой строке заголовка.
    with open(csv_path, "rb") as f:
        sample = f.read(4096)

    # декодируем в максимально совместимом виде
    try:
        text = sample.decode("utf-8-sig")
    except Exception:
        text = sample.decode("latin1", errors="ignore")

    first_line = text.splitlines()[0] if text else ""
    if "\t" in first_line:
        return "\t"
    if ";" in first_line and "," not in first_line:
        return ";"
    return ","  # дефолт


def _build_spark_session() -> SparkSession:
    # local[*] => используем все доступные ядра на твоей машине.
    return (
        SparkSession.builder.appName("JangoCSVAnalysis")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def _is_spark_available() -> bool:
    # PySpark в pip-версии обычно не содержит бинарники Spark целиком.
    # Поэтому нужны `spark-submit` или корректно заданный SPARK_HOME.
    if shutil.which("spark-submit") or shutil.which("spark-submit.cmd"):
        return True

    sp_home = os.environ.get("SPARK_HOME")
    if sp_home:
        for rel in ("bin/spark-submit", "bin/spark-submit.cmd"):
            if os.path.exists(os.path.join(sp_home, rel)):
                return True

    return False


def _is_docker_available() -> bool:
    return shutil.which("docker") is not None


def _parse_json_from_docker_output(text: str) -> Dict[str, Any]:
    # Spark-логи могут окружать JSON, поэтому извлекаем по первым/последним фигурным скобкам.
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"Не удалось извлечь JSON из вывода контейнера. Head: {text[:200]}")
    payload = text[start : end + 1]
    return json.loads(payload)


def _analyze_csv_with_spark_docker(
    csv_path: str, image: str, top_n: int = 5, show_all_string_values: bool = False
) -> Dict[str, Any]:
    if not _is_docker_available():
        raise SparkDockerUnavailableError(
            "Docker не найден в PATH. Установи/разреши Docker для запуска контейнеров."
        )

    abs_path = os.path.abspath(csv_path)
    host_dir = os.path.dirname(abs_path)
    csv_name = os.path.basename(abs_path)

    # Для совместимости заменяем "\" на "/" в -v параметре.
    host_dir_docker = host_dir.replace("\\", "/")

    # Делаем скрипт рядом с CSV, чтобы его тоже можно было примонтировать.
    script_name = f"analyze_spark_{uuid.uuid4().hex}.py"
    script_host_path = os.path.join(host_dir, script_name)
    script_container_path = f"/data/{script_name}"
    csv_container_path = f"/data/{csv_name}"

    delimiter = _guess_delimiter(abs_path)

    script = r'''from __future__ import annotations
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType

EXPECTED_COLUMNS = ("ts", "category", "value")

COLUMN_RU_MAP = {
    "ts": "Дата/время",
    "category": "Категория",
    "value": "Значение",
    "patient_id": "ID пациента",
    "patientid": "ID пациента",
    "age": "Возраст",
    "gender": "Пол",
    "sex": "Пол",
    "diagnosis": "Диагноз",
    "diagnosis_code": "Код диагноза",
    "id": "ID",
    "date": "Дата",
    "time": "Время",
}

TOKEN_RU_MAP = {
    "patient": "Пациент",
    "id": "ID",
    "age": "Возраст",
    "gender": "Пол",
    "sex": "Пол",
    "diagnosis": "Диагноз",
    "code": "Код",
    "date": "Дата",
    "time": "Время",
    "status": "Статус",
    "type": "Тип",
    "group": "Группа",
    "value": "Значение",
    "category": "Категория",
    "count": "Кол-во",
}

def to_ru_column_name(name):
    if name is None:
        return ""
    s = str(name).strip()
    if not s:
        return s

    s_lower = s.lower()
    if s_lower in COLUMN_RU_MAP:
        return COLUMN_RU_MAP[s_lower]

    # Если это уже кириллица — возвращаем как есть.
    if any("\u0400" <= ch <= "\u04FF" for ch in s):
        return s

    parts = s_lower.replace("-", "_").split("_")
    out = []
    for p in parts:
        if not p:
            continue
        if p in TOKEN_RU_MAP:
            out.append(TOKEN_RU_MAP[p])
        elif p.isdigit():
            out.append(p)
        else:
            out.append(p.capitalize())
    return " ".join(out) if out else s

def main():
    if len(sys.argv) < 5:
        raise SystemExit("Usage: analyze.py <csv_path> <delimiter> <top_n> <show_all>")
    csv_path = sys.argv[1]
    delimiter = sys.argv[2]
    top_n = int(sys.argv[3])
    show_all = (sys.argv[4].strip().lower() == "true")

    spark = (
        SparkSession.builder.appName("JangoCSVAnalysisDocker")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    df = (
        spark.read.option("header", True)
        .option("sep", delimiter)
        .option("inferSchema", False)
        .option("mode", "FAILFAST")
        .csv(csv_path)
    )

    cols_lower = {c: c.strip().lower() for c in df.columns}
    for old, new in cols_lower.items():
        if old != new:
            df = df.withColumnRenamed(old, new)

    base_cols_present = all(c in df.columns for c in EXPECTED_COLUMNS)

    # Адаптивная часть: подготовка числовых метрик по всем колонкам
    numeric_stats = []
    for c in df.columns:
        num_col = f"{c}__num"
        num_df = df.withColumn(num_col, F.col(c).cast(DoubleType()))
        # Берём статистики и одновременно определяем, "целочисленная" ли колонка
        # (для этого проверяем отсутствие дробной части с небольшой погрешностью).
        stat_row = num_df.agg(
            F.count(F.col(num_col)).alias("count"),
            F.avg(F.col(num_col)).alias("avg"),
            F.min(F.col(num_col)).alias("min_d"),
            F.max(F.col(num_col)).alias("max_d"),
            F.sum(
                F.when(
                    F.col(num_col).isNotNull()
                    & (F.abs(F.col(num_col) - F.round(F.col(num_col))) > 1e-9),
                    1,
                ).otherwise(0)
            ).alias("non_int_count"),
            F.min(F.col(num_col).cast(LongType())).alias("min_int"),
            F.max(F.col(num_col).cast(LongType())).alias("max_int"),
            # Медиана: используем percentile_approx для отчёта.
            F.expr(f"percentile_approx(`{num_col.replace('`','``')}`, 0.5)").alias("median"),
        ).collect()[0]

        if int(stat_row["count"]) > 0:
            integer_like = int(stat_row["non_int_count"]) == 0
            median_val = float(stat_row["median"]) if stat_row["median"] is not None else None
            if integer_like:
                median_decimals = 0 if median_val is not None and abs(median_val - round(median_val)) < 1e-9 else 1
            else:
                median_decimals = 2

            numeric_stats.append(
                {
                    "column": c,
                    "column_ru": to_ru_column_name(c),
                    "count": int(stat_row["count"]),
                    "avg": float(stat_row["avg"]),
                    "median": median_val,
                    "median_decimals": median_decimals,
                    "integer_like": integer_like,
                    "min": int(stat_row["min_int"]) if integer_like else float(stat_row["min_d"]),
                    "max": int(stat_row["max_int"]) if integer_like else float(stat_row["max_d"]),
                    "min_decimals": 0 if integer_like else 2,
                    "max_decimals": 0 if integer_like else 2,
                }
            )

    # summary по value только если есть базовые колонки.
    if base_cols_present:
        df = (
            df.withColumn("ts", F.to_timestamp(F.col("ts")))
            .withColumn("category", F.col("category").cast(StringType()))
            .withColumn("value", F.col("value").cast(DoubleType()))
        )
        total_rows_before_clean = df.count()
        df = df.filter(F.col("ts").isNotNull() & F.col("value").isNotNull())
        if df.rdd.isEmpty():
            raise ValueError("После парсинга данных не осталось строк (ts/value не распознаны).")
        summary_row = df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.avg("value").alias("avg"),
            F.max("value").alias("max"),
            F.min("value").alias("min"),
        ).collect()[0]
    else:
        total_rows_before_clean = df.count()
        summary_row = {"rows": total_rows_before_clean, "avg": None, "max": None, "min": None}

    by_category = []
    if base_cols_present:
        by_category_df = (
            df.groupBy("category")
            .agg(F.count("*").alias("rows"), F.avg("value").alias("avg_value"))
            .orderBy(F.col("rows").desc())
        )
        by_category = [
            {"category": r["category"], "rows": int(r["rows"]), "avg_value": float(r["avg_value"])}
            for r in by_category_df.collect()
        ]

    timeseries = []
    if base_cols_present:
        by_time_df = (
            df.groupBy(F.date_trunc("hour", F.col("ts")).alias("hour"))
            .agg(F.avg("value").alias("value"))
            .orderBy(F.col("hour").desc())
            .orderBy(F.col("hour").asc())
        )
        timeseries = [
            {"ts": (r["hour"].strftime("%Y-%m-%d %H:%M") if r["hour"] is not None else None), "value": float(r["value"])}
            for r in by_time_df.collect()
        ]

    # Для строковых колонок: топ-N значений (исключая пустые).
    string_top_values = {}
    SMALL_UNIQUE_LIMIT = 20
    MEDIUM_UNIQUE_LIMIT = 100
    TOP_VALUES_MEDIUM = 10
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            col_name = field.name
            filtered = df.filter(F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != ""))
            agg_row = filtered.agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct(F.col(col_name)).alias("unique_count"),
            ).collect()[0]
            unique_count = int(agg_row["unique_count"])
            nonempty_rows = int(agg_row["rows"])
            if unique_count <= 0:
                continue

            cardinality_percent = (unique_count / nonempty_rows * 100.0) if nonempty_rows > 0 else 0.0

            if unique_count <= SMALL_UNIQUE_LIMIT:
                values_df = (
                    filtered.groupBy(col_name)
                    .agg(F.count("*").alias("count"))
                    .orderBy(F.col("count").desc(), F.col(col_name).asc())
                )
                values = [
                    {"type": "value", "value": r[col_name], "count": int(r["count"])}
                    for r in values_df.collect()
                ]
                string_top_values[col_name] = {
                    "mode": "small",
                    "column_ru": to_ru_column_name(col_name),
                    "unique_count": unique_count,
                    "cardinality_percent": float(cardinality_percent),
                    "values": values,
                }
            elif unique_count <= MEDIUM_UNIQUE_LIMIT:
                values_df = (
                    filtered.groupBy(col_name)
                    .agg(F.count("*").alias("count"))
                    .orderBy(F.col("count").desc(), F.col(col_name).asc())
                    .limit(TOP_VALUES_MEDIUM)
                )
                top_vals = [
                    {"type": "value", "value": r[col_name], "count": int(r["count"])}
                    for r in values_df.collect()
                ]
                other_unique = max(0, unique_count - len(top_vals))
                top_vals.append(
                    {"type": "other", "value": "Другие", "other_unique_count": other_unique}
                )
                string_top_values[col_name] = {
                    "mode": "medium",
                    "column_ru": to_ru_column_name(col_name),
                    "unique_count": unique_count,
                    "cardinality_percent": float(cardinality_percent),
                    "values": top_vals,
                }
            else:
                string_top_values[col_name] = {
                    "mode": "high",
                    "column_ru": to_ru_column_name(col_name),
                    "unique_count": unique_count,
                    "cardinality_percent": float(cardinality_percent),
                    "values": [],
                }

    warnings = []
    dropped_rows = total_rows_before_clean - int(summary_row["rows"])
    if dropped_rows > 0:
        warnings.append(f"Отфильтровано строк из-за некорректных ts/value: {dropped_rows}.")

    result = {
        "summary": {
            "rows": int(summary_row["rows"]),
            "avg": (round(float(summary_row["avg"]), 2) if summary_row["avg"] is not None else None),
            "max": (round(float(summary_row["max"]), 2) if summary_row["max"] is not None else None),
            "min": (round(float(summary_row["min"]), 2) if summary_row["min"] is not None else None),
        },
        "columns_count": len(df.columns),
        "schema": [
            {"name": f.name, "type": f.dataType.simpleString(), "display_name": to_ru_column_name(f.name)}
            for f in df.schema.fields
        ],
        "numeric_stats": numeric_stats,
        "by_category": by_category,
        "timeseries": timeseries,
        "string_top_values": string_top_values,
        "warnings": warnings,
    }

    # Печатаем ТОЛЬКО JSON (но Spark логи всё равно могут быть окружением).
    print(json.dumps(result, ensure_ascii=False))
    spark.stop()

if __name__ == "__main__":
    main()
'''

    try:
        with open(script_host_path, "w", encoding="utf-8") as f:
            f.write(script)

        # В зависимости от образа `spark-submit` может быть не в PATH.
        # Для твоего образа `spark` бинарник обычно лежит в `/opt/spark/bin/spark-submit`.
        spark_submit_cmd = os.environ.get("SPARK_DOCKER_SPARK_SUBMIT_CMD", "/opt/spark/bin/spark-submit")
        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{host_dir_docker}:/data",
            image,
            spark_submit_cmd,
            "--master",
            "local[2]",
            "--conf",
            "spark.ui.enabled=false",
            script_container_path,
            csv_container_path,
            delimiter,
            str(top_n),
            "true" if show_all_string_values else "false",
        ]
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=180,
            )
        except subprocess.TimeoutExpired as exc:
            raise SparkExecutionTimeoutError(
                "Spark-анализ выполняется слишком долго (таймаут 180 сек)."
            ) from exc

        output_text = (proc.stdout or "") + "\n" + (proc.stderr or "")
        if proc.returncode != 0:
            if "CSV заголовок неверный" in output_text or "не осталось строк" in output_text:
                raise SparkInvalidCsvError(output_text[:1000])
            raise SparkAnalysisError(
                f"Ошибка spark внутри Docker (exit={proc.returncode}). Output: {output_text[:5000]}"
            )

        return _parse_json_from_docker_output(output_text)
    finally:
        try:
            if os.path.exists(script_host_path):
                os.remove(script_host_path)
        except Exception:
            pass


def analyze_csv_with_spark(
    csv_path: str, top_n: int = 5, show_all_string_values: bool = False
) -> Dict[str, Any]:
    """
    Запускает Apache Spark-анализ CSV и возвращает готовые данные для рендера на фронте.

    Ожидаемые колонки:
      - ts: дата/время (парсится в Timestamp)
      - category: строка
      - value: число
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    delimiter = _guess_delimiter(csv_path)
    # По умолчанию запускаем через Docker (ты просил лучше так).
    # Можно переопределить через SPARK_DOCKER_IMAGE.
    image = os.environ.get("SPARK_DOCKER_IMAGE", "spark")

    # Сначала пробуем Docker. Если Docker не доступен — падаем понятной ошибкой.
    if _is_docker_available():
        return _analyze_csv_with_spark_docker(
            csv_path=csv_path,
            image=image,
            top_n=top_n,
            show_all_string_values=show_all_string_values,
        )

    if not _is_spark_available():
        raise SparkDockerUnavailableError(
            "Не найден локальный spark-submit, и Docker тоже недоступен. "
            "Либо установи Spark локально, либо включи Docker."
        )

    # fallback: локальный PySpark (если у тебя действительно есть spark-binaries).
    spark = _build_spark_session()

    df = (
        spark.read.option("header", True)
        .option("sep", delimiter)
        .option("inferSchema", False)
        .option("mode", "FAILFAST")
        .csv(csv_path)
    )

    cols_lower = {c: c.strip().lower() for c in df.columns}
    for old, new in cols_lower.items():
        if old != new:
            df = df.withColumnRenamed(old, new)

    base_cols_present = all(c in df.columns for c in EXPECTED_COLUMNS)

    numeric_stats = []
    for c in df.columns:
        num_col = f"{c}__num"
        num_df = df.withColumn(num_col, F.col(c).cast(DoubleType()))
        # Берём статистики и одновременно определяем, "целочисленная" ли колонка
        # (для этого проверяем отсутствие дробной части с небольшой погрешностью).
        stat_row = num_df.agg(
            F.count(F.col(num_col)).alias("count"),
            F.avg(F.col(num_col)).alias("avg"),
            F.min(F.col(num_col)).alias("min_d"),
            F.max(F.col(num_col)).alias("max_d"),
            F.sum(
                F.when(
                    F.col(num_col).isNotNull()
                    & (F.abs(F.col(num_col) - F.round(F.col(num_col))) > 1e-9),
                    1,
                ).otherwise(0)
            ).alias("non_int_count"),
            F.min(F.col(num_col).cast(LongType())).alias("min_int"),
            F.max(F.col(num_col).cast(LongType())).alias("max_int"),
            # Медиана: используем percentile_approx для отчёта.
            F.expr(f"percentile_approx(`{num_col.replace('`','``')}`, 0.5)").alias("median"),
        ).collect()[0]

        if int(stat_row["count"]) > 0:
            integer_like = int(stat_row["non_int_count"]) == 0
            median_val = float(stat_row["median"]) if stat_row["median"] is not None else None
            if integer_like:
                median_decimals = 0 if median_val is not None and abs(median_val - round(median_val)) < 1e-9 else 1
            else:
                median_decimals = 2

            numeric_stats.append(
                {
                    "column": c,
                    "column_ru": _to_ru_column_name(c),
                    "count": int(stat_row["count"]),
                    "avg": float(stat_row["avg"]),
                    "median": median_val,
                    "median_decimals": median_decimals,
                    "integer_like": integer_like,
                    "min": int(stat_row["min_int"]) if integer_like else float(stat_row["min_d"]),
                    "max": int(stat_row["max_int"]) if integer_like else float(stat_row["max_d"]),
                    "min_decimals": 0 if integer_like else 2,
                    "max_decimals": 0 if integer_like else 2,
                }
            )

    if base_cols_present:
        df = (
            df.withColumn("ts", F.to_timestamp(F.col("ts")))
            .withColumn("category", F.col("category").cast(StringType()))
            .withColumn("value", F.col("value").cast(DoubleType()))
        )
        total_rows_before_clean = df.count()
        df = df.filter(F.col("ts").isNotNull() & F.col("value").isNotNull())

        if df.rdd.isEmpty():
            raise SparkInvalidCsvError("После парсинга данных не осталось строк (ts/value не распознаны).")

        summary_row = df.agg(
            F.count(F.lit(1)).alias("rows"),
            F.avg("value").alias("avg"),
            F.max("value").alias("max"),
            F.min("value").alias("min"),
        ).collect()[0]
    else:
        total_rows_before_clean = df.count()
        summary_row = {"rows": total_rows_before_clean, "avg": None, "max": None, "min": None}

    by_category: List[Dict[str, Any]] = []
    if base_cols_present:
        by_category_df = (
            df.groupBy("category")
            .agg(F.count("*").alias("rows"), F.avg("value").alias("avg_value"))
            .orderBy(F.col("rows").desc())
        )
        by_category = [
            {"category": r["category"], "rows": int(r["rows"]), "avg_value": float(r["avg_value"])}
            for r in by_category_df.collect()
        ]

    timeseries: List[Dict[str, Any]] = []
    if base_cols_present:
        by_time_df = (
            df.groupBy(F.date_trunc("hour", F.col("ts")).alias("hour"))
            .agg(F.avg("value").alias("value"))
            .orderBy(F.col("hour").desc())
            .orderBy(F.col("hour").asc())
        )

        timeseries = [
            {
                "ts": r["hour"].strftime("%Y-%m-%d %H:%M") if r["hour"] is not None else None,
                "value": float(r["value"]),
            }
            for r in by_time_df.collect()
        ]

    string_top_values = {}
    SMALL_UNIQUE_LIMIT = 20
    MEDIUM_UNIQUE_LIMIT = 100
    TOP_VALUES_MEDIUM = 10
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            col_name = field.name
            filtered = df.filter(F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != ""))
            agg_row = filtered.agg(
                F.count(F.lit(1)).alias("rows"),
                F.countDistinct(F.col(col_name)).alias("unique_count"),
            ).collect()[0]
            unique_count = int(agg_row["unique_count"])
            nonempty_rows = int(agg_row["rows"])
            if unique_count <= 0:
                continue

            cardinality_percent = (unique_count / nonempty_rows * 100.0) if nonempty_rows > 0 else 0.0

            if unique_count <= SMALL_UNIQUE_LIMIT:
                values_df = (
                    filtered.groupBy(col_name)
                    .agg(F.count("*").alias("count"))
                    .orderBy(F.col("count").desc(), F.col(col_name).asc())
                )
                values = [
                    {"type": "value", "value": r[col_name], "count": int(r["count"])}
                    for r in values_df.collect()
                ]
                string_top_values[col_name] = {
                    "mode": "small",
                    "column_ru": _to_ru_column_name(col_name),
                    "unique_count": unique_count,
                    "cardinality_percent": float(cardinality_percent),
                    "values": values,
                }
            elif unique_count <= MEDIUM_UNIQUE_LIMIT:
                values_df = (
                    filtered.groupBy(col_name)
                    .agg(F.count("*").alias("count"))
                    .orderBy(F.col("count").desc(), F.col(col_name).asc())
                    .limit(TOP_VALUES_MEDIUM)
                )
                top_vals = [
                    {"type": "value", "value": r[col_name], "count": int(r["count"])}
                    for r in values_df.collect()
                ]
                other_unique = max(0, unique_count - len(top_vals))
                top_vals.append(
                    {"type": "other", "value": "Другие", "other_unique_count": other_unique}
                )
                string_top_values[col_name] = {
                    "mode": "medium",
                    "column_ru": _to_ru_column_name(col_name),
                    "unique_count": unique_count,
                    "cardinality_percent": float(cardinality_percent),
                    "values": top_vals,
                }
            else:
                string_top_values[col_name] = {
                    "mode": "high",
                    "column_ru": _to_ru_column_name(col_name),
                    "unique_count": unique_count,
                    "cardinality_percent": float(cardinality_percent),
                    "values": [],
                }

    warnings = []
    dropped_rows = total_rows_before_clean - int(summary_row["rows"])
    if dropped_rows > 0:
        warnings.append(
            f"Отфильтровано строк из-за некорректных ts/value: {dropped_rows}."
        )

    return {
        "summary": {
            "rows": int(summary_row["rows"]),
            "avg": (round(float(summary_row["avg"]), 2) if summary_row["avg"] is not None else None),
            "max": (round(float(summary_row["max"]), 2) if summary_row["max"] is not None else None),
            "min": (round(float(summary_row["min"]), 2) if summary_row["min"] is not None else None),
        },
        "columns_count": len(df.columns),
        "schema": [
            {"name": f.name, "type": f.dataType.simpleString(), "display_name": _to_ru_column_name(f.name)}
            for f in df.schema.fields
        ],
        "numeric_stats": numeric_stats,
        "by_category": by_category,
        "timeseries": timeseries,
        "string_top_values": string_top_values,
        "warnings": warnings,
    }

