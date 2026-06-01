"""
Чистый PySpark-анализ CSV без зависимостей Django.
Используется локально (spark_service) и в Docker (spark_docker_entry).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType

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


def to_ru_column_name(name: Any) -> str:
    if name is None:
        return ""
    s = str(name).strip()
    if not s:
        return s
    s_lower = s.lower()
    if s_lower in COLUMN_RU_MAP:
        return COLUMN_RU_MAP[s_lower]
    if any("\u0400" <= ch <= "\u04FF" for ch in s):
        return s
    parts = s_lower.replace("-", "_").split("_")
    out: List[str] = []
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


def _logical_type_ru(kind: str, integer_like: bool) -> str:
    if kind == "timestamp":
        return "Дата и время"
    if kind == "numeric":
        return "Число (целое)" if integer_like else "Число"
    return "Текст"


def _auto_mapping(cols: List[str]) -> Dict[str, Optional[str]]:
    s = set(cols)
    m: Dict[str, Optional[str]] = {"ts": None, "category": None, "value": None}
    for cand in (
        "ts",
        "datetime",
        "timestamp",
        "time",
        "date",
        "дата",
        "время",
        "event_time",
        "eventtime",
        "created_at",
        "updated_at",
        "admitted_at",
    ):
        if cand in s:
            m["ts"] = cand
            break
    for cand in (
        "value",
        "val",
        "amount",
        "count",
        "price",
        "sum",
        "metric",
        "measurement",
        "score",
        "rating",
        "значение",
        "число",
    ):
        if cand in s:
            m["value"] = cand
            break
    for cand in (
        "category",
        "cat",
        "type",
        "group",
        "stream_type",
        "streamtype",
        "kind",
        "категория",
        "класс",
        "label",
    ):
        if cand in s:
            m["category"] = cand
            break
    return m


def _merge_mapping(cols: List[str], user: Dict[str, Any]) -> Dict[str, Optional[str]]:
    col_set = set(cols)
    auto = _auto_mapping(cols)
    out: Dict[str, Optional[str]] = {}
    for key in ("ts", "category", "value"):
        raw = user.get(key)
        v = (str(raw).strip().lower() if raw is not None else "")
        if v and v in col_set:
            out[key] = v
        elif auto.get(key) and auto[key] in col_set:
            out[key] = auto[key]
        else:
            out[key] = None
    return out


def _infer_column_kinds(df) -> Dict[str, Dict[str, Any]]:
    kinds: Dict[str, Dict[str, Any]] = {}
    for c in df.columns:
        num_col = f"__infer_{c}__num"
        tmp = df.withColumn(num_col, F.col(c).cast(DoubleType()))
        # Распознаём даты в типичных форматах CSV: ISO и "ДД.ММ.ГГГГ[ ЧЧ:ММ[:СС]]".
        c_s = F.trim(F.col(c).cast(StringType()))
        ts_try = F.coalesce(
            F.to_timestamp(c_s),
            F.to_timestamp(c_s, "dd.MM.yyyy HH:mm:ss"),
            F.to_timestamp(c_s, "dd.MM.yyyy HH:mm"),
            F.to_timestamp(c_s, "dd.MM.yyyy"),
        )
        row = tmp.agg(
            F.sum(
                F.when(F.col(c).isNotNull() & (F.trim(F.col(c)) != ""), 1).otherwise(0)
            ).alias("nn"),
            F.sum(F.when(ts_try.isNotNull(), 1).otherwise(0)).alias("nts"),
            F.sum(F.when(F.col(num_col).isNotNull(), 1).otherwise(0)).alias("nnum"),
            F.sum(
                F.when(
                    F.col(num_col).isNotNull()
                    & (F.abs(F.col(num_col) - F.round(F.col(num_col))) > 1e-9),
                    1,
                ).otherwise(0)
            ).alias("non_int_count"),
        ).collect()[0]
        nn = int(row["nn"] or 0)
        nts = int(row["nts"] or 0)
        nnum = int(row["nnum"] or 0)
        non_int = int(row["non_int_count"] or 0)
        if nn <= 0:
            kinds[c] = {"kind": "string", "integer_like": False}
            continue
        ts_ratio = nts / nn if nn else 0.0
        num_ratio = nnum / nn if nn else 0.0
        if ts_ratio >= 0.85 and ts_ratio >= num_ratio:
            kinds[c] = {"kind": "timestamp", "integer_like": False}
        elif num_ratio >= 0.85:
            integer_like = non_int == 0
            kinds[c] = {"kind": "numeric", "integer_like": integer_like}
        else:
            kinds[c] = {"kind": "string", "integer_like": False}
    return kinds


def _numeric_block(df, c: str, column_ru: str) -> Optional[Dict[str, Any]]:
    num_col = f"{c}__num"
    num_df = df.withColumn(num_col, F.col(c).cast(DoubleType()))
    stat_row = num_df.agg(
        F.count(F.col(num_col)).alias("count"),
        F.sum(F.col(num_col)).alias("sum_v"),
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
        F.expr(f"percentile_approx(`{num_col.replace('`', '``')}`, 0.5)").alias("median"),
    ).collect()[0]
    if int(stat_row["count"]) <= 0:
        return None
    integer_like = int(stat_row["non_int_count"]) == 0
    median_val = float(stat_row["median"]) if stat_row["median"] is not None else None
    if integer_like:
        md = 0 if median_val is not None and abs(median_val - round(median_val)) < 1e-9 else 1
    else:
        md = 2
    sum_v = stat_row["sum_v"]
    sum_v_f = float(sum_v) if sum_v is not None else None
    return {
        "column": c,
        "column_ru": column_ru,
        "count": int(stat_row["count"]),
        "sum": sum_v_f,
        "avg": float(stat_row["avg"]),
        "median": median_val,
        "median_decimals": md,
        "integer_like": integer_like,
        "min": int(stat_row["min_int"]) if integer_like else float(stat_row["min_d"]),
        "max": int(stat_row["max_int"]) if integer_like else float(stat_row["max_d"]),
        "min_decimals": 0 if integer_like else 2,
        "max_decimals": 0 if integer_like else 2,
    }


def _numeric_top_values_for_col(
    df, col_name: str, column_ru: str, top_n: int, total_rows: int
) -> Optional[Dict[str, Any]]:
    """
    Частоты значений для числовых колонок.

    Для float-колонок кардинальность часто очень высокая, поэтому:
    - small: показываем все значения (если уникальных мало)
    - medium: показываем top-N и добавляем "Другие"
    - high: только сводка по числу уникальных (без списка значений)
    """
    SMALL_UNIQUE_LIMIT = 20
    MEDIUM_UNIQUE_LIMIT = 200
    TOP_VALUES_MEDIUM = max(10, int(top_n) if top_n else 10)
    denom_all = max(int(total_rows), 1)

    # Приводим к числу устойчиво: убираем пробелы/nbsp и меняем "," на "."
    raw_s = F.trim(F.col(col_name).cast(StringType()))
    norm = F.regexp_replace(raw_s, r"[\u00A0\s]", "")
    norm = F.regexp_replace(norm, ",", ".")
    num = norm.cast(DoubleType())

    base = df.withColumn("__num_tv", num).filter(F.col("__num_tv").isNotNull())
    agg_row = base.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(F.col("__num_tv")).alias("unique_count"),
    ).collect()[0]
    unique_count = int(agg_row["unique_count"] or 0)
    nonempty_rows = int(agg_row["rows"] or 0)
    if unique_count <= 0 or nonempty_rows <= 0:
        return None

    cardinality_percent = (unique_count / nonempty_rows * 100.0) if nonempty_rows else 0.0

    def add_pct(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for r in records:
            if r.get("type") == "value" and "count" in r:
                cnt = int(r["count"])
                r["pct_of_all_rows"] = round(cnt / denom_all * 100.0, 2)
        return records

    if unique_count <= SMALL_UNIQUE_LIMIT:
        values_df = (
            base.groupBy(F.col("__num_tv").alias("value"))
            .agg(F.count("*").alias("count"))
            .orderBy(F.col("count").desc(), F.col("value").asc())
        )
        values = add_pct(
            [
                {"type": "value", "value": float(r["value"]), "count": int(r["count"])}
                for r in values_df.collect()
            ]
        )
        return {
            "mode": "small",
            "column_ru": column_ru,
            "unique_count": unique_count,
            "cardinality_percent": float(cardinality_percent),
            "values": values,
        }

    if unique_count <= MEDIUM_UNIQUE_LIMIT:
        values_df = (
            base.groupBy(F.col("__num_tv").alias("value"))
            .agg(F.count("*").alias("count"))
            .orderBy(F.col("count").desc(), F.col("value").asc())
            .limit(TOP_VALUES_MEDIUM)
        )
        top_vals = [
            {"type": "value", "value": float(r["value"]), "count": int(r["count"])}
            for r in values_df.collect()
        ]
        top_vals = add_pct(top_vals)
        other_unique = max(0, unique_count - len(top_vals))
        top_vals.append({"type": "other", "value": "Другие", "other_unique_count": other_unique})
        return {
            "mode": "medium",
            "column_ru": column_ru,
            "unique_count": unique_count,
            "cardinality_percent": float(cardinality_percent),
            "values": top_vals,
        }

    return {
        "mode": "high",
        "column_ru": column_ru,
        "unique_count": unique_count,
        "cardinality_percent": float(cardinality_percent),
        "values": [],
    }


def _string_top_values_for_col(
    df, col_name: str, column_ru: str, top_n: int, total_rows: int
) -> Optional[Dict[str, Any]]:
    SMALL_UNIQUE_LIMIT = 20
    MEDIUM_UNIQUE_LIMIT = 100
    TOP_VALUES_MEDIUM = max(10, top_n)
    filtered = df.filter(F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != ""))
    agg_row = filtered.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(F.col(col_name)).alias("unique_count"),
    ).collect()[0]
    unique_count = int(agg_row["unique_count"])
    nonempty_rows = int(agg_row["rows"])
    if unique_count <= 0:
        return None
    cardinality_percent = (
        (unique_count / nonempty_rows * 100.0) if nonempty_rows > 0 else 0.0
    )
    denom_all = max(total_rows, 1)

    def add_pct(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for r in records:
            if r.get("type") == "value" and "count" in r:
                cnt = int(r["count"])
                r["pct_of_all_rows"] = round(cnt / denom_all * 100.0, 2)
        return records

    if unique_count <= SMALL_UNIQUE_LIMIT:
        values_df = (
            filtered.groupBy(col_name)
            .agg(F.count("*").alias("count"))
            .orderBy(F.col("count").desc(), F.col(col_name).asc())
        )
        values = add_pct(
            [
                {"type": "value", "value": r[col_name], "count": int(r["count"])}
                for r in values_df.collect()
            ]
        )
        return {
            "mode": "small",
            "column_ru": column_ru,
            "unique_count": unique_count,
            "cardinality_percent": float(cardinality_percent),
            "values": values,
        }
    if unique_count <= MEDIUM_UNIQUE_LIMIT:
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
        top_vals = add_pct(top_vals)
        other_unique = max(0, unique_count - len(top_vals))
        top_vals.append(
            {"type": "other", "value": "Другие", "other_unique_count": other_unique}
        )
        return {
            "mode": "medium",
            "column_ru": column_ru,
            "unique_count": unique_count,
            "cardinality_percent": float(cardinality_percent),
            "values": top_vals,
        }
    return {
        "mode": "high",
        "column_ru": column_ru,
        "unique_count": unique_count,
        "cardinality_percent": float(cardinality_percent),
        "values": [],
    }


def _format_bucket(bucket: Any, unit: str) -> Optional[str]:
    if bucket is None:
        return None
    if unit == "year":
        return bucket.strftime("%Y")
    if unit == "month":
        return bucket.strftime("%Y-%m")
    if unit == "day":
        return bucket.strftime("%Y-%m-%d")
    return bucket.strftime("%Y-%m-%d %H:%M")


def run_csv_analysis(
    spark: SparkSession,
    csv_path: str,
    delimiter: str,
    top_n: int,
    show_all_string_values: bool,
    column_mapping: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    del show_all_string_values  # пока не используется отдельно от top_n в строковом блоке

    df = (
        spark.read.option("header", True)
        .option("sep", delimiter)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(csv_path)
    )

    for old in list(df.columns):
        if old == "_corrupt_record":
            continue
        new = old.strip().lower()
        if old != new:
            df = df.withColumnRenamed(old, new)

    if "_corrupt_record" in df.columns:
        df = df.drop("_corrupt_record")

    total_rows_file = df.count()
    kinds = _infer_column_kinds(df)
    user_map = column_mapping if isinstance(column_mapping, dict) else {}
    mapping = _merge_mapping(list(df.columns), user_map)

    ts_col, val_col, cat_col = mapping["ts"], mapping["value"], mapping["category"]
    base_ok = bool(ts_col and val_col and ts_col in df.columns and val_col in df.columns)

    numeric_stats: List[Dict[str, Any]] = []
    numeric_top_values: Dict[str, Any] = {}
    for c in df.columns:
        if kinds[c]["kind"] != "numeric":
            continue
        block = _numeric_block(df, c, to_ru_column_name(c))
        if block:
            numeric_stats.append(block)
            tv = _numeric_top_values_for_col(
                df, c, to_ru_column_name(c), top_n=top_n, total_rows=total_rows_file
            )
            if tv:
                numeric_top_values[c] = tv

    schema_rows = [
        {
            "name": c,
            "type": _logical_type_ru(kinds[c]["kind"], kinds[c].get("integer_like", False)),
            "display_name": to_ru_column_name(c),
        }
        for c in df.columns
    ]

    string_top_values: Dict[str, Any] = {}
    for c in df.columns:
        if kinds[c]["kind"] != "string":
            continue
        st = _string_top_values_for_col(df, c, to_ru_column_name(c), top_n, total_rows_file)
        if st:
            string_top_values[c] = st

    summary_row: Dict[str, Any]
    by_category: List[Dict[str, Any]] = []
    timeseries_by_granularity: Dict[str, List[Dict[str, Any]]] = {}
    warnings: List[str] = []

    if not base_ok:
        summary_row = {
            "rows": int(total_rows_file),
            "avg": None,
            "max": None,
            "min": None,
            "sum": None,
        }
        warnings.append(
            "Расширенный анализ по времени и «полям ts/category/value» недоступен: "
            "укажи в форме колонки «Дата/время» и «Значение», либо добавь колонки с распознаваемыми именами."
        )
    else:
        sel_cols = [
            F.col(ts_col).alias("__raw_ts"),
            F.col(val_col).alias("__raw_val"),
        ]
        if cat_col and cat_col in df.columns:
            sel_cols.append(F.col(cat_col).alias("__raw_cat"))
        df_b = df.select(*sel_cols)

        # Устойчивый парсинг дат и чисел для реальных CSV.
        # - даты часто приходят как "ДД.ММ.ГГГГ" или "ДД.ММ.ГГГГ ЧЧ:ММ[:СС]"
        # - числа часто приходят со знаком "," в качестве десятичного разделителя
        raw_ts_s = F.trim(F.col("__raw_ts").cast(StringType()))
        raw_val_s = F.trim(F.col("__raw_val").cast(StringType()))

        # Несколько попыток распознать timestamp: ISO, "dd.MM.yyyy", "dd.MM.yyyy HH:mm", "dd.MM.yyyy HH:mm:ss".
        ts_parsed = F.coalesce(
            F.to_timestamp(raw_ts_s),  # если Spark сам угадает (ISO и т.п.)
            F.to_timestamp(raw_ts_s, "dd.MM.yyyy HH:mm:ss"),
            F.to_timestamp(raw_ts_s, "dd.MM.yyyy HH:mm"),
            F.to_timestamp(raw_ts_s, "dd.MM.yyyy"),
        )

        # Число: заменяем запятую на точку и убираем пробелы/неразрывные пробелы как разделители тысяч.
        val_norm = F.regexp_replace(raw_val_s, r"[\u00A0\s]", "")
        val_norm = F.regexp_replace(val_norm, ",", ".")
        val_parsed = val_norm.cast(DoubleType())

        df_b = df_b.withColumn("__ts", ts_parsed).withColumn("__val", val_parsed)

        total_before = df_b.count()
        trim_ts = F.trim(F.col("__raw_ts").cast(StringType()))
        trim_val = F.trim(F.col("__raw_val").cast(StringType()))
        empty_ts = df_b.filter(
            F.col("__raw_ts").isNull() | (trim_ts == "") | (trim_ts == "null")
        ).count()
        bad_ts_parse = df_b.filter(
            F.col("__raw_ts").isNotNull()
            & (trim_ts != "")
            & (trim_ts != "null")
            & F.col("__ts").isNull()
        ).count()
        empty_val = df_b.filter(
            F.col("__raw_val").isNull() | (trim_val == "") | (trim_val == "null")
        ).count()
        bad_val_parse = df_b.filter(
            F.col("__raw_val").isNotNull()
            & (trim_val != "")
            & (trim_val != "null")
            & F.col("__val").isNull()
        ).count()

        df_clean = df_b.filter(F.col("__ts").isNotNull() & F.col("__val").isNotNull())
        if df_clean.rdd.isEmpty():
            # Не падаем всем анализом: возвращаем базовые блоки и подробное предупреждение.
            warnings.append(
                "После разбора по выбранным колонкам «Дата/время» и «Значение» не осталось строк. "
                "Проверь, что «Дата/время» имеет вид, например, 21.11.2022 или 21.11.2022 14:30, "
                "а «Значение» — число (допускается 123.45 или 123,45). "
                "Расширенный анализ по времени пропущен."
            )
            timeseries_by_granularity = {}
            by_category = []
            summary_row = {
                "rows": 0,
                "avg": None,
                "max": None,
                "min": None,
                "sum": None,
            }
            # дальше код ниже использует summary_row/by_category/timeseries_by_granularity
            # поэтому просто пропускаем блок расчётов.
            base_ok = False
        if base_ok:
            summary_agg = df_clean.agg(
                F.count(F.lit(1)).alias("rows"),
                F.avg("__val").alias("avg"),
                F.max("__val").alias("max"),
                F.min("__val").alias("min"),
                F.sum("__val").alias("sum_v"),
            ).collect()[0]

            summary_row = {
                "rows": int(summary_agg["rows"]),
                "avg": (
                    round(float(summary_agg["avg"]), 2)
                    if summary_agg["avg"] is not None
                    else None
                ),
                "max": (
                    round(float(summary_agg["max"]), 2)
                    if summary_agg["max"] is not None
                    else None
                ),
                "min": (
                    round(float(summary_agg["min"]), 2)
                    if summary_agg["min"] is not None
                    else None
                ),
                "sum": (
                    round(float(summary_agg["sum_v"]), 2)
                    if summary_agg["sum_v"] is not None
                    else None
                ),
            }

            dropped = total_before - int(summary_row["rows"])
            if dropped > 0:
                parts = [
                    "Строка считается некорректной, если для выбранных колонок «Дата/время» или «Значение» "
                    "значение пустое, пробелы, NULL/пустая строка, либо формат не удаётся распознать "
                    "(дата не читается как дата, число — как число)."
                ]
                parts.append(
                    f"Исключено таких строк: {dropped} из {total_before}. "
                    f"Пустое/отсутствующее время: {empty_ts}; не распознано как дата: {bad_ts_parse}; "
                    f"пустое/отсутствующее значение: {empty_val}; не распознано как число: {bad_val_parse}."
                )
                warnings.append(" ".join(parts))

            if cat_col and cat_col in df.columns and "__raw_cat" in df_clean.columns:
                by_cat_df = (
                    df_clean.groupBy(
                        F.col("__raw_cat").cast(StringType()).alias("category")
                    )
                    .agg(F.count("*").alias("rows"), F.avg("__val").alias("avg_value"))
                    .orderBy(F.col("rows").desc())
                )
                by_category = [
                    {
                        "category": r["category"],
                        "rows": int(r["rows"]),
                        "avg_value": float(r["avg_value"]),
                    }
                    for r in by_cat_df.collect()
                ]

            work_ts = df_clean.select("__ts", "__val")
            for unit in ("hour", "day", "month", "year"):
                by_time_df = (
                    work_ts.groupBy(F.date_trunc(unit, F.col("__ts")).alias("bucket"))
                    .agg(
                        F.count("*").alias("rows"),
                        F.sum("__val").alias("sum_value"),
                        F.avg("__val").alias("avg_value"),
                    )
                    .orderBy(F.col("bucket").asc())
                )
                timeseries_by_granularity[unit] = [
                    {
                        "ts": _format_bucket(r["bucket"], unit),
                        "rows": int(r["rows"]),
                        "sum_value": float(r["sum_value"]),
                        "value": float(r["avg_value"]),
                    }
                    for r in by_time_df.collect()
                ]

    timeseries = timeseries_by_granularity.get("hour", [])

    return {
        "summary": {
            "rows": int(summary_row["rows"]),
            "avg": summary_row.get("avg"),
            "max": summary_row.get("max"),
            "min": summary_row.get("min"),
            "sum": summary_row.get("sum"),
        },
        "total_rows_file": int(total_rows_file),
        "columns_count": len(df.columns),
        "column_mapping_resolved": {
            "ts": ts_col,
            "category": cat_col,
            "value": val_col,
        },
        "schema": schema_rows,
        "numeric_stats": numeric_stats,
        "numeric_top_values": numeric_top_values,
        "by_category": by_category,
        "timeseries": timeseries,
        "timeseries_by_granularity": timeseries_by_granularity,
        "string_top_values": string_top_values,
        "warnings": warnings,
    }


