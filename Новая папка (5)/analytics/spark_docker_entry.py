"""Точка входа для spark-submit внутри Docker (рядом с spark_csv_core.py в /data)."""
from __future__ import annotations

import json
import sys

from pyspark.sql import SparkSession

from spark_csv_core import run_csv_analysis


def main() -> None:
    if len(sys.argv) < 6:
        print(
            json.dumps(
                {
                    "error": "Usage: spark_docker_entry.py <csv> <delim> <top_n> <show_all> <mapping_json_path>"
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        sys.exit(1)
    csv_path = sys.argv[1]
    delimiter = sys.argv[2]
    top_n = int(sys.argv[3])
    show_all = sys.argv[4].strip().lower() == "true"
    with open(sys.argv[5], encoding="utf-8") as f:
        mapping = json.load(f)

    spark = (
        SparkSession.builder.appName("JangoCSVAnalysisDocker")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    try:
        out = run_csv_analysis(spark, csv_path, delimiter, top_n, show_all, mapping)
        print(json.dumps(out, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
