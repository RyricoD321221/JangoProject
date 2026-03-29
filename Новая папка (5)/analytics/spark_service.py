from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from .spark_csv_core import run_csv_analysis


class SparkAnalysisError(Exception):
    """Базовая ошибка анализа Spark."""


class SparkDockerUnavailableError(SparkAnalysisError):
    """Docker недоступен."""


class SparkInvalidCsvError(SparkAnalysisError):
    """Некорректный CSV для анализа."""


class SparkExecutionTimeoutError(SparkAnalysisError):
    """Таймаут выполнения Spark-задачи."""


def _guess_delimiter(csv_path: str) -> str:
    with open(csv_path, "rb") as f:
        sample = f.read(4096)
    try:
        text = sample.decode("utf-8-sig")
    except Exception:
        text = sample.decode("latin1", errors="ignore")
    first_line = text.splitlines()[0] if text else ""
    if "\t" in first_line:
        return "\t"
    if ";" in first_line and "," not in first_line:
        return ";"
    return ","


def _build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("JangoCSVAnalysis")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def _is_spark_available() -> bool:
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
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"Не удалось извлечь JSON из вывода контейнера. Head: {text[:200]}")
    payload = text[start : end + 1]
    return json.loads(payload)


def _analyze_csv_with_spark_docker(
    csv_path: str,
    image: str,
    top_n: int = 5,
    show_all_string_values: bool = False,
    column_mapping: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not _is_docker_available():
        raise SparkDockerUnavailableError(
            "Docker не найден в PATH. Установи/разреши Docker для запуска контейнеров."
        )

    abs_path = os.path.abspath(csv_path)
    host_dir = os.path.dirname(abs_path)
    csv_name = os.path.basename(abs_path)
    host_dir_docker = host_dir.replace("\\", "/")
    csv_container_path = f"/data/{csv_name}"

    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    core_src = os.path.join(pkg_dir, "spark_csv_core.py")
    entry_src = os.path.join(pkg_dir, "spark_docker_entry.py")
    core_dest = os.path.join(host_dir, "spark_csv_core.py")
    entry_dest = os.path.join(host_dir, "spark_docker_entry.py")
    map_host = os.path.join(host_dir, "column_mapping.json")

    shutil.copy2(core_src, core_dest)
    shutil.copy2(entry_src, entry_dest)
    with open(map_host, "w", encoding="utf-8") as mf:
        json.dump(column_mapping or {}, mf, ensure_ascii=False)

    delimiter = _guess_delimiter(abs_path)
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
        "/data/spark_docker_entry.py",
        csv_container_path,
        delimiter,
        str(top_n),
        "true" if show_all_string_values else "false",
        "/data/column_mapping.json",
    ]

    try:
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
            if "не осталось строк" in output_text.lower() or "ValueError" in output_text:
                raise SparkInvalidCsvError(output_text[:1000])
            raise SparkAnalysisError(
                f"Ошибка spark внутри Docker (exit={proc.returncode}). Output: {output_text[:5000]}"
            )

        return _parse_json_from_docker_output(output_text)
    finally:
        for p in (core_dest, entry_dest, map_host):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass


def analyze_csv_with_spark(
    csv_path: str,
    top_n: int = 5,
    show_all_string_values: bool = False,
    column_mapping: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    mapping = column_mapping or {}
    delimiter = _guess_delimiter(csv_path)
    image = os.environ.get("SPARK_DOCKER_IMAGE", "spark")

    if _is_docker_available():
        return _analyze_csv_with_spark_docker(
            csv_path=csv_path,
            image=image,
            top_n=top_n,
            show_all_string_values=show_all_string_values,
            column_mapping=mapping,
        )

    if not _is_spark_available():
        raise SparkDockerUnavailableError(
            "Не найден локальный spark-submit, и Docker тоже недоступен или завершился с ошибкой. "
            "Либо установи Spark локально, либо включи Docker."
        )

    spark = _build_spark_session()
    try:
        return run_csv_analysis(
            spark,
            csv_path,
            delimiter,
            top_n,
            show_all_string_values,
            mapping,
        )
    except ValueError as exc:
        raise SparkInvalidCsvError(str(exc)) from exc
    finally:
        spark.stop()
