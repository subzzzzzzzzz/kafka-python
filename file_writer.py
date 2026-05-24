"""Append-safe CSV writing with size-based rotation."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

from config import FILE_ROTATION_BYTES, OUTPUT_FIELDS, ROTATED_OUTPUT_DIR, ensure_directories
from validator import validate_output_record


SAFE_NAME_PATTERN = re.compile(r"[^a-zA-Z0-9_.-]+")


def safe_filename(value: str, default: str = "unknown") -> str:
    cleaned = SAFE_NAME_PATTERN.sub("_", value.strip().lower()).strip("._")
    return cleaned or default


class RotatingCsvWriter:
    """CSV writer that appends safely and rotates active files after a byte limit."""

    def __init__(self, fields: tuple[str, ...] = OUTPUT_FIELDS, rotation_bytes: int = FILE_ROTATION_BYTES) -> None:
        self.fields = fields
        self.rotation_bytes = rotation_bytes
        ensure_directories()

    def write(self, path: Path, record: dict[str, Any]) -> Path:
        validate_output_record(record, self.fields)
        path.parent.mkdir(parents=True, exist_ok=True)
        active_path = self._active_path(path)
        self._validate_or_create_file(active_path)

        if active_path.exists() and active_path.stat().st_size >= self.rotation_bytes:
            active_path = self._next_rotation_path(path)
            self._validate_or_create_file(active_path)
            self._record_rotation(path, active_path)

        file_was_empty = active_path.stat().st_size == 0
        with active_path.open("a", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=self.fields, extrasaction="ignore")
            if file_was_empty:
                writer.writeheader()
            writer.writerow({field: record.get(field) for field in self.fields})

        return active_path

    def _active_path(self, base_path: Path) -> Path:
        if not base_path.exists() or base_path.stat().st_size < self.rotation_bytes:
            return base_path

        index = 1
        while True:
            candidate = self._with_index(base_path, index)
            if not candidate.exists() or candidate.stat().st_size < self.rotation_bytes:
                return candidate
            index += 1

    def _next_rotation_path(self, base_path: Path) -> Path:
        index = 1
        while self._with_index(base_path, index).exists():
            index += 1
        return self._with_index(base_path, index)

    def _with_index(self, path: Path, index: int) -> Path:
        return path.with_name(f"{path.stem}_{index}{path.suffix}")

    def _validate_or_create_file(self, path: Path) -> None:
        if not path.exists() or path.stat().st_size == 0:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
            return

        with path.open("r", encoding="utf-8", newline="") as file_obj:
            reader = csv.reader(file_obj)
            header = next(reader, None)
        if header != list(self.fields):
            raise ValueError(f"CSV header mismatch for {path}: {header} != {list(self.fields)}")

    def _record_rotation(self, base_path: Path, rotated_path: Path) -> None:
        ROTATED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        event = {
            "source_file": str(base_path),
            "active_file": str(rotated_path),
            "rotation_bytes": self.rotation_bytes,
        }
        metadata_path = ROTATED_OUTPUT_DIR / "rotation_events.csv"
        file_was_empty = not metadata_path.exists() or metadata_path.stat().st_size == 0
        with metadata_path.open("a", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=("source_file", "active_file", "rotation_bytes"))
            if file_was_empty:
                writer.writeheader()
            writer.writerow(event)
