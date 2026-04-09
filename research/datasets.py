from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence

from research.fair_values import parse_timestamp
from research.schemas import SportsBenchmarkCase, serialize_benchmark_case
from research.storage import (
    normalize_for_json,
    read_jsonl_records,
    write_json,
    write_jsonl_records,
)

DatasetKind = Literal["rows_jsonl", "benchmark_cases"]

_ROW_TIMESTAMP_CANDIDATES = (
    "captured_at",
    "observed_at",
    "generated_at",
    "recorded_at",
    "snapshot_at",
    "timestamp",
    "ts",
    "date",
)
_CASE_TIMESTAMP_CANDIDATES = (
    "recorded_at",
    "snapshot_at",
    "as_of",
    "created_at",
)
_ALLOWED_DATASET_KINDS: tuple[DatasetKind, ...] = (
    "rows_jsonl",
    "benchmark_cases",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _default_version() -> str:
    return _utc_now().strftime("%Y%m%dT%H%M%S%fZ")


def _slugify(value: str) -> str:
    slug = "".join(
        character.lower() if character.isalnum() else "-"
        for character in str(value).strip()
    )
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "dataset"


def _require_object(name: str, value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _lookup_path(payload: dict[str, Any], dotted_path: str) -> object:
    value: object = payload
    for part in dotted_path.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def _normalize_recorded_at(value: object) -> str | None:
    if value in (None, ""):
        return None
    return _isoformat_utc(parse_timestamp(value))


def _resolve_recorded_at(
    payload: dict[str, Any],
    *,
    timestamp_field: str | None,
    candidate_fields: Sequence[str],
) -> str | None:
    if timestamp_field is not None:
        return _normalize_recorded_at(_lookup_path(payload, timestamp_field))
    for field_name in candidate_fields:
        normalized = _normalize_recorded_at(_lookup_path(payload, field_name))
        if normalized is not None:
            return normalized
    return None


def _ordered_unique(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def _coerce_dataset_kind(value: object) -> DatasetKind:
    normalized = str(value)
    if normalized not in _ALLOWED_DATASET_KINDS:
        raise ValueError(f"unknown dataset snapshot kind: {normalized}")
    return normalized


def _ensure_unique_record_ids(records: Sequence[DatasetSnapshotRecord]) -> None:
    seen: set[str] = set()
    duplicates: list[str] = []
    for record in records:
        if record.record_id in seen:
            duplicates.append(record.record_id)
            continue
        seen.add(record.record_id)
    if duplicates:
        raise ValueError(
            "dataset snapshot record_id values must be unique: "
            + ", ".join(_ordered_unique(duplicates))
        )


@dataclass(frozen=True)
class DatasetSnapshotRecord:
    record_id: str
    recorded_at: str | None = None
    relative_path: str | None = None
    row_index: int | None = None
    name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"record_id": self.record_id}
        if self.recorded_at is not None:
            payload["recorded_at"] = self.recorded_at
        if self.relative_path is not None:
            payload["relative_path"] = self.relative_path
        if self.row_index is not None:
            payload["row_index"] = self.row_index
        if self.name is not None:
            payload["name"] = self.name
        if self.metadata:
            payload["metadata"] = normalize_for_json(self.metadata)
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "DatasetSnapshotRecord":
        return cls(
            record_id=str(payload["record_id"]),
            recorded_at=(
                str(payload["recorded_at"])
                if payload.get("recorded_at") is not None
                else None
            ),
            relative_path=(
                str(payload["relative_path"])
                if payload.get("relative_path") is not None
                else None
            ),
            row_index=(
                int(payload["row_index"])
                if payload.get("row_index") is not None
                else None
            ),
            name=str(payload["name"]) if payload.get("name") is not None else None,
            metadata=dict(payload.get("metadata", {})),
        )


@dataclass(frozen=True)
class DatasetSnapshotManifest:
    dataset_name: str
    version: str
    kind: DatasetKind
    created_at: str
    records: tuple[DatasetSnapshotRecord, ...]
    metadata: dict[str, Any] = field(default_factory=dict)
    snapshot_dir: Path | None = field(default=None, compare=False, repr=False)

    @property
    def record_count(self) -> int:
        return len(self.records)

    @property
    def earliest_recorded_at(self) -> str | None:
        dated = [record.recorded_at for record in self.records if record.recorded_at]
        return min(dated) if dated else None

    @property
    def latest_recorded_at(self) -> str | None:
        dated = [record.recorded_at for record in self.records if record.recorded_at]
        return max(dated) if dated else None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "dataset_name": self.dataset_name,
            "version": self.version,
            "kind": self.kind,
            "created_at": self.created_at,
            "summary": {
                "record_count": self.record_count,
                "earliest_recorded_at": self.earliest_recorded_at,
                "latest_recorded_at": self.latest_recorded_at,
            },
            "records": [record.to_payload() for record in self.records],
        }
        if self.metadata:
            payload["metadata"] = normalize_for_json(self.metadata)
        return payload

    @classmethod
    def from_payload(
        cls,
        payload: dict[str, Any],
        *,
        snapshot_dir: Path | None = None,
    ) -> "DatasetSnapshotManifest":
        records_payload = payload.get("records", [])
        if not isinstance(records_payload, list):
            raise ValueError("dataset snapshot manifest records must be a list")
        return cls(
            dataset_name=str(payload["dataset_name"]),
            version=str(payload["version"]),
            kind=_coerce_dataset_kind(payload["kind"]),
            created_at=str(payload["created_at"]),
            records=tuple(
                DatasetSnapshotRecord.from_payload(_require_object("record", item))
                for item in records_payload
            ),
            metadata=dict(payload.get("metadata", {})),
            snapshot_dir=snapshot_dir,
        )


@dataclass(frozen=True)
class WalkForwardSplit:
    split_index: int
    train_record_ids: tuple[str, ...]
    test_record_ids: tuple[str, ...]
    train_start_at: str
    train_end_at: str
    test_start_at: str
    test_end_at: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "split_index": self.split_index,
            "train_record_ids": list(self.train_record_ids),
            "test_record_ids": list(self.test_record_ids),
            "train_start_at": self.train_start_at,
            "train_end_at": self.train_end_at,
            "test_start_at": self.test_start_at,
            "test_end_at": self.test_end_at,
        }


class DatasetRegistry:
    def __init__(self, root_dir: Path | str = "research/datasets"):
        self.root_dir = Path(root_dir)
        self.registry_path = self.root_dir / "registry.json"
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def _read_registry_payload(self) -> dict[str, Any]:
        if not self.registry_path.exists():
            return {"datasets": {}}
        payload = _require_object(
            "dataset registry",
            json.loads(self.registry_path.read_text(encoding="utf-8")),
        )
        payload.setdefault("datasets", {})
        return payload

    def _write_registry_payload(self, payload: dict[str, Any]) -> None:
        write_json(self.registry_path, payload)

    def _dataset_dir_name(self, dataset_name: str) -> str:
        registry = self._read_registry_payload()
        datasets = _require_object("datasets", registry.get("datasets", {}))
        dataset_payload = datasets.get(dataset_name)
        if isinstance(dataset_payload, dict) and dataset_payload.get("dataset_dir"):
            return str(dataset_payload["dataset_dir"])
        return _slugify(dataset_name)

    def _snapshot_dir(self, dataset_name: str, version: str) -> Path:
        return self.root_dir / self._dataset_dir_name(dataset_name) / version

    def _update_registry(self, manifest: DatasetSnapshotManifest) -> None:
        payload = self._read_registry_payload()
        datasets = _require_object("datasets", payload.setdefault("datasets", {}))
        dataset_payload = _require_object(
            "dataset entry",
            datasets.setdefault(
                manifest.dataset_name,
                {
                    "dataset_name": manifest.dataset_name,
                    "dataset_dir": self._dataset_dir_name(manifest.dataset_name),
                    "versions": {},
                },
            ),
        )
        versions = _require_object(
            "dataset versions", dataset_payload.setdefault("versions", {})
        )
        dataset_dir_name = str(
            dataset_payload.setdefault(
                "dataset_dir", self._dataset_dir_name(manifest.dataset_name)
            )
        )
        versions[manifest.version] = {
            "kind": manifest.kind,
            "manifest_path": str(
                Path(dataset_dir_name) / manifest.version / "manifest.json"
            ),
            "created_at": manifest.created_at,
            "record_count": manifest.record_count,
            "earliest_recorded_at": manifest.earliest_recorded_at,
            "latest_recorded_at": manifest.latest_recorded_at,
        }
        dataset_payload["latest_version"] = manifest.version
        dataset_payload["updated_at"] = manifest.created_at
        self._write_registry_payload(payload)

    def _resolve_version(self, dataset_name: str, version: str | None) -> str:
        if version is not None:
            return version
        registry = self._read_registry_payload()
        datasets = _require_object("datasets", registry.get("datasets", {}))
        dataset_payload = datasets.get(dataset_name)
        if not isinstance(dataset_payload, dict):
            raise ValueError(f"unknown dataset: {dataset_name}")
        latest_version = dataset_payload.get("latest_version")
        if latest_version in (None, ""):
            raise ValueError(f"dataset has no registered versions: {dataset_name}")
        return str(latest_version)

    def write_rows_snapshot(
        self,
        dataset_name: str,
        records: Sequence[dict[str, Any]],
        *,
        version: str | None = None,
        timestamp_field: str | None = None,
        record_id_field: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DatasetSnapshotManifest:
        if not records:
            raise ValueError("records must not be empty")
        resolved_version = version or _default_version()
        snapshot_dir = self._snapshot_dir(dataset_name, resolved_version)
        if snapshot_dir.exists():
            raise ValueError(
                f"dataset snapshot already exists: {dataset_name}@{resolved_version}"
            )
        normalized_records = [
            normalize_for_json(_require_object("row record", record))
            for record in records
        ]
        snapshot_dir.mkdir(parents=True, exist_ok=False)
        write_jsonl_records(snapshot_dir / "rows.jsonl", normalized_records)
        manifest = DatasetSnapshotManifest(
            dataset_name=dataset_name,
            version=resolved_version,
            kind="rows_jsonl",
            created_at=_isoformat_utc(_utc_now()),
            records=tuple(
                DatasetSnapshotRecord(
                    record_id=(
                        str(_lookup_path(record, record_id_field))
                        if record_id_field is not None
                        and _lookup_path(record, record_id_field) not in (None, "")
                        else f"row-{index:06d}"
                    ),
                    recorded_at=_resolve_recorded_at(
                        record,
                        timestamp_field=timestamp_field,
                        candidate_fields=_ROW_TIMESTAMP_CANDIDATES,
                    ),
                    row_index=index,
                    metadata={
                        "market_key": record.get("market_key"),
                        "event_key": record.get("event_key"),
                    },
                )
                for index, record in enumerate(normalized_records)
            ),
            metadata=dict(normalize_for_json(metadata or {})),
            snapshot_dir=snapshot_dir,
        )
        _ensure_unique_record_ids(manifest.records)
        write_json(snapshot_dir / "manifest.json", manifest.to_payload())
        self._update_registry(manifest)
        return manifest

    def write_benchmark_case_snapshot(
        self,
        dataset_name: str,
        cases: Sequence[SportsBenchmarkCase | dict[str, Any]],
        *,
        version: str | None = None,
        timestamp_field: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DatasetSnapshotManifest:
        if not cases:
            raise ValueError("cases must not be empty")
        resolved_version = version or _default_version()
        snapshot_dir = self._snapshot_dir(dataset_name, resolved_version)
        if snapshot_dir.exists():
            raise ValueError(
                f"dataset snapshot already exists: {dataset_name}@{resolved_version}"
            )
        cases_dir = snapshot_dir / "cases"
        cases_dir.mkdir(parents=True, exist_ok=False)
        snapshot_records: list[DatasetSnapshotRecord] = []
        for index, case in enumerate(cases):
            normalized_case = (
                serialize_benchmark_case(case)
                if isinstance(case, SportsBenchmarkCase)
                else normalize_for_json(
                    serialize_benchmark_case(SportsBenchmarkCase.from_payload(case))
                    | {
                        key: normalize_for_json(value)
                        for key, value in case.items()
                        if key
                        not in {"name", "description", "fair_value_case", "replay_case"}
                    }
                )
            )
            case_name = str(normalized_case["name"])
            filename = f"{index:04d}-{_slugify(case_name)}.json"
            relative_path = Path("cases") / filename
            write_json(snapshot_dir / relative_path, normalized_case)
            snapshot_records.append(
                DatasetSnapshotRecord(
                    record_id=f"case-{index:06d}-{_slugify(case_name)}",
                    recorded_at=_resolve_recorded_at(
                        normalized_case,
                        timestamp_field=timestamp_field,
                        candidate_fields=_CASE_TIMESTAMP_CANDIDATES,
                    ),
                    relative_path=str(relative_path),
                    name=case_name,
                )
            )
        manifest = DatasetSnapshotManifest(
            dataset_name=dataset_name,
            version=resolved_version,
            kind="benchmark_cases",
            created_at=_isoformat_utc(_utc_now()),
            records=tuple(snapshot_records),
            metadata=dict(normalize_for_json(metadata or {})),
            snapshot_dir=snapshot_dir,
        )
        _ensure_unique_record_ids(manifest.records)
        write_json(snapshot_dir / "manifest.json", manifest.to_payload())
        self._update_registry(manifest)
        return manifest

    def load_snapshot(
        self,
        dataset_name: str,
        version: str | None = None,
    ) -> DatasetSnapshotManifest:
        resolved_version = self._resolve_version(dataset_name, version)
        snapshot_dir = self._snapshot_dir(dataset_name, resolved_version)
        manifest_path = snapshot_dir / "manifest.json"
        if not manifest_path.exists():
            raise ValueError(
                f"dataset snapshot manifest not found: {dataset_name}@{resolved_version}"
            )
        manifest = DatasetSnapshotManifest.from_payload(
            _require_object(
                "dataset snapshot manifest",
                json.loads(manifest_path.read_text(encoding="utf-8")),
            ),
            snapshot_dir=snapshot_dir,
        )
        _ensure_unique_record_ids(manifest.records)
        return manifest

    def read_rows(
        self,
        dataset_name: str,
        version: str | None = None,
    ) -> list[dict[str, Any]]:
        snapshot = self.load_snapshot(dataset_name, version)
        if snapshot.kind != "rows_jsonl":
            raise ValueError(f"dataset snapshot is not a rows snapshot: {dataset_name}")
        if snapshot.snapshot_dir is None:
            raise ValueError("rows snapshot is missing snapshot_dir context")
        return read_jsonl_records(snapshot.snapshot_dir / "rows.jsonl")

    def read_rows_by_record_ids(
        self,
        dataset_name: str,
        record_ids: Sequence[str],
        version: str | None = None,
    ) -> list[dict[str, Any]]:
        snapshot = self.load_snapshot(dataset_name, version)
        if snapshot.kind != "rows_jsonl":
            raise ValueError(f"dataset snapshot is not a rows snapshot: {dataset_name}")
        rows = self.read_rows(dataset_name, version)
        record_map = {record.record_id: record for record in snapshot.records}
        selected_rows: list[dict[str, Any]] = []
        for record_id in record_ids:
            record = record_map.get(record_id)
            if record is None or record.row_index is None:
                raise ValueError(f"unknown rows snapshot record id: {record_id}")
            selected_rows.append(dict(rows[record.row_index]))
        return selected_rows

    def benchmark_case_paths(
        self,
        dataset_name: str,
        version: str | None = None,
    ) -> tuple[Path, ...]:
        snapshot = self.load_snapshot(dataset_name, version)
        if snapshot.kind != "benchmark_cases":
            raise ValueError(
                f"dataset snapshot is not a benchmark case snapshot: {dataset_name}"
            )
        if snapshot.snapshot_dir is None:
            raise ValueError("benchmark case snapshot is missing snapshot_dir context")
        paths: list[Path] = []
        for record in snapshot.records:
            if record.relative_path is None:
                raise ValueError(
                    f"benchmark case snapshot record is missing relative_path: {record.record_id}"
                )
            paths.append(snapshot.snapshot_dir / record.relative_path)
        return tuple(paths)

    def benchmark_case_paths_by_record_ids(
        self,
        dataset_name: str,
        record_ids: Sequence[str],
        version: str | None = None,
    ) -> tuple[Path, ...]:
        snapshot = self.load_snapshot(dataset_name, version)
        if snapshot.kind != "benchmark_cases":
            raise ValueError(
                f"dataset snapshot is not a benchmark case snapshot: {dataset_name}"
            )
        if snapshot.snapshot_dir is None:
            raise ValueError("benchmark case snapshot is missing snapshot_dir context")
        record_map = {record.record_id: record for record in snapshot.records}
        paths: list[Path] = []
        for record_id in record_ids:
            record = record_map.get(record_id)
            if record is None or record.relative_path is None:
                raise ValueError(
                    f"unknown benchmark case snapshot record id: {record_id}"
                )
            paths.append(snapshot.snapshot_dir / record.relative_path)
        return tuple(paths)


def generate_walk_forward_splits(
    snapshot: DatasetSnapshotManifest,
    *,
    min_train_size: int,
    test_size: int,
    step_size: int | None = None,
    max_splits: int | None = None,
) -> tuple[WalkForwardSplit, ...]:
    if min_train_size <= 0:
        raise ValueError("min_train_size must be positive")
    if test_size <= 0:
        raise ValueError("test_size must be positive")
    resolved_step_size = step_size or test_size
    if resolved_step_size <= 0:
        raise ValueError("step_size must be positive")
    if max_splits is not None and max_splits <= 0:
        raise ValueError("max_splits must be positive when provided")

    missing_record_ids = [
        record.record_id for record in snapshot.records if record.recorded_at is None
    ]
    if missing_record_ids:
        raise ValueError(
            "walk-forward splits require recorded_at for every record; missing: "
            + ", ".join(missing_record_ids)
        )

    ordered_records = sorted(
        snapshot.records,
        key=lambda record: (
            parse_timestamp(record.recorded_at),
            record.record_id,
        ),
    )
    splits: list[WalkForwardSplit] = []
    train_end_index = min_train_size
    while train_end_index + test_size <= len(ordered_records):
        train_records = ordered_records[:train_end_index]
        test_records = ordered_records[train_end_index : train_end_index + test_size]
        splits.append(
            WalkForwardSplit(
                split_index=len(splits),
                train_record_ids=_ordered_unique(
                    record.record_id for record in train_records
                ),
                test_record_ids=_ordered_unique(
                    record.record_id for record in test_records
                ),
                train_start_at=train_records[0].recorded_at or "",
                train_end_at=train_records[-1].recorded_at or "",
                test_start_at=test_records[0].recorded_at or "",
                test_end_at=test_records[-1].recorded_at or "",
            )
        )
        if max_splits is not None and len(splits) >= max_splits:
            break
        train_end_index += resolved_step_size
    return tuple(splits)
