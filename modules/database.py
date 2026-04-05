from __future__ import annotations

import json
import os
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


def safe_default_root_path() -> str:
    home = Path.home()
    preferred = home / "Projects"
    return str(preferred if preferred.exists() else home)


def current_month_key() -> str:
    return datetime.now().strftime("%Y-%m")


DEFAULT_STATS: dict[str, Any] = {
    "totals": {
        "folders_archived": 0,
        "space_saved_bytes": 0,
    },
    "monthly": {},
    "blacklist": [],
    "history": [],
}

DEFAULT_SETTINGS: dict[str, Any] = {
    "root_path": safe_default_root_path(),
}


@dataclass(slots=True)
class DashboardStats:
    total_saved_bytes: int
    saved_this_month_bytes: int
    total_folders_archived: int
    folders_archived_this_month: int
    blacklist: list[str]


class StatsDatabase:
    def __init__(self, file_path: str | Path = "stats.json") -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.file_path.exists():
            data = deepcopy(DEFAULT_STATS)
            self._write(data)
            return data

        try:
            with self.file_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, OSError):
            data = deepcopy(DEFAULT_STATS)
            self._write(data)
            return data

        merged = deepcopy(DEFAULT_STATS)
        merged["totals"].update(data.get("totals", {}))
        merged["monthly"].update(data.get("monthly", {}))
        merged["history"] = data.get("history", [])
        merged["blacklist"] = sorted(set(data.get("blacklist", [])))
        self._write(merged)
        return merged

    def _write(self, data: dict[str, Any] | None = None) -> None:
        payload = data if data is not None else self._data
        temp_path = self.file_path.with_suffix(f"{self.file_path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        os.replace(temp_path, self.file_path)

    def reload(self) -> None:
        self._data = self._load()

    def get_dashboard_stats(self) -> DashboardStats:
        month_data = self._data["monthly"].get(
            current_month_key(),
            {"folders_archived": 0, "space_saved_bytes": 0},
        )
        return DashboardStats(
            total_saved_bytes=int(self._data["totals"]["space_saved_bytes"]),
            saved_this_month_bytes=int(month_data["space_saved_bytes"]),
            total_folders_archived=int(self._data["totals"]["folders_archived"]),
            folders_archived_this_month=int(month_data["folders_archived"]),
            blacklist=list(self._data["blacklist"]),
        )

    def get_blacklist(self) -> list[str]:
        return list(self._data["blacklist"])

    def replace_blacklist(self, items: list[str]) -> None:
        cleaned = sorted({item.strip() for item in items if item.strip()})
        self._data["blacklist"] = cleaned
        self._write()

    def record_archive(
        self,
        *,
        source_path: str,
        archive_path: str,
        original_size: int,
        archive_size: int,
        saved_bytes: int,
    ) -> None:
        month_key = current_month_key()
        monthly_data = self._data["monthly"].setdefault(
            month_key,
            {"folders_archived": 0, "space_saved_bytes": 0},
        )
        self._data["totals"]["folders_archived"] += 1
        self._data["totals"]["space_saved_bytes"] += saved_bytes
        monthly_data["folders_archived"] += 1
        monthly_data["space_saved_bytes"] += saved_bytes
        self._data["history"].append(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "source_path": source_path,
                "archive_path": archive_path,
                "original_size": original_size,
                "archive_size": archive_size,
                "saved_bytes": saved_bytes,
            }
        )
        self._write()


class SettingsDatabase:
    def __init__(self, file_path: str | Path = "settings.json") -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.file_path.exists():
            data = deepcopy(DEFAULT_SETTINGS)
            self._write(data)
            return data

        try:
            with self.file_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, OSError):
            data = deepcopy(DEFAULT_SETTINGS)
            self._write(data)
            return data

        merged = deepcopy(DEFAULT_SETTINGS)
        merged.update(data)
        self._write(merged)
        return merged

    def _write(self, data: dict[str, Any] | None = None) -> None:
        payload = data if data is not None else self._data
        temp_path = self.file_path.with_suffix(f"{self.file_path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        os.replace(temp_path, self.file_path)

    def get_root_path(self) -> str:
        return str(self._data.get("root_path", DEFAULT_SETTINGS["root_path"]))

    def set_root_path(self, root_path: str | Path) -> None:
        self._data["root_path"] = str(root_path)
        self._write()
