from __future__ import annotations

import os
import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable

from .database import StatsDatabase, safe_default_root_path


ScanProgressCallback = Callable[[str], None]
ArchiveProgressCallback = Callable[[dict], None]
LogCallback = Callable[[str, str], None]
TranslateCallback = Callable[[str], str]


@dataclass(slots=True)
class FolderCandidate:
    path: Path
    folder_name: str
    modified_at: datetime
    size_bytes: int
    relative_parts: tuple[str, ...]
    archive_name: str


@dataclass(slots=True)
class ArchiveBatchResult:
    successful_archives: list[dict]
    deleted_source_paths: list[str]
    failed_archives: list[dict]


def format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{num_bytes} B"


class ProjectArchiverEngine:
    def __init__(
        self,
        root_path: str | Path = safe_default_root_path(),
        *,
        threshold_days: int = 30,
        blacklist: Iterable[str] | None = None,
        stats_db: StatsDatabase | None = None,
        translate: Callable[..., str] | None = None,
    ) -> None:
        self.root_path = Path(root_path)
        self.threshold_days = threshold_days
        self.blacklist = {item.casefold() for item in (blacklist or [])}
        self.stats_db = stats_db
        self.translate = translate or self._fallback_translate

    def _fallback_translate(self, key: str, **kwargs) -> str:
        return key.format(**kwargs) if kwargs else key

    def _is_within_root_jail(self, candidate_path: Path) -> bool:
        resolved_root = self.root_path.resolve(strict=False)
        resolved_candidate = candidate_path.resolve(strict=False)
        return resolved_candidate.is_relative_to(resolved_root)

    def set_blacklist(self, blacklist: Iterable[str]) -> None:
        self.blacklist = {item.casefold() for item in blacklist}

    def scan_candidates(
        self,
        *,
        log_callback: LogCallback | None = None,
        progress_callback: ScanProgressCallback | None = None,
    ) -> list[FolderCandidate]:
        candidates: list[FolderCandidate] = []
        if not self.root_path.exists():
            raise FileNotFoundError(f"Root path not found: {self.root_path}")

        cutoff = datetime.now() - timedelta(days=self.threshold_days)
        progress_callback = progress_callback or (lambda _message: None)
        log_callback = log_callback or (lambda _message, _level="INFO": None)

        log_callback(self.translate("engine_scan_root", path=self.root_path))
        project_paths = self._discover_project_paths(self.root_path, progress_callback)
        for project_path in project_paths:
            candidate = self._build_candidate(project_path, cutoff)
            if candidate is not None:
                candidates.append(candidate)

        candidates.sort(key=lambda item: item.modified_at)
        log_callback(self.translate("engine_scan_complete", count=len(candidates)))
        return candidates

    def _discover_project_paths(
        self,
        current_path: Path,
        progress_callback: ScanProgressCallback,
    ) -> list[Path]:
        progress_callback(self.translate("engine_inspecting", path=current_path))

        if self._is_month_folder(current_path):
            return self._list_direct_child_dirs(current_path)

        if self._is_year_folder(current_path):
            projects: list[Path] = []
            for month_path in self._list_direct_child_dirs(current_path):
                if self._is_month_folder(month_path):
                    progress_callback(self.translate("engine_month_detected", path=month_path))
                    projects.extend(self._list_direct_child_dirs(month_path))
            return projects

        found: list[Path] = []
        for child_path in self._list_direct_child_dirs(current_path):
            if self._is_month_folder(child_path):
                progress_callback(self.translate("engine_month_detected", path=child_path))
                found.extend(self._list_direct_child_dirs(child_path))
                continue

            if self._is_year_folder(child_path):
                progress_callback(self.translate("engine_year_detected", path=child_path))
                found.extend(self._discover_project_paths(child_path, progress_callback))
                continue

            found.extend(self._discover_project_paths(child_path, progress_callback))
        return found

    def _build_candidate(self, folder_path: Path, cutoff: datetime) -> FolderCandidate | None:
        try:
            modified_at = datetime.fromtimestamp(folder_path.stat().st_mtime)
        except OSError:
            return None

        if modified_at > cutoff:
            return None

        relative_parts = self._extract_project_parts(folder_path)
        if relative_parts is None:
            return None

        size_bytes = self._folder_size(folder_path)
        if size_bytes <= 0:
            return None

        archive_name = self.build_archive_name(folder_path, relative_parts)
        return FolderCandidate(
            path=folder_path,
            folder_name=folder_path.name,
            modified_at=modified_at,
            size_bytes=size_bytes,
            relative_parts=relative_parts,
            archive_name=archive_name,
        )

    def _relative_parts(self, folder_path: Path) -> tuple[str, ...]:
        try:
            relative_path = folder_path.relative_to(self.root_path)
            return relative_path.parts
        except ValueError:
            return (folder_path.name,)

    def _extract_project_parts(self, folder_path: Path) -> tuple[str, str, str, str] | None:
        project_name = folder_path.name
        month_folder = folder_path.parent
        year_folder = month_folder.parent
        customer_folder = year_folder.parent

        if not month_folder or not year_folder or not customer_folder:
            return None
        if month_folder == folder_path or year_folder == month_folder or customer_folder == year_folder:
            return None
        if not self._is_month_folder(month_folder):
            return None
        if not self._is_year_folder(year_folder):
            return None

        return (
            customer_folder.name,
            year_folder.name,
            month_folder.name,
            project_name,
        )

    def _list_direct_child_dirs(self, folder_path: Path) -> list[Path]:
        directories: list[Path] = []
        try:
            with os.scandir(folder_path) as entries:
                for entry in entries:
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    if entry.name.casefold() in self.blacklist:
                        continue
                    directories.append(Path(entry.path))
        except PermissionError:
            return []
        directories.sort(key=lambda item: item.name.casefold())
        return directories

    def _is_year_folder(self, folder_path: Path) -> bool:
        return bool(re.fullmatch(r"\d{4}", folder_path.name.strip()))

    def _is_month_folder(self, folder_path: Path) -> bool:
        return bool(re.fullmatch(r"\d{1,2}\s*-\s*.+", folder_path.name.strip()))

    def _clean_month_name(self, month_name: str) -> str:
        cleaned = re.sub(r"^\d{1,2}\s*-\s*", "", month_name.strip())
        return cleaned or month_name.strip()

    def _folder_size(self, folder_path: Path) -> int:
        total = 0
        stack = [folder_path]
        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        if entry.is_dir(follow_symlinks=False):
                            if entry.name.casefold() in self.blacklist:
                                continue
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            try:
                                total += entry.stat(follow_symlinks=False).st_size
                            except OSError:
                                continue
            except PermissionError:
                continue
        return total

    def build_archive_name(self, folder_path: Path, relative_parts: tuple[str, ...]) -> str:
        client = relative_parts[0] if len(relative_parts) >= 1 else "Unknown Client"
        if len(relative_parts) >= 2:
            year = relative_parts[1]
        else:
            year = datetime.fromtimestamp(folder_path.stat().st_mtime).strftime("%Y")
        raw_month = relative_parts[2] if len(relative_parts) >= 3 else folder_path.parent.name
        month = self._clean_month_name(raw_month)
        project_name = folder_path.name

        base_name = f"{client} - {year} - {month} {project_name}"
        safe_name = "".join(char if char not in '<>:"/\\|?*' else "_" for char in base_name).strip()
        zip_path = folder_path.parent / f"{safe_name}.zip"
        if not zip_path.exists():
            return zip_path.name

        version = 2
        while True:
            versioned_name = f"{safe_name}.v{version}.zip"
            if not (folder_path.parent / versioned_name).exists():
                return versioned_name
            version += 1

    def archive_batch(
        self,
        candidates: list[FolderCandidate],
        *,
        progress_callback: ArchiveProgressCallback | None = None,
        log_callback: LogCallback | None = None,
    ) -> ArchiveBatchResult:
        progress_callback = progress_callback or (lambda _payload: None)
        log_callback = log_callback or (lambda _message, _level="INFO": None)

        safe_to_delete: list[dict] = []
        failures: list[dict] = []
        total = len(candidates)

        for index, candidate in enumerate(candidates, start=1):
            progress_callback(
                {
                    "type": "batch",
                    "phase": "archive",
                    "index": index,
                    "total": total,
                    "percent": (index - 1) / total if total else 0.0,
                    "label": self.translate("phase_archive_label", index=index, total=total),
                }
            )
            try:
                result = self._create_verified_archive(
                    candidate,
                    progress_callback=progress_callback,
                    log_callback=log_callback,
                )
                safe_to_delete.append(result)
            except Exception as exc:  # noqa: BLE001
                failures.append({"source_path": str(candidate.path), "error": str(exc)})
                log_callback(
                    self.translate("engine_archive_failed", name=candidate.folder_name, error=exc),
                    "ERROR",
                )

        deleted_source_paths: list[str] = []
        safe_total = len(safe_to_delete)
        for index, result in enumerate(safe_to_delete, start=1):
            progress_callback(
                {
                    "type": "batch",
                    "phase": "cleanup",
                    "index": index,
                    "total": safe_total,
                    "percent": (index - 1) / safe_total if safe_total else 1.0,
                    "label": self.translate("phase_cleanup_label", index=index, total=safe_total),
                }
            )
            try:
                self._delete_original_folder_after_final_check(result, log_callback=log_callback)
                if self.stats_db is not None:
                    self.stats_db.record_archive(**result)
                deleted_source_paths.append(result["source_path"])
            except Exception as exc:  # noqa: BLE001
                failures.append({"source_path": result["source_path"], "error": str(exc)})
                log_callback(
                    self.translate(
                        "engine_cleanup_aborted",
                        name=Path(result["source_path"]).name,
                        error=exc,
                    ),
                    "ERROR",
                )

        progress_callback(
            {
                "type": "batch",
                "phase": "done",
                "index": total,
                "total": total,
                "percent": 1.0,
                "label": self.translate("phase_done_label"),
            }
        )
        return ArchiveBatchResult(
            successful_archives=safe_to_delete,
            deleted_source_paths=deleted_source_paths,
            failed_archives=failures,
        )

    def _create_verified_archive(
        self,
        candidate: FolderCandidate,
        *,
        progress_callback: ArchiveProgressCallback | None = None,
        log_callback: LogCallback | None = None,
    ) -> dict:
        progress_callback = progress_callback or (lambda _payload: None)
        log_callback = log_callback or (lambda _message, _level="INFO": None)

        all_files = self._collect_files(candidate.path)
        total_bytes = sum(size for _, size in all_files)
        if total_bytes <= 0:
            raise ValueError(f"Folder is empty or unreadable: {candidate.path}")

        archive_path = candidate.path.parent / candidate.archive_name
        processed_bytes = 0
        log_callback(self.translate("engine_phase1_create", name=archive_path.name))
        progress_callback(
            {
                "type": "item",
                "label": candidate.folder_name,
                "processed_bytes": 0,
                "total_bytes": total_bytes,
                "percent": 0.0,
            }
        )

        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_handle:
            for file_path, file_size in all_files:
                arcname = file_path.relative_to(candidate.path.parent)
                zip_handle.write(file_path, arcname=arcname)
                processed_bytes += file_size
                percent = processed_bytes / total_bytes if total_bytes else 0.0
                progress_callback(
                    {
                        "type": "item",
                        "label": candidate.folder_name,
                        "processed_bytes": processed_bytes,
                        "total_bytes": total_bytes,
                        "percent": percent,
                    }
                )

        log_callback(self.translate("engine_phase1_test", name=archive_path.name))
        with zipfile.ZipFile(archive_path, "r") as zip_handle:
            invalid_member = zip_handle.testzip()
        if invalid_member is not None:
            archive_path.unlink(missing_ok=True)
            raise ValueError(f"Integrity check failed for {archive_path.name}: {invalid_member}")

        archive_exists = archive_path.exists()
        archive_size = archive_path.stat().st_size if archive_exists else 0
        if not archive_exists:
            raise ValueError(f"Archive file not found after creation: {archive_path.name}")
        if archive_size <= 0:
            archive_path.unlink(missing_ok=True)
            raise ValueError(f"Archive size validation failed for {archive_path.name}")

        saved_bytes = max(candidate.size_bytes - archive_size, 0)
        progress_callback(
            {
                "type": "item",
                "label": candidate.folder_name,
                "processed_bytes": total_bytes,
                "total_bytes": total_bytes,
                "percent": 1.0,
            }
        )
        log_callback(self.translate("engine_phase1_safe", name=archive_path.name), "SUCCESS")
        return {
            "source_path": str(candidate.path),
            "archive_path": str(archive_path),
            "original_size": candidate.size_bytes,
            "archive_size": archive_size,
            "saved_bytes": saved_bytes,
        }

    def _delete_original_folder_after_final_check(
        self,
        archive_result: dict,
        *,
        log_callback: LogCallback | None = None,
    ) -> None:
        log_callback = log_callback or (lambda _message, _level="INFO": None)

        source_path = Path(archive_result["source_path"])
        archive_path = Path(archive_result["archive_path"])

        if not self._is_within_root_jail(source_path):
            raise ValueError(
                f"Safety abort: source '{source_path}' is outside "
                f"root '{self.root_path}'. Deletion cancelled."
            )

        if not archive_path.exists():
            raise ValueError(f"Final check failed: archive missing on disk for {source_path.name}")

        resolved_archive = archive_path.resolve(strict=True)
        if resolved_archive.stat().st_size <= 0:
            raise ValueError(f"Final check failed: archive size is zero for {archive_path.name}")

        try:
            with zipfile.ZipFile(resolved_archive, "r") as zip_handle:
                invalid_member = zip_handle.testzip()
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Final check failed: cannot reopen {archive_path.name}") from exc

        if invalid_member is not None:
            raise ValueError(f"Final check failed: corrupt member {invalid_member} in {archive_path.name}")

        if not source_path.exists():
            log_callback(self.translate("engine_final_check_missing", name=source_path.name), "WARNING")
            return

        log_callback(self.translate("engine_final_check_delete", name=source_path.name))
        shutil.rmtree(source_path.resolve(strict=True))

    def _collect_files(self, folder_path: Path) -> list[tuple[Path, int]]:
        files: list[tuple[Path, int]] = []
        stack = [folder_path]
        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        if entry.is_dir(follow_symlinks=False):
                            if entry.name.casefold() in self.blacklist:
                                continue
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            file_path = Path(entry.path)
                            try:
                                file_size = entry.stat(follow_symlinks=False).st_size
                            except OSError:
                                continue
                            files.append((file_path, file_size))
            except PermissionError:
                continue
        files.sort(key=lambda item: str(item[0]).lower())
        return files
