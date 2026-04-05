from __future__ import annotations

import re
import os
import queue
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

from .database import DashboardStats, SettingsDatabase, StatsDatabase, safe_default_root_path
from .engine import ArchiveBatchResult, FolderCandidate, ProjectArchiverEngine, format_bytes
from .translations import TranslationManager


SURFACE = "#131313"
SURFACE_LOW = "#1c1b1b"
SURFACE_CARD = "#201f1f"
SURFACE_VARIANT = "#353534"
SURFACE_STATUS = "#181717"
INPUT_BG = "#1e1e1e"
TEXT_PRIMARY = "#e5e2e1"
TEXT_SECONDARY = "#8b90a0"
ACCENT = "#4b8eff"
ACCENT_TEXT = "#00285c"
SCAN_INFO_BG = "#16232e"
SCAN_INFO_TEXT = "#b9dfff"

TABLE_COLUMN_SPECS = (
    {"minsize": 52, "weight": 0, "uniform": None},
    {"minsize": 320, "weight": 1, "uniform": "table"},
    {"minsize": 168, "weight": 0, "uniform": None},
    {"minsize": 96, "weight": 0, "uniform": None},
    {"minsize": 140, "weight": 0, "uniform": None},
    {"minsize": 104, "weight": 0, "uniform": None},
)


@dataclass(slots=True)
class RowState:
    candidate: FolderCandidate
    variable: tk.BooleanVar
    frame: ctk.CTkFrame
    status_label: ctk.CTkLabel


class ProjectArchiverApp(ctk.CTk):
    POLL_INTERVAL_MS = 120

    def __init__(self) -> None:
        super().__init__()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.title("Project Archiver Tool")
        self.geometry("1440x920")
        self.minsize(1200, 760)
        self.configure(fg_color=SURFACE)

        self.project_root = Path(__file__).resolve().parent.parent
        self.settings_db = SettingsDatabase(self.project_root / "settings.json")
        configured_root = Path(self.settings_db.get_root_path())
        self.base_root_path = configured_root if configured_root.exists() else Path(safe_default_root_path())
        self.lang_manager = TranslationManager("PT")
        self.db = StatsDatabase(self.project_root / "stats.json")
        self.engine = ProjectArchiverEngine(
            root_path=self.base_root_path,
            blacklist=self.db.get_blacklist(),
            stats_db=self.db,
            translate=self.lang_manager.get,
        )
        self.ui_queue: queue.Queue[dict] = queue.Queue()
        self.scan_results: list[FolderCandidate] = []
        self.row_states: list[RowState] = []
        self.is_busy = False
        self.period_path_map: dict[str, Path] = {}
        self.current_scan_path = self.base_root_path

        self._build_ui()
        self._initialize_path_selectors()
        self._load_dashboard_metrics()
        self.apply_translations()
        self._start_queue_poller()
        self.log(self.t("logs_init"))

    def t(self, key: str, **kwargs) -> str:
        return self.lang_manager.get(key, **kwargs)

    def on_language_change(self, language: str) -> None:
        current_client = self.client_combobox.get()
        current_scan_path = self.current_scan_path
        self.lang_manager.set_language(language)
        self.engine.translate = self.lang_manager.get
        if self.client_combobox.cget("state") != "disabled" and current_client:
            self.on_client_change(current_client)
            for label, path in self.period_path_map.items():
                if path == current_scan_path:
                    self.period_combobox.set(label)
                    self.on_period_change(label)
                    break
        self.apply_translations()

    def apply_translations(self) -> None:
        self.title(self.t("app_title"))
        self.sidebar_title_label.configure(text=self.t("sidebar_title"))
        self.metrics_title_label.configure(text=self.t("system_metrics"))
        self.language_label.configure(text=self.t("language"))
        self.client_label.configure(text=self.t("select_client"))
        self.period_label.configure(text=self.t("select_period"))
        self.blacklist_button.configure(text=self.t("blacklist_btn"))
        self.root_title_label.configure(text=self.t("root_path"))
        self.change_root_button.configure(text=self.t("change_root"))
        self.threshold_label.configure(text=self.t("threshold_label"))
        self.scan_button.configure(text=self.t("scan_btn"))
        self.archive_button.configure(text=self.t("archive_btn"))
        self.status_label.configure(text=self.t("system_ready"))
        self.global_progress_label.configure(text=self.t("global_operation"))
        self.item_progress_label.configure(text=self.t("current_item"))

        metric_titles = [
            self.t("metric_total_saved"),
            self.t("metric_saved_month"),
            self.t("metric_folders_count"),
            self.t("metric_folders_month"),
        ]
        for label, text in zip(self.metric_title_labels, metric_titles):
            label.configure(text=text)

        header_titles = [
            self.t("header_folder"),
            self.t("header_date"),
            self.t("header_days"),
            self.t("header_size"),
            self.t("header_status"),
        ]
        for label, text in zip(self.table_header_labels, header_titles):
            label.configure(text=text)

        self._refresh_selector_placeholders()
        self._refresh_empty_state_text()
        self._update_scan_summary(self.scan_results)
        self._render_rows(self.scan_results)
        self._update_root_path_label()

    def _refresh_selector_placeholders(self) -> None:
        if self.client_combobox.cget("state") == "disabled" and self.client_combobox.get() != self.t("no_client"):
            self.client_combobox.configure(values=[self.t("no_client")])
            self.client_combobox.set(self.t("no_client"))

        if self.period_combobox.cget("state") == "disabled" and self.period_combobox.get() != self.t("no_period"):
            self.period_combobox.configure(values=[self.t("no_period")])
            self.period_combobox.set(self.t("no_period"))

    def _format_root_path_label(self, path: Path) -> str:
        full_path = str(path)
        max_chars = 28
        if len(full_path) <= max_chars:
            return full_path
        return f"...{full_path[-(max_chars - 3):]}"

    def _update_root_path_label(self) -> None:
        self.root_path_label.configure(text=self._format_root_path_label(self.base_root_path))

    def _refresh_empty_state_text(self) -> None:
        if not hasattr(self, "empty_state"):
            return
        if self.scan_results:
            return
        current = self.empty_state.cget("text")
        if "matched" in current or "correspondeu" in current:
            self.empty_state.configure(text=self.t("empty_no_matches"))
        else:
            self.empty_state.configure(text=self.t("empty_before_scan"))

    def _build_ui(self) -> None:
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.sidebar = ctk.CTkFrame(self, width=250, corner_radius=0, fg_color=SURFACE_LOW)
        self.sidebar.grid(row=0, column=0, sticky="nsw")
        self.sidebar.grid_propagate(False)

        self.main_panel = ctk.CTkFrame(self, corner_radius=0, fg_color=SURFACE)
        self.main_panel.grid(row=0, column=1, sticky="nsew")
        self.main_panel.grid_columnconfigure(0, weight=1)
        self.main_panel.grid_rowconfigure(1, weight=1)

        self._build_sidebar()
        self._build_main_panel()

    def _build_sidebar(self) -> None:
        self.sidebar_title_label = ctk.CTkLabel(
            self.sidebar,
            text="",
            text_color=TEXT_PRIMARY,
            font=ctk.CTkFont(size=22, weight="bold"),
            anchor="w",
        )
        self.sidebar_title_label.pack(fill="x", padx=24, pady=(26, 16))

        self.metrics_title_label = ctk.CTkLabel(
            self.sidebar,
            text="",
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=11, weight="bold"),
            anchor="w",
        )
        self.metrics_title_label.pack(fill="x", padx=24, pady=(4, 10))

        self.total_saved_value = self._build_metric("Total Saved Space")
        self.saved_month_value = self._build_metric("Saved this Month")
        self.total_folders_value = self._build_metric("Folders Count")
        self.month_folders_value = self._build_metric("Folders This Month")

        footer = ctk.CTkFrame(self.sidebar, fg_color=SURFACE_STATUS, corner_radius=0)
        footer.pack(side="bottom", fill="x")

        self.language_label = ctk.CTkLabel(
            footer,
            text="",
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=11, weight="bold"),
            anchor="w",
        )
        self.language_label.pack(fill="x", padx=24, pady=(18, 6))

        self.language_toggle = ctk.CTkSegmentedButton(
            footer,
            values=["PT", "EN"],
            command=self.on_language_change,
            fg_color=INPUT_BG,
            selected_color=ACCENT,
            selected_hover_color="#629dff",
            unselected_color=SURFACE_CARD,
            unselected_hover_color=SURFACE_VARIANT,
            text_color=TEXT_PRIMARY,
        )
        self.language_toggle.pack(fill="x", padx=24)
        self.language_toggle.set("PT")

        self._build_path_selectors(footer)

        self.blacklist_button = ctk.CTkButton(
            footer,
            text="",
            command=self.open_blacklist_dialog,
            fg_color=SURFACE_CARD,
            hover_color=SURFACE_VARIANT,
            text_color=TEXT_PRIMARY,
            height=42,
            corner_radius=8,
        )
        self.blacklist_button.pack(fill="x", padx=24, pady=(18, 12))

        self.root_title_label = ctk.CTkLabel(
            footer,
            text="",
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=10, weight="bold"),
            anchor="w",
        )
        self.root_title_label.pack(fill="x", padx=24)

        root_row = ctk.CTkFrame(footer, fg_color="transparent")
        root_row.pack(fill="x", padx=24, pady=(4, 18))
        root_row.grid_columnconfigure(0, weight=1)

        self.root_path_label = ctk.CTkLabel(
            root_row,
            text=str(self.engine.root_path),
            text_color=ACCENT,
            font=ctk.CTkFont(size=13, family="Consolas"),
            anchor="w",
        )
        self.root_path_label.grid(row=0, column=0, sticky="ew")

        self.change_root_button = ctk.CTkButton(
            root_row,
            text="",
            command=self.change_root_path,
            fg_color=SURFACE_CARD,
            hover_color=SURFACE_VARIANT,
            text_color=TEXT_PRIMARY,
            width=64,
            height=28,
            corner_radius=8,
            font=ctk.CTkFont(size=11, weight="bold"),
        )
        self.change_root_button.grid(row=0, column=1, padx=(8, 0), sticky="e")

    def _build_path_selectors(self, parent: ctk.CTkFrame) -> None:
        self.client_label = ctk.CTkLabel(
            parent,
            text="",
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=11, weight="bold"),
            anchor="w",
        )
        self.client_label.pack(fill="x", padx=24, pady=(18, 6))

        self.client_combobox = ctk.CTkComboBox(
            parent,
            values=[""],
            command=self.on_client_change,
            state="readonly",
            height=38,
            corner_radius=8,
            fg_color=INPUT_BG,
            border_color=ACCENT,
            button_color=ACCENT,
            button_hover_color="#629dff",
            dropdown_fg_color=INPUT_BG,
            dropdown_hover_color=SURFACE_VARIANT,
            dropdown_text_color=TEXT_PRIMARY,
            text_color=TEXT_PRIMARY,
        )
        self.client_combobox.pack(fill="x", padx=24)

        self.period_label = ctk.CTkLabel(
            parent,
            text="",
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=11, weight="bold"),
            anchor="w",
        )
        self.period_label.pack(fill="x", padx=24, pady=(14, 6))

        self.period_combobox = ctk.CTkComboBox(
            parent,
            values=[""],
            command=self.on_period_change,
            state="readonly",
            height=38,
            corner_radius=8,
            fg_color=INPUT_BG,
            border_color=ACCENT,
            button_color=ACCENT,
            button_hover_color="#629dff",
            dropdown_fg_color=INPUT_BG,
            dropdown_hover_color=SURFACE_VARIANT,
            dropdown_text_color=TEXT_PRIMARY,
            text_color=TEXT_PRIMARY,
        )
        self.period_combobox.pack(fill="x", padx=24)

    def _build_metric(self, label: str) -> ctk.CTkLabel:
        wrapper = ctk.CTkFrame(self.sidebar, fg_color="transparent")
        wrapper.pack(fill="x", padx=24, pady=6)
        title_label = ctk.CTkLabel(
            wrapper,
            text=label,
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=12),
            anchor="w",
        )
        title_label.pack(fill="x")
        value = ctk.CTkLabel(
            wrapper,
            text="0",
            text_color=ACCENT,
            font=ctk.CTkFont(size=20, weight="bold"),
            anchor="w",
        )
        value.pack(fill="x", pady=(4, 0))
        if not hasattr(self, "metric_title_labels"):
            self.metric_title_labels = []
        self.metric_title_labels.append(title_label)
        return value

    def _build_main_panel(self) -> None:
        topbar = ctk.CTkFrame(self.main_panel, fg_color=SURFACE, corner_radius=0, height=72)
        topbar.grid(row=0, column=0, sticky="ew", padx=0, pady=0)
        topbar.grid_columnconfigure(0, weight=1)

        self.breadcrumb_label = ctk.CTkLabel(
            topbar,
            text=r"G: > Work",
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=12, weight="bold"),
            anchor="w",
        )
        self.breadcrumb_label.grid(row=0, column=0, padx=(26, 12), pady=18, sticky="w")

        threshold_chip = ctk.CTkFrame(topbar, fg_color=SURFACE_LOW, corner_radius=8)
        threshold_chip.grid(row=0, column=1, padx=12, pady=16)
        self.threshold_label = ctk.CTkLabel(
            threshold_chip,
            text="",
            text_color=ACCENT,
            font=ctk.CTkFont(size=12, weight="bold"),
        )
        self.threshold_label.pack(padx=14, pady=10)

        self.scan_button = ctk.CTkButton(
            topbar,
            text="",
            command=self.start_scan,
            fg_color=ACCENT,
            hover_color="#629dff",
            text_color=ACCENT_TEXT,
            width=124,
            height=42,
            corner_radius=8,
            font=ctk.CTkFont(size=13, weight="bold"),
        )
        self.scan_button.grid(row=0, column=2, padx=(0, 26), pady=16)

        explorer_card = ctk.CTkFrame(self.main_panel, fg_color=SURFACE, corner_radius=0)
        explorer_card.grid(row=1, column=0, sticky="nsew", padx=22, pady=(0, 210))
        explorer_card.grid_columnconfigure(0, weight=1)
        explorer_card.grid_rowconfigure(2, weight=1)

        self.scan_result_bar = ctk.CTkFrame(
            explorer_card,
            fg_color=SCAN_INFO_BG,
            corner_radius=10,
            height=52,
        )
        self.scan_result_bar.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        self.scan_result_bar.grid_columnconfigure((0, 1, 2), weight=1)

        self.scan_found_label = self._build_scan_summary_label(
            self.scan_result_bar,
            "",
        )
        self.scan_found_label.grid(row=0, column=0, sticky="w", padx=(18, 12), pady=14)

        self.scan_size_label = self._build_scan_summary_label(
            self.scan_result_bar,
            "",
        )
        self.scan_size_label.grid(row=0, column=1, sticky="w", padx=12, pady=14)

        self.scan_savings_label = self._build_scan_summary_label(
            self.scan_result_bar,
            "",
        )
        self.scan_savings_label.grid(row=0, column=2, sticky="w", padx=12, pady=14)

        header_frame = ctk.CTkFrame(explorer_card, fg_color=SURFACE, corner_radius=0)
        header_frame.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        self._configure_table_columns(header_frame)

        self.select_all_var = tk.BooleanVar(value=False)
        select_all = ctk.CTkCheckBox(
            header_frame,
            text="",
            variable=self.select_all_var,
            command=self.toggle_all_rows,
            checkbox_width=18,
            checkbox_height=18,
            border_width=1,
            fg_color=ACCENT,
            hover_color="#629dff",
        )
        select_all.grid(row=0, column=0, padx=(14, 10), pady=(6, 6), sticky="w")

        headers = ["", "", "", "", ""]
        self.table_header_labels: list[ctk.CTkLabel] = []
        for idx, title in enumerate(headers, start=1):
            label = ctk.CTkLabel(
                header_frame,
                text=title.upper(),
                text_color=TEXT_SECONDARY,
                font=ctk.CTkFont(size=11, weight="bold"),
                anchor="w" if idx < 4 else "e",
            )
            padx = (0, 0) if idx < 4 else (0, 14)
            sticky = "w"
            label.grid(row=0, column=idx, padx=padx, pady=6, sticky=sticky)
            self.table_header_labels.append(label)

        self.rows_container = ctk.CTkScrollableFrame(
            explorer_card,
            fg_color="transparent",
            corner_radius=0,
            scrollbar_button_color=SURFACE_VARIANT,
            scrollbar_button_hover_color="#4b4b4b",
        )
        self.rows_container.grid(row=2, column=0, sticky="nsew")
        self.rows_container.grid_columnconfigure(0, weight=1)

        self.empty_state = ctk.CTkLabel(
            self.rows_container,
            text="",
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=14),
        )
        self.empty_state.grid(row=0, column=0, padx=12, pady=32, sticky="n")

        self._build_bottom_dock()

    def _build_bottom_dock(self) -> None:
        dock = ctk.CTkFrame(self.main_panel, fg_color=SURFACE_LOW, corner_radius=0, height=190)
        dock.place(relx=0, rely=1, relwidth=1, anchor="sw", y=0)

        progress_wrapper = ctk.CTkFrame(dock, fg_color="transparent")
        progress_wrapper.pack(fill="x", padx=24, pady=(18, 10))
        progress_wrapper.grid_columnconfigure(0, weight=1)
        progress_wrapper.grid_columnconfigure(1, weight=1)

        self.global_progress_label = ctk.CTkLabel(
            progress_wrapper,
            text="",
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=11, weight="bold"),
            anchor="w",
        )
        self.global_progress_label.grid(row=0, column=0, sticky="w", padx=(0, 20))
        self.item_progress_label = ctk.CTkLabel(
            progress_wrapper,
            text="",
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=11, weight="bold"),
            anchor="w",
        )
        self.item_progress_label.grid(row=0, column=1, sticky="w")

        self.global_progress = ctk.CTkProgressBar(
            progress_wrapper,
            fg_color=SURFACE_VARIANT,
            progress_color=ACCENT,
            height=10,
        )
        self.global_progress.grid(row=1, column=0, sticky="ew", padx=(0, 20), pady=(4, 0))
        self.global_progress.set(0)

        self.item_progress = ctk.CTkProgressBar(
            progress_wrapper,
            fg_color=SURFACE_VARIANT,
            progress_color=ACCENT,
            height=10,
        )
        self.item_progress.grid(row=1, column=1, sticky="ew", pady=(4, 0))
        self.item_progress.set(0)

        lower = ctk.CTkFrame(dock, fg_color="transparent")
        lower.pack(fill="both", expand=True, padx=24, pady=(0, 12))
        lower.grid_columnconfigure(0, weight=1)
        lower.grid_columnconfigure(1, weight=0)
        lower.grid_rowconfigure(0, weight=1)

        self.log_text = ctk.CTkTextbox(
            lower,
            fg_color=SURFACE,
            text_color=TEXT_SECONDARY,
            corner_radius=8,
            font=ctk.CTkFont(size=12, family="Consolas"),
            wrap="word",
        )
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=(0, 18))
        self.log_text.configure(state="disabled")

        self.archive_button = ctk.CTkButton(
            lower,
            text="",
            command=self.start_archive,
            fg_color=ACCENT,
            hover_color="#629dff",
            text_color=ACCENT_TEXT,
            width=200,
            height=108,
            corner_radius=8,
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        self.archive_button.grid(row=0, column=1, sticky="ns")

        status_line = ctk.CTkFrame(dock, fg_color="transparent")
        status_line.pack(fill="x", padx=24, pady=(0, 10))

        self.status_label = ctk.CTkLabel(
            status_line,
            text="",
            text_color=ACCENT,
            font=ctk.CTkFont(size=11, weight="bold"),
            anchor="w",
        )
        self.status_label.pack(side="left")

        self.version_label = ctk.CTkLabel(
            status_line,
            text="v1.0.0",
            text_color="#414755",
            font=ctk.CTkFont(size=11),
            anchor="e",
        )
        self.version_label.pack(side="right")

    def _build_scan_summary_label(self, parent: ctk.CTkFrame, text: str) -> ctk.CTkLabel:
        return ctk.CTkLabel(
            parent,
            text=text,
            text_color=SCAN_INFO_TEXT,
            font=ctk.CTkFont(size=12, weight="bold"),
            anchor="w",
        )

    def _configure_table_columns(self, widget: tk.Misc) -> None:
        for index, spec in enumerate(TABLE_COLUMN_SPECS):
            widget.grid_columnconfigure(
                index,
                minsize=spec["minsize"],
                weight=spec["weight"],
                uniform=spec["uniform"],
            )

    def _initialize_path_selectors(self) -> None:
        clients = self._list_direct_child_dirs(self.base_root_path)
        if not clients:
            self.client_combobox.configure(values=[self.t("no_client")], state="disabled")
            self.client_combobox.set(self.t("no_client"))
            self.period_combobox.configure(values=[self.t("no_period")], state="disabled")
            self.period_combobox.set(self.t("no_period"))
            self._update_selected_scan_path(self.base_root_path)
            return

        client_names = [client.name for client in clients]
        self.client_combobox.configure(values=client_names, state="readonly")
        self.client_combobox.set(client_names[0])
        self.on_client_change(client_names[0])

    def _list_direct_child_dirs(self, path: Path) -> list[Path]:
        directories: list[Path] = []
        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    if entry.is_dir(follow_symlinks=False):
                        directories.append(Path(entry.path))
        except FileNotFoundError:
            return []
        directories.sort(key=lambda item: item.name.casefold())
        return directories

    def on_client_change(self, selected_client: str) -> None:
        client_path = self.base_root_path / selected_client
        year_paths = [path for path in self._list_direct_child_dirs(client_path) if self._is_year_folder(path)]
        values: list[str] = []
        self.period_path_map.clear()

        for year_path in year_paths:
            year_label = f"{year_path.name}\\{self.t('all_months')}"
            values.append(year_label)
            self.period_path_map[year_label] = year_path

            month_paths = [path for path in self._list_direct_child_dirs(year_path) if self._is_month_folder(path)]
            for month_path in month_paths:
                month_label = f"{year_path.name}\\{month_path.name}"
                values.append(month_label)
                self.period_path_map[month_label] = month_path

        if not values:
            self.period_combobox.configure(values=[self.t("no_period")], state="disabled")
            self.period_combobox.set(self.t("no_period"))
            self._update_selected_scan_path(client_path)
            return

        self.period_combobox.configure(values=values, state="readonly")
        self.period_combobox.set(values[0])
        self.on_period_change(values[0])

    def on_period_change(self, selected_period: str) -> None:
        target_path = self.period_path_map.get(selected_period)
        if target_path is None:
            selected_client = self.client_combobox.get()
            target_path = self.base_root_path / selected_client if selected_client else self.base_root_path
        self._update_selected_scan_path(target_path)

    def _update_selected_scan_path(self, target_path: Path) -> None:
        self.current_scan_path = target_path
        self.engine.root_path = target_path
        self._update_root_path_label()
        self.breadcrumb_label.configure(text=self._format_breadcrumb(target_path))
        has_target = target_path.exists()
        if not self.is_busy:
            self.scan_button.configure(state="normal" if has_target else "disabled")

    def change_root_path(self) -> None:
        selected_path = filedialog.askdirectory(
            parent=self,
            initialdir=str(self.base_root_path),
            mustexist=True,
        )
        if not selected_path:
            return

        new_root = Path(selected_path)
        self.base_root_path = new_root
        self.current_scan_path = new_root
        self.engine.root_path = new_root
        self.settings_db.set_root_path(new_root)

        self.period_path_map.clear()
        self.scan_results = []
        self.row_states.clear()
        self._initialize_path_selectors()
        self._reset_scan_summary()
        self._render_rows(self.scan_results)
        self._update_root_path_label()
        self.status_label.configure(text=self.t("system_ready"), text_color=ACCENT)
        self.push_log(self.t("root_changed", path=new_root))

    def _format_breadcrumb(self, target_path: Path) -> str:
        parts = [part for part in target_path.parts if part]
        if len(parts) >= 2:
            return " > ".join(parts[-min(len(parts), 4):])
        return str(target_path)

    def _is_year_folder(self, path: Path) -> bool:
        return bool(re.fullmatch(r"\d{4}", path.name.strip()))

    def _is_month_folder(self, path: Path) -> bool:
        return bool(re.fullmatch(r"\d{1,2}\s*-\s*.+", path.name.strip()))

    def _load_dashboard_metrics(self) -> None:
        stats = self.db.get_dashboard_stats()
        self._apply_dashboard_stats(stats)

    def _apply_dashboard_stats(self, stats: DashboardStats) -> None:
        self.db.reload()
        stats = self.db.get_dashboard_stats()
        self.total_saved_value.configure(text=format_bytes(stats.total_saved_bytes))
        self.saved_month_value.configure(text=format_bytes(stats.saved_this_month_bytes))
        self.total_folders_value.configure(text=f"{stats.total_folders_archived:,}")
        self.month_folders_value.configure(text=f"{stats.folders_archived_this_month:,}")

    def _start_queue_poller(self) -> None:
        self.after(self.POLL_INTERVAL_MS, self._drain_ui_queue)

    def _drain_ui_queue(self) -> None:
        try:
            while True:
                event = self.ui_queue.get_nowait()
                self._handle_event(event)
        except queue.Empty:
            pass
        self.after(self.POLL_INTERVAL_MS, self._drain_ui_queue)

    def _handle_event(self, event: dict) -> None:
        event_type = event.get("type")
        if event_type == "log":
            self.log(event["message"], event.get("level", "INFO"))
        elif event_type == "scan_progress":
            self.status_label.configure(text=event["message"], text_color=TEXT_SECONDARY)
        elif event_type == "scan_complete":
            self.scan_results = event["candidates"]
            self._render_rows(self.scan_results)
            self._update_scan_summary(self.scan_results)
            self.set_busy(False)
            count = len(self.scan_results)
            self.status_label.configure(
                text=self.t("status_scan_complete", count=count),
                text_color=ACCENT,
            )
        elif event_type == "archive_global":
            self.global_progress.set(event["percent"])
            self.global_progress_label.configure(text=event["label"])
        elif event_type == "archive_item":
            self.item_progress.set(event["percent"])
            self.item_progress_label.configure(text=event["label"])
        elif event_type == "archive_batch_complete":
            self._apply_archive_batch_result(event["result"])
        elif event_type == "archive_done":
            self.set_busy(False)
            self.global_progress.set(1)
            self.item_progress.set(1)
            self.status_label.configure(text=event["message"], text_color=ACCENT)
        elif event_type == "error":
            self.set_busy(False)
            self.status_label.configure(text=event["message"], text_color="#ffb4ab")
            messagebox.showerror(self.t("error_title"), event["message"])

    def log(self, message: str, level: str = "INFO") -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{timestamp}] [{level}] {message}\n"
        self.log_text.configure(state="normal")
        self.log_text.insert("end", line)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def push_log(self, message: str, level: str = "INFO") -> None:
        self.ui_queue.put({"type": "log", "message": message, "level": level})

    def set_busy(self, busy: bool) -> None:
        self.is_busy = busy
        state = "disabled" if busy else "normal"
        scan_state = state if self.current_scan_path.exists() else "disabled"
        self.scan_button.configure(state=scan_state)
        self.archive_button.configure(state=state)
        self.blacklist_button.configure(state=state)
        self.client_combobox.configure(state="disabled" if busy else "readonly")
        period_state = "disabled" if busy or not self.period_path_map else "readonly"
        self.period_combobox.configure(state=period_state)

    def start_scan(self) -> None:
        if self.is_busy:
            return
        self.set_busy(True)
        self.global_progress.set(0)
        self.item_progress.set(0)
        self.global_progress_label.configure(text=self.t("global_operation"))
        self.item_progress_label.configure(text=self.t("current_item"))
        self._reset_scan_summary()
        self.status_label.configure(text=self.t("status_scanning"), text_color=ACCENT)
        self.push_log(self.t("logs_scanning", path=self.engine.root_path))

        worker = threading.Thread(target=self._scan_worker, daemon=True)
        worker.start()

    def _scan_worker(self) -> None:
        try:
            candidates = self.engine.scan_candidates(
                log_callback=self.push_log,
                progress_callback=lambda message: self.ui_queue.put(
                    {"type": "scan_progress", "message": message}
                ),
            )
            self.ui_queue.put({"type": "scan_complete", "candidates": candidates})
        except Exception as exc:  # noqa: BLE001
            self.ui_queue.put({"type": "error", "message": str(exc)})

    def _render_rows(self, candidates: list[FolderCandidate]) -> None:
        for child in self.rows_container.winfo_children():
            child.destroy()
        self.row_states.clear()

        if not candidates:
            self.empty_state = ctk.CTkLabel(
                self.rows_container,
                text=self.t("empty_no_matches"),
                text_color=TEXT_SECONDARY,
                font=ctk.CTkFont(size=14),
            )
            self.empty_state.grid(row=0, column=0, padx=12, pady=32, sticky="n")
            return

        current_month_marker = ""
        row_index = 0
        for candidate in candidates:
            month_marker = self.lang_manager.month_name(candidate.modified_at.month)
            if month_marker != current_month_marker:
                separator = ctk.CTkFrame(
                    self.rows_container,
                    fg_color=SURFACE_VARIANT,
                    corner_radius=8,
                    height=34,
                )
                separator.grid(row=row_index, column=0, sticky="ew", padx=0, pady=(10, 4))
                ctk.CTkLabel(
                    separator,
                    text=month_marker,
                    text_color=TEXT_PRIMARY,
                    font=ctk.CTkFont(size=12, weight="bold"),
                    anchor="w",
                ).pack(fill="x", padx=16, pady=7)
                current_month_marker = month_marker
                row_index += 1

            frame = ctk.CTkFrame(self.rows_container, fg_color=SURFACE_LOW, corner_radius=8, height=54)
            frame.grid(row=row_index, column=0, sticky="ew", padx=0, pady=4)
            self._configure_table_columns(frame)

            variable = tk.BooleanVar(value=True)
            checkbox = ctk.CTkCheckBox(
                frame,
                text="",
                variable=variable,
                command=self._sync_select_all_checkbox,
                checkbox_width=18,
                checkbox_height=18,
                border_width=1,
                fg_color=ACCENT,
                hover_color="#629dff",
            )
            checkbox.grid(row=0, column=0, padx=(14, 10), pady=12, sticky="w")

            ctk.CTkLabel(
                frame,
                text=candidate.folder_name,
                text_color=TEXT_PRIMARY,
                font=ctk.CTkFont(size=13, weight="bold"),
                anchor="w",
            ).grid(row=0, column=1, sticky="w", padx=(0, 10))

            ctk.CTkLabel(
                frame,
                text=candidate.modified_at.strftime("%Y-%m-%d %H:%M"),
                text_color=TEXT_SECONDARY,
                font=ctk.CTkFont(size=12),
                anchor="w",
            ).grid(row=0, column=2, sticky="w", padx=(0, 10))

            age_days = max((datetime.now() - candidate.modified_at).days, 0)
            ctk.CTkLabel(
                frame,
                text=self.t("days_suffix", days=age_days),
                text_color=TEXT_SECONDARY,
                font=ctk.CTkFont(size=12, family="Consolas"),
                anchor="w",
            ).grid(row=0, column=3, sticky="w", padx=(0, 10))

            ctk.CTkLabel(
                frame,
                text=format_bytes(candidate.size_bytes),
                text_color=ACCENT,
                font=ctk.CTkFont(size=12, family="Consolas"),
                anchor="w",
            ).grid(row=0, column=4, sticky="w", padx=(0, 10))

            status = ctk.CTkLabel(
                frame,
                text=self.t("ready_status"),
                text_color=ACCENT,
                fg_color="#16396d",
                corner_radius=6,
                font=ctk.CTkFont(size=11, weight="bold"),
                width=78,
                height=28,
            )
            status.grid(row=0, column=5, sticky="w", padx=(0, 14))

            self.row_states.append(RowState(candidate, variable, frame, status))
            row_index += 1

        self.select_all_var.set(True)

    def _update_scan_summary(self, candidates: list[FolderCandidate]) -> None:
        total_size = sum(candidate.size_bytes for candidate in candidates)
        estimated_savings = int(total_size * 0.4)
        self.scan_found_label.configure(text=self.t("scan_summary_found", count=len(candidates)))
        self.scan_size_label.configure(text=self.t("scan_summary_size", size=format_bytes(total_size)))
        self.scan_savings_label.configure(
            text=self.t("scan_summary_savings", size=format_bytes(estimated_savings))
        )

    def _reset_scan_summary(self) -> None:
        self.scan_found_label.configure(text=self.t("scan_summary_found", count=0))
        self.scan_size_label.configure(text=self.t("scan_summary_size", size="0 B"))
        self.scan_savings_label.configure(text=self.t("scan_summary_savings", size="0 B"))

    def toggle_all_rows(self) -> None:
        value = self.select_all_var.get()
        for row in self.row_states:
            row.variable.set(value)

    def _sync_select_all_checkbox(self) -> None:
        self.select_all_var.set(all(row.variable.get() for row in self.row_states) if self.row_states else False)

    def get_selected_candidates(self) -> list[FolderCandidate]:
        return [row.candidate for row in self.row_states if row.variable.get()]

    def start_archive(self) -> None:
        if self.is_busy:
            return

        selected = self.get_selected_candidates()
        if not selected:
            messagebox.showinfo(self.t("error_title"), self.t("select_at_least_one"))
            return

        confirm = messagebox.askyesno(
            self.t("confirm_title"),
            self.t("confirm_message", count=len(selected)),
        )
        if not confirm:
            return

        self.set_busy(True)
        self.global_progress.set(0)
        self.item_progress.set(0)
        self.status_label.configure(text=self.t("status_batch_running"), text_color=ACCENT)
        worker = threading.Thread(target=self._archive_worker, args=(selected,), daemon=True)
        worker.start()

    def _archive_worker(self, selected: list[FolderCandidate]) -> None:
        try:
            self.push_log(self.t("batch_archive_start", count=len(selected)))
            result = self.engine.archive_batch(
                selected,
                log_callback=self.push_log,
                progress_callback=self._publish_archive_progress,
            )
            self.ui_queue.put({"type": "archive_batch_complete", "result": result})
            success_count = len(result.deleted_source_paths)
            failure_count = len(result.failed_archives)
            message = self.t(
                "status_batch_done",
                success_count=success_count,
                failure_count=failure_count,
            )
            self.ui_queue.put({"type": "archive_done", "message": message})
        except Exception as exc:  # noqa: BLE001
            self.ui_queue.put({"type": "error", "message": str(exc)})

    def _publish_archive_progress(self, payload: dict) -> None:
        payload_type = payload.get("type")
        if payload_type == "item":
            label = payload.get("label", "Current Item")
            percent = float(payload.get("percent", 0.0))
            self.ui_queue.put(
                {
                    "type": "archive_item",
                    "percent": percent,
                    "label": self.t("current_progress", label=label, percent=percent * 100),
                }
            )
            return

        if payload_type == "batch":
            label = payload.get("label", "Batch Processing")
            percent = float(payload.get("percent", 0.0))
            self.ui_queue.put(
                {
                    "type": "archive_global",
                    "percent": percent,
                    "label": label,
                }
            )

    def _apply_archive_batch_result(self, result: ArchiveBatchResult) -> None:
        self.db.reload()
        self._load_dashboard_metrics()
        deleted_set = set(result.deleted_source_paths)
        self.scan_results = [
            candidate for candidate in self.scan_results if str(candidate.path) not in deleted_set
        ]
        self._render_rows(self.scan_results)
        self._update_scan_summary(self.scan_results)
        for failure in result.failed_archives:
            self.push_log(
                self.t(
                    "batch_issue",
                    name=Path(failure["source_path"]).name,
                    error=failure["error"],
                ),
                "ERROR",
            )

    def open_blacklist_dialog(self) -> None:
        dialog = ctk.CTkToplevel(self)
        dialog.title(self.t("blacklist_title"))
        dialog.geometry("520x420")
        dialog.configure(fg_color=SURFACE_LOW)
        dialog.transient(self)
        dialog.grab_set()

        ctk.CTkLabel(
            dialog,
            text=self.t("blacklist_title"),
            text_color=TEXT_PRIMARY,
            font=ctk.CTkFont(size=20, weight="bold"),
        ).pack(anchor="w", padx=20, pady=(20, 8))

        ctk.CTkLabel(
            dialog,
            text=self.t("blacklist_help"),
            text_color=TEXT_SECONDARY,
            font=ctk.CTkFont(size=12),
            justify="left",
        ).pack(anchor="w", padx=20)

        textbox = ctk.CTkTextbox(
            dialog,
            fg_color=SURFACE,
            text_color=TEXT_PRIMARY,
            corner_radius=8,
            font=ctk.CTkFont(size=13, family="Consolas"),
        )
        textbox.pack(fill="both", expand=True, padx=20, pady=16)
        textbox.insert("1.0", "\n".join(self.db.get_blacklist()))

        button_row = ctk.CTkFrame(dialog, fg_color="transparent")
        button_row.pack(fill="x", padx=20, pady=(0, 20))

        def save_blacklist() -> None:
            items = textbox.get("1.0", "end").splitlines()
            self.db.replace_blacklist(items)
            self.engine.set_blacklist(self.db.get_blacklist())
            self.push_log(self.t("logs_blacklist_updated"))
            dialog.destroy()

        ctk.CTkButton(
            button_row,
            text=self.t("save"),
            command=save_blacklist,
            fg_color=ACCENT,
            hover_color="#629dff",
            text_color=ACCENT_TEXT,
        ).pack(side="right")

        ctk.CTkButton(
            button_row,
            text=self.t("cancel"),
            command=dialog.destroy,
            fg_color=SURFACE_CARD,
            hover_color=SURFACE_VARIANT,
            text_color=TEXT_PRIMARY,
        ).pack(side="right", padx=(0, 10))
