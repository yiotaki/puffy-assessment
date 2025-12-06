#!/usr/bin/env python3
"""
PUFFY – FULL PIPELINE

Stages:
1) Merge + QA of multiple CSV exports
2) ETL (URL parsing, UTM extraction, UA parsing, validity flags, dedup, sessions, revenue extraction)
3) Journeys & funnel (category-based + Sankey, with revenue per journey)
4) Multi-channel attribution (UTM, category, referrer, UTM|referrer) with revenue
5) Advanced & Revenue analysis (single module):
   - Acquisition funnel
   - UTM-based funnel (top channels)
   - UTM next-step funnel (7-day window)
   - UTM first/last-touch attribution (with revenue)
   - UTM attribution by device/device_family (with revenue)
   - Device mix at conversion (with revenue)
   - Revenue overview, by day, by UTM, by conversion path
6) Monitoring:
   - Daily metrics & anomaly flags
   - Device mix shifts

All outputs go under BASE_OUTPUT_FOLDER.
"""

import os
import glob
import math
import json
from urllib.parse import urlparse, parse_qs

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# -------------------------------------------------------------------
# GLOBAL CONFIG – edit paths here
# -------------------------------------------------------------------
CONFIG = {
    # --------- RAW MERGE INPUT / OUTPUT ----------
    # Folder where your raw daily CSVs live
    "INPUT_FOLDER": r"C:\puffy\puffy_data",              # raw daily csvs

    # Path where the merged CSV will be written
    "OUTPUT_FILE": r"C:\puffy\puffy_files\merged.csv",   # merged csv

    # Alias used by ETL block (kept for compatibility)
    "INPUT_MERGED_FILE": r"C:\puffy\puffy_files\merged.csv",

    "CLEANED_SUBFOLDER": "cleaned_exports",
    "QA_FILENAME": "qa_results.xlsx",

    # --------- ETL / JOURNEY / ATTRIBUTION / ANALYSIS OUTPUT ROOT ----------
    "BASE_OUTPUT_FOLDER": r"C:\puffy\puffy_outputs",
    "ETL_SUBFOLDER": "etl",
    "JOURNEY_SUBFOLDER": "journey",
    "ATTRIBUTION_SUBFOLDER": "attribution",
    "ANALYSIS_SUBFOLDER": "analysis",
    "RECON_SUBFOLDER": "reconciliation",

    # Canonical columns
    "CLIENT_ID_COLUMN": "client_id",
    "EVENT_NAME_COLUMN": "event_name",
    "EVENT_DATA_COLUMN": "event_data",
    "PAGE_URL_COLUMN": "page_url",
    "REFERRER_COLUMN": "referrer",
    "USER_AGENT_COLUMN": "user_agent",
    "TIMESTAMP_COLUMN": "timestamp",

    # Expected columns (optional) for QA merge. If empty, derived from data.
    "EXPECTED_COLUMNS": [],

    # URL / category / UTM
    "DOMAIN_PREFIX": "https://puffy.com/",
    "URL_CATEGORIES": [
        "apps",
        "cart",
        "shop",
        "_next",
        "blogs",
        "pages",
        "tools",
        "account",
        "products",
        "checkouts",
        "collections",
    ],
    "UTM_COLS": ["utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"],

    # Required cols for ETL valid record
    "REQUIRED_COLUMNS": ["client_id", "event_name", "timestamp"],

    # QA thresholds
    "EVENT_DATA_NULL_PCT_THRESHOLD": 20.0,

    # Journey / funnel
    "FUNNEL_STAGES": [
        "home",
        "blogs",
        "products",
        "cart",
        "checkouts",
        "checkout_completed",
    ],
    "TOP_N_JOURNEYS": 15,

    # Attribution
    "CONVERSION_EVENT": "checkout_completed",
    "ATTRIBUTION_LOOKBACK_DAYS": 7,

    # Advanced analysis (UTM analysis)
    "TOP_UTM_CHANNELS_FOR_FUNNEL": 5,
    "TOP_CHANNELS_FOR_ATTR": 10,
}


# ===================================================================
# COMMON HELPERS
# ===================================================================

def ensure_folder(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def is_text_dtype(series: pd.Series) -> bool:
    return str(series.dtype) in ("object", "string", "category")


def detect_timestamp_columns(df: pd.DataFrame) -> dict:
    """
    Detect timestamp-like columns based on name hints and parse success.

    Returns:
        dict: {column_name: parsed_datetime_series}
    """
    ts_cols: dict[str, pd.Series] = {}

    for col in df.columns:
        s = df[col]
        name_hint = any(k in col.lower() for k in ["time", "date", "timestamp"])

        if "datetime" in str(s.dtype).lower():
            parsed = pd.to_datetime(s, errors="coerce")
        elif name_hint:
            parsed = pd.to_datetime(s, errors="coerce")
        else:
            continue

        non_null = s.notna().sum()
        if non_null == 0:
            continue

        parsed_non_null = parsed.notna().sum()
        if parsed_non_null / non_null >= 0.8:
            ts_cols[col] = parsed

    return ts_cols


def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return np.nan


# --------- Revenue extraction from event_data JSON ---------

def extract_revenue_from_event_data(event_data_value, event_name: str, cfg: dict) -> dict:
    """
    Extract order-level metrics from event_data JSON for checkout_completed events.

    Returns dict with keys:
        order_revenue: float
        order_quantity: float
        order_currency: str
        order_products: str  (comma-separated product names / SKUs)
    """
    conv_event = cfg["CONVERSION_EVENT"]

    if event_name != conv_event:
        return {
            "order_revenue": np.nan,
            "order_quantity": np.nan,
            "order_currency": None,
            "order_products": None,
        }

    if event_data_value is None or (isinstance(event_data_value, float) and math.isnan(event_data_value)):
        return {
            "order_revenue": np.nan,
            "order_quantity": np.nan,
            "order_currency": None,
            "order_products": None,
        }

    # Decode JSON if string
    obj = None
    if isinstance(event_data_value, dict):
        obj = event_data_value
    elif isinstance(event_data_value, str):
        event_data_value = event_data_value.strip()
        if not event_data_value:
            return {
                "order_revenue": np.nan,
                "order_quantity": np.nan,
                "order_currency": None,
                "order_products": None,
            }
        try:
            obj = json.loads(event_data_value)
        except Exception:
            return {
                "order_revenue": np.nan,
                "order_quantity": np.nan,
                "order_currency": None,
                "order_products": None,
            }
    else:
        return {
            "order_revenue": np.nan,
            "order_quantity": np.nan,
            "order_currency": None,
            "order_products": None,
        }

    if not isinstance(obj, dict):
        return {
            "order_revenue": np.nan,
            "order_quantity": np.nan,
            "order_currency": None,
            "order_products": None,
        }

    revenue = np.nan
    quantity = np.nan
    currency = None
    product_names: list[str] = []

    # 1) Direct revenue keys in root
    revenue_keys = [
        "order_revenue",
        "revenue",
        "value",
        "total",
        "amount",
        "order_value",
        "orderTotal",
        "totalPrice",
        "grand_total",
        "price",
    ]
    for key in revenue_keys:
        if key in obj:
            rev = _safe_float(obj[key])
            if not math.isnan(rev):
                revenue = rev
                break

    # 2) Items / products list
    items = (
        obj.get("items")
        or obj.get("products")
        or obj.get("line_items")
        or obj.get("cart")
        or obj.get("order_items")
    )

    if isinstance(items, list):
        total_items_qty = 0.0
        items_revenue = 0.0
        have_revenue_from_items = False

        for it in items:
            if not isinstance(it, dict):
                continue
            name = (
                it.get("name")
                or it.get("product_name")
                or it.get("title")
                or it.get("sku")
                or it.get("id")
            )
            if name:
                product_names.append(str(name))

            q = it.get("quantity") or it.get("qty") or 1
            qf = _safe_float(q)
            if not math.isnan(qf) and qf > 0:
                total_items_qty += qf
            else:
                qf = 1.0

            p = (
                it.get("price")
                or it.get("value")
                or it.get("amount")
                or it.get("unit_price")
            )
            pf = _safe_float(p)
            if not math.isnan(pf):
                items_revenue += pf * qf
                have_revenue_from_items = True

        if not math.isnan(total_items_qty) and total_items_qty > 0:
            quantity = total_items_qty

        if math.isnan(revenue) and have_revenue_from_items:
            revenue = items_revenue

    # 3) Fallback quantity from root
    if math.isnan(quantity):
        quantity_keys = ["order_quantity", "quantity", "qty", "total_quantity", "items_count"]
        for key in quantity_keys:
            if key in obj:
                q = _safe_float(obj[key])
                if not math.isnan(q):
                    quantity = q
                    break

    # 4) Currency
    for key in ["currency", "currency_code", "curr"]:
        if key in obj:
            currency = str(obj[key])
            break

    products_str = ", ".join(sorted(set(product_names))) if product_names else None

    return {
        "order_revenue": revenue,
        "order_quantity": quantity,
        "order_currency": currency,
        "order_products": products_str,
    }


def extract_basic_product_metrics(event_data_value, event_name: str, cfg: dict) -> dict:
    """
    Additional extraction specifically for merged_puffy_etl:
    - revenue
    - price
    - quantity
    - product

    Only populated for event_name == checkout_completed.
    Otherwise returns NaNs / None, so non-conversion rows stay empty.
    """
    conv_event = cfg["CONVERSION_EVENT"]

    if event_name != conv_event:
        return {
            "revenue": np.nan,
            "price": np.nan,
            "quantity": np.nan,
            "product": None,
        }

    if event_data_value is None or (isinstance(event_data_value, float) and math.isnan(event_data_value)):
        return {
            "revenue": np.nan,
            "price": np.nan,
            "quantity": np.nan,
            "product": None,
        }

    # Decode JSON
    obj = None
    if isinstance(event_data_value, dict):
        obj = event_data_value
    elif isinstance(event_data_value, str):
        s = event_data_value.strip()
        if not s:
            return {
                "revenue": np.nan,
                "price": np.nan,
                "quantity": np.nan,
                "product": None,
            }
        try:
            obj = json.loads(s)
        except Exception:
            return {
                "revenue": np.nan,
                "price": np.nan,
                "quantity": np.nan,
                "product": None,
            }
    else:
        return {
            "revenue": np.nan,
            "price": np.nan,
            "quantity": np.nan,
            "product": None,
        }

    if not isinstance(obj, dict):
        return {
            "revenue": np.nan,
            "price": np.nan,
            "quantity": np.nan,
            "product": None,
        }

    revenue = np.nan
    quantity = np.nan
    unit_price = np.nan
    product_name = None

    # Candidate revenue keys
    for key in ["revenue", "value", "total", "amount", "order_value", "order_revenue", "totalPrice", "price"]:
        if key in obj:
            rev = _safe_float(obj[key])
            if not math.isnan(rev):
                revenue = rev
                break

    # Items
    items = (
        obj.get("items")
        or obj.get("products")
        or obj.get("line_items")
        or obj.get("cart")
        or obj.get("order_items")
    )

    if isinstance(items, list) and items:
        # Take first item for price/product, sum quantity
        first = items[0] if isinstance(items[0], dict) else {}
        name = (
            first.get("name")
            or first.get("product_name")
            or first.get("title")
            or first.get("sku")
            or first.get("id")
        )
        if name:
            product_name = str(name)

        p = (
            first.get("price")
            or first.get("value")
            or first.get("amount")
            or first.get("unit_price")
        )
        unit_price = _safe_float(p)

        total_qty = 0.0
        for it in items:
            if not isinstance(it, dict):
                continue
            q = it.get("quantity") or it.get("qty") or 1
            qf = _safe_float(q)
            if not math.isnan(qf) and qf > 0:
                total_qty += qf
        if total_qty > 0:
            quantity = total_qty

        # If revenue is still NaN, derive as sum(price * qty)
        if math.isnan(revenue):
            tmp_rev = 0.0
            have_rev = False
            for it in items:
                if not isinstance(it, dict):
                    continue
                q = it.get("quantity") or it.get("qty") or 1
                qf = _safe_float(q)
                if math.isnan(qf) or qf <= 0:
                    qf = 1.0
                p = (
                    it.get("price")
                    or it.get("value")
                    or it.get("amount")
                    or it.get("unit_price")
                )
                pf = _safe_float(p)
                if not math.isnan(pf):
                    tmp_rev += pf * qf
                    have_rev = True
            if have_rev:
                revenue = tmp_rev

    # Fallback quantity from root if still NaN
    if math.isnan(quantity):
        for key in ["order_quantity", "quantity", "qty", "total_quantity", "items_count"]:
            if key in obj:
                q = _safe_float(obj[key])
                if not math.isnan(q):
                    quantity = q
                    break

    return {
        "revenue": revenue,
        "price": unit_price,
        "quantity": quantity,
        "product": product_name,
    }


def build_data_quality_summary(merged_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    High-level data quality checks (daily):
    - Event volume by day
    - Distinct client_ids by day
    - Conversions by day
    - Revenue by day:
        * revenue is derived from event_data JSON for event_name=checkout_completed,
          using the same logic as ETL (extract_revenue_from_event_data),
          so every source_file's checkout_completed events are included.
    """
    ts_col = cfg["TIMESTAMP_COLUMN"]
    client_col = cfg["CLIENT_ID_COLUMN"]
    event_col = cfg["EVENT_NAME_COLUMN"]
    data_col = cfg["EVENT_DATA_COLUMN"]
    conv_event = cfg["CONVERSION_EVENT"]

    df = merged_df.copy()

    # --- Ensure we have order_revenue at this stage as well (from event_data) ---
    # This does NOT remove/affect any later ETL logic; it's local to DQ.
    if "order_revenue" not in df.columns and data_col in df.columns and event_col in df.columns:
        try:
            metrics_df = df.apply(
                lambda r: pd.Series(
                    extract_revenue_from_event_data(
                        r.get(data_col), r.get(event_col), cfg
                    )
                ),
                axis=1,
            )
            if "order_revenue" in metrics_df.columns:
                df["order_revenue"] = metrics_df["order_revenue"]
        except Exception as e:
            print(f"⚠️ DQ-level revenue extraction failed: {e}")
            # If it fails, we simply won't have revenue; no existing behavior is removed.

    if ts_col not in df.columns:
        raise KeyError(f"Timestamp column '{ts_col}' not found for DQ summary.")

    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df["event_date"] = df[ts_col].dt.date

    agg = (
        df.groupby("event_date")
        .agg(
            total_events=(event_col, "size"),
            distinct_clients=(client_col, "nunique"),
            conversions=(event_col, lambda s: (s == conv_event).sum()),
        )
        .reset_index()
        .sort_values("event_date")
    )

    # Prefer order_revenue we just built; fallback to other numeric revenue-like columns
    revenue_col = None
    candidates = ["order_revenue", "value", "revenue", "amount", "total", "order_value"]
    for cand in candidates:
        if cand in df.columns and np.issubdtype(df[cand].dtype, np.number):
            revenue_col = cand
            break

    if revenue_col:
        rev = (
            df[df[event_col] == conv_event]
            .groupby("event_date")[revenue_col]
            .sum()
            .reindex(agg["event_date"])
            .reset_index(drop=True)
        )
        agg["revenue"] = rev.fillna(0.0)
    else:
        agg["revenue"] = np.nan

    return agg


# ===================================================================
# PART 1 – MERGE + QA
# ===================================================================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize inconsistent column names across files.
    - clientId variants -> client_id
    - eventData variants -> event_data
    """
    client_id = CONFIG["CLIENT_ID_COLUMN"]
    event_data = CONFIG["EVENT_DATA_COLUMN"]

    col_map = {
        "clientId": client_id,
        "ClientId": client_id,
        "ClientID": client_id,
        "clientID": client_id,
        "eventData": event_data,
        "EventData": event_data,
        "event_Data": event_data,
    }
    return df.rename(columns=col_map)


def merge_csv_files(folder_path: str,
                    output_file: str = "merged.csv",
                    export_cleaned: bool = False) -> None:
    """
    Merge all CSV files from folder_path, run QA, and write:
      - merged CSV to output_file
      - QA workbook to QA_FILENAME in output_file's directory
    """

    out_dir = os.path.dirname(output_file)
    if out_dir:
        ensure_folder(out_dir)

    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        print(f"❌ No CSV files found in: {folder_path}")
        return

    print(f"🔍 Found {len(csv_files)} CSV files.\n")

    df_list: list[pd.DataFrame] = []
    file_columns_original: dict[str, list[str]] = {}
    file_columns_normalized: dict[str, list[str]] = {}
    file_record_counts: dict[str, int] = {}
    file_skipped_records: dict[str, int] = {}
    orig_name_map: dict[tuple[str, str], str] = {}

    cleaned_folder = os.path.join(folder_path, CONFIG["CLEANED_SUBFOLDER"])
    if export_cleaned:
        os.makedirs(cleaned_folder, exist_ok=True)

    # ---------------- read + normalize each file ----------------
    for file in csv_files:
        fname = os.path.basename(file)
        print(f"📄 Reading: {fname}")

        with open(file, "r", encoding="utf-8", errors="ignore") as f:
            raw_lines = sum(1 for _ in f) - 1

        df_raw = pd.read_csv(file, on_bad_lines="skip")
        parsed_rows = len(df_raw)
        skipped_rows = max(raw_lines - parsed_rows, 0)
        file_record_counts[fname] = parsed_rows
        file_skipped_records[fname] = skipped_rows

        orig_cols = list(df_raw.columns)
        file_columns_original[fname] = orig_cols

        df_norm = normalize_columns(df_raw)
        norm_cols = list(df_norm.columns)
        file_columns_normalized[fname] = norm_cols

        for orig, norm in zip(orig_cols, norm_cols):
            orig_name_map[(fname, norm)] = orig

        df_norm["source_file"] = fname
        df_list.append(df_norm)

        if export_cleaned:
            cleaned_path = os.path.join(cleaned_folder, fname)
            df_norm.to_csv(cleaned_path, index=False)
            print(f"   ↳ Exported cleaned: {cleaned_path}")

    merged_df = pd.concat(df_list, ignore_index=True)

    # All columns seen after normalization, unless EXPECTED_COLUMNS is provided
    if CONFIG["EXPECTED_COLUMNS"]:
        expected_cols = CONFIG["EXPECTED_COLUMNS"]
    else:
        expected_cols = sorted(
            set().union(*[set(cols) for cols in file_columns_normalized.values()])
        )

    # ---------------- duplicate record counts per source file ----------------
    duplicate_records_per_file: dict[str, int] = {}
    feature_cols = [c for c in merged_df.columns if c != "source_file"]

    for fname in merged_df["source_file"].unique():
        df_file = merged_df[merged_df["source_file"] == fname]
        if feature_cols:
            dup_mask = df_file.duplicated(subset=feature_cols, keep=False)
            dup_count = int(dup_mask.sum())
        else:
            dup_count = 0
        duplicate_records_per_file[fname] = dup_count

    # ---------------- input file structure / presence ----------------
    input_rows: list[dict] = []

    for fname in file_columns_original:
        row: dict[str, object] = {"file": fname}
        norm_cols = file_columns_normalized[fname]

        for col in expected_cols:
            if col in norm_cols:
                row[col] = orig_name_map.get((fname, col), col)
            else:
                row[col] = "—"

        row["records_read"] = file_record_counts[fname]
        row["skipped_records"] = file_skipped_records[fname]
        row["duplicate_records"] = duplicate_records_per_file.get(fname, 0)
        input_rows.append(row)

    presence_df = pd.DataFrame(input_rows)

    # ---------------- overall summary ----------------
    summary_rows = [
        {"metric": "files_merged", "value": len(csv_files)},
        {"metric": "total_rows", "value": len(merged_df)},
        {"metric": "total_columns", "value": merged_df.shape[1]},
    ]

    client_id_col = CONFIG["CLIENT_ID_COLUMN"]
    if client_id_col in merged_df.columns:
        summary_rows.append(
            {
                "metric": f"distinct_{client_id_col}",
                "value": merged_df[client_id_col].nunique(dropna=True),
            }
        )

    merged_summary_df = pd.DataFrame(summary_rows)

    # ---------------- timestamp detection ----------------
    timestamp_info = detect_timestamp_columns(merged_df)

    # ---------------- column stats (overall) ----------------
    col_stats: list[dict] = []
    total_rows = len(merged_df)
    page_url_col = CONFIG["PAGE_URL_COLUMN"]
    referrer_col = CONFIG["REFERRER_COLUMN"]

    for col in merged_df.columns:
        s = merged_df[col]
        non_null = int(s.notna().sum())
        nulls = int(s.isna().sum())
        distinct_vals = int(s.nunique(dropna=True))

        stat: dict[str, object] = {
            "column": col,
            "dtype": str(s.dtype),
            "non_null_count": non_null,
            "null_count": nulls,
            "null_pct": round(nulls / total_rows * 100, 2) if total_rows else 0.0,
            "distinct_values": distinct_vals,
            "distinct_pct": round(distinct_vals / total_rows * 100, 2)
            if total_rows
            else 0.0,
        }

        if is_text_dtype(s):
            lengths = s.dropna().astype(str).str.len()
            if len(lengths) > 0:
                stat["text_len_min"] = int(lengths.min())
                stat["text_len_max"] = int(lengths.max())
                stat["text_len_avg"] = float(round(lengths.mean(), 2))
            else:
                stat["text_len_min"] = None
                stat["text_len_max"] = None
                stat["text_len_avg"] = None

        if col in (page_url_col, referrer_col):
            non_null_url = s.dropna().astype(str)
            https_valid = int(non_null_url.str.startswith("https://").sum())
            https_invalid = len(non_null_url) - https_valid
            stat["https_valid"] = https_valid
            stat["https_invalid"] = https_invalid

        if col in timestamp_info:
            parsed = timestamp_info[col]
            non_null_orig = int(s.notna().sum())
            parsed_non_null = int(parsed.notna().sum())
            invalid_ts = non_null_orig - parsed_non_null
            stat["timestamp_parsed_non_null"] = parsed_non_null
            stat["timestamp_invalid"] = invalid_ts

        col_stats.append(stat)

    merged_col_stats_df = pd.DataFrame(col_stats)

    # ---------------- per-file column stats ----------------
    per_file_col_rows: list[dict] = []

    for fname in merged_df["source_file"].unique():
        df_file = merged_df[merged_df["source_file"] == fname]
        file_rows = len(df_file)

        for col in merged_df.columns:
            s = df_file[col]
            non_null = int(s.notna().sum())
            nulls = int(s.isna().sum())
            distinct_vals = int(s.nunique(dropna=True))

            row: dict[str, object] = {
                "file": fname,
                "column": col,
                "original_column": orig_name_map.get((fname, col), col),
                "non_null_count": non_null,
                "null_count": nulls,
                "null_pct": round(nulls / file_rows * 100, 2)
                if file_rows
                else 0.0,
                "distinct_values": distinct_vals,
                "distinct_pct": round(distinct_vals / file_rows * 100, 2)
                if file_rows
                else 0.0,
            }

            if is_text_dtype(s):
                lengths = s.dropna().astype(str).str.len()
                if len(lengths) > 0:
                    row["text_len_min"] = int(lengths.min())
                    row["text_len_max"] = int(lengths.max())
                    row["text_len_avg"] = float(round(lengths.mean(), 2))
                else:
                    row["text_len_min"] = None
                    row["text_len_max"] = None
                    row["text_len_avg"] = None

            if col in (page_url_col, referrer_col):
                non_null_url = s.dropna().astype(str)
                https_valid = int(non_null_url.str.startswith("https://").sum())
                https_invalid = len(non_null_url) - https_valid
                row["https_valid"] = https_valid
                row["https_invalid"] = https_invalid

            if col in timestamp_info:
                parsed_all = timestamp_info[col]
                parsed_file = parsed_all[df_file.index]
                non_null_orig = int(s.notna().sum())
                parsed_non_null = int(parsed_file.notna().sum())
                invalid_ts = non_null_orig - parsed_non_null
                row["timestamp_parsed_non_null"] = parsed_non_null
                row["timestamp_invalid"] = invalid_ts

            per_file_col_rows.append(row)

    per_file_col_stats_df = pd.DataFrame(per_file_col_rows)

    # ---------------- event stats ----------------
    event_name_col = CONFIG["EVENT_NAME_COLUMN"]
    event_data_col = CONFIG["EVENT_DATA_COLUMN"]

    event_stats_df = None
    event_file_totals_df = None
    page_url_category_stats_df = None

    if event_name_col in merged_df.columns and event_data_col in merged_df.columns:
        event_stats_df = (
            merged_df.groupby(["source_file", event_name_col], dropna=False)
            .agg(
                row_count=(event_name_col, "size"),
                event_data_nulls=(event_data_col, lambda s: s.isna().sum()),
                event_data_distinct=(event_data_col, lambda s: s.nunique(dropna=True)),
            )
            .reset_index()
        )

        event_stats_df["event_data_null_pct"] = (
            event_stats_df["event_data_nulls"] / event_stats_df["row_count"] * 100
        ).round(2)

        event_stats_df["event_data_distinct_pct"] = (
            event_stats_df["event_data_distinct"] / event_stats_df["row_count"] * 100
        ).round(2)

        event_stats_df["event_name_missing_pct"] = (
            event_stats_df[event_name_col].isna().astype(float) * 100.0
        )

        threshold = CONFIG["EVENT_DATA_NULL_PCT_THRESHOLD"]
        event_stats_df["event_data_null_flag"] = (
            event_stats_df["event_data_null_pct"] > threshold
        )

        grouped_file = merged_df.groupby("source_file", dropna=False)
        event_file_totals_df = (
            grouped_file.agg(
                total_events=(event_name_col, "size"),
                event_data_nulls=(event_data_col, lambda s: s.isna().sum()),
            )
            .reset_index()
        )
        event_file_totals_df["event_data_null_pct"] = (
            event_file_totals_df["event_data_nulls"]
            / event_file_totals_df["total_events"]
            * 100
        ).round(2)

    # Optional: simple URL stats
    page_url_col = CONFIG["PAGE_URL_COLUMN"]
    if page_url_col in merged_df.columns:
        page_url_category_stats_df = (
            merged_df.groupby("source_file")[page_url_col]
            .apply(lambda s: s.notna().sum())
            .reset_index(name="non_null_page_url")
        )

    # ---------------- data quality daily summary ----------------
    try:
        dq_daily = build_data_quality_summary(merged_df, CONFIG)
    except Exception as dq_err:
        print(f"⚠️ Could not build data quality daily summary: {dq_err}")
        dq_daily = pd.DataFrame()

    # ---------------- write QA workbook ----------------
    qa_results_xlsx = os.path.join(out_dir, CONFIG["QA_FILENAME"])
    with pd.ExcelWriter(qa_results_xlsx, engine="openpyxl") as writer:
        merged_summary_df.to_excel(writer, sheet_name="summary", index=False)
        presence_df.to_excel(writer, sheet_name="input structure", index=False)
        merged_col_stats_df.to_excel(writer, sheet_name="overall column stats", index=False)
        per_file_col_stats_df.to_excel(writer, sheet_name="per file column stats", index=False)
        if event_stats_df is not None:
            event_stats_df.to_excel(writer, sheet_name="event stats", index=False)
        if event_file_totals_df is not None:
            event_file_totals_df.to_excel(writer, sheet_name="event totals", index=False)
        if page_url_category_stats_df is not None:
            page_url_category_stats_df.to_excel(
                writer, sheet_name="url category stats", index=False
            )
        if not dq_daily.empty:
            dq_daily.to_excel(writer, sheet_name="daily_dq_summary", index=False)

    print(f"\n📘 QA results written to Excel: {qa_results_xlsx}")

    if os.path.exists(output_file):
        os.remove(output_file)
    merged_df.to_csv(output_file, index=False)
    print(f"💾 Merged dataset saved to: {output_file}")


# ===================================================================
# PART 2 – ETL + JOURNEY + ATTRIBUTION
# ===================================================================

def extract_category_from_url(url: str) -> str | None:
    if not isinstance(url, str):
        return None
    try:
        parsed = urlparse(url)
        path = parsed.path.strip("/")
        if not path:
            return "home"
        return path.split("/", 1)[0]
    except Exception:
        return None


def extract_utm_series(url: str, utm_keys: list[str]) -> pd.Series:
    data = {k: None for k in utm_keys}
    if not isinstance(url, str):
        return pd.Series(data)
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        for k in utm_keys:
            if k in qs and qs[k]:
                data[k] = qs[k][0]
    except Exception:
        pass
    return pd.Series(data)


def parse_user_agent(ua: str) -> dict:
    if not isinstance(ua, str):
        ua = ""
    low = ua.lower()

    # device_type
    if "ipad" in low or "tablet" in low:
        device_type = "tablet"
    elif any(x in low for x in ["iphone", "ipod"]):
        device_type = "mobile"
    elif "android" in low:
        device_type = "mobile"
    elif any(x in low for x in ["windows", "macintosh", "linux"]):
        device_type = "desktop"
    else:
        device_type = "unknown"

    # device_family
    if "iphone" in low:
        device_family = "iphone"
    elif "android" in low:
        device_family = "android"
    elif "ipad" in low or "tablet" in low:
        device_family = "tablet"
    elif device_type == "desktop":
        device_family = "desktop"
    elif device_type == "mobile":
        device_family = "mobile"
    else:
        device_family = "unknown"

    # browser
    if "edg" in low:
        browser = "edge"
    elif "opr" in low or "opera" in low:
        browser = "opera"
    elif "chrome" in low and "chromium" not in low:
        browser = "chrome"
    elif "safari" in low and "chrome" not in low:
        browser = "safari"
    elif "firefox" in low:
        browser = "firefox"
    else:
        browser = "unknown"

    # OS
    if "windows" in low:
        os_name = "windows"
    elif "mac os x" in low or "macintosh" in low:
        os_name = "macos"
    elif "android" in low:
        os_name = "android"
    elif any(x in low for x in ["iphone", "ipad", "ipod"]):
        os_name = "ios"
    else:
        os_name = "unknown"

    return {
        "device_type": device_type,
        "device_family": device_family,
        "browser": browser,
        "os_name": os_name,
    }


def extract_referrer_domain(ref: str) -> str | None:
    if not isinstance(ref, str) or ref.strip() == "":
        return None
    try:
        parsed = urlparse(ref)
        host = parsed.netloc or parsed.path
        host = host.strip()
        return host if host else None
    except Exception:
        return None


def load_merged(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_csv(path, low_memory=False)


def run_etl(cfg: dict):
    base_out = ensure_folder(cfg["BASE_OUTPUT_FOLDER"])
    etl_out = ensure_folder(os.path.join(base_out, cfg["ETL_SUBFOLDER"]))

    merged_path = cfg.get("INPUT_MERGED_FILE") or cfg["OUTPUT_FILE"]
    raw = load_merged(merged_path)

    ts_col = cfg["TIMESTAMP_COLUMN"]
    user_col = cfg["CLIENT_ID_COLUMN"]
    page_col = cfg["PAGE_URL_COLUMN"]
    ua_col = cfg["USER_AGENT_COLUMN"]
    event_col = cfg["EVENT_NAME_COLUMN"]
    data_col = cfg["EVENT_DATA_COLUMN"]

    # ---- timestamp & date parts ----
    raw[ts_col] = pd.to_datetime(raw[ts_col], errors="coerce")
    raw["timestamp_invalid"] = raw[ts_col].isna()
    raw["event_date"] = raw[ts_col].dt.date
    raw["event_hour"] = raw[ts_col].dt.hour
    raw["event_dayofweek"] = raw[ts_col].dt.day_name()

    # ---- page_url / category / UTM ----
    if page_col in raw.columns:
        raw[page_col] = raw[page_col].astype(str)
        raw["page_category"] = raw[page_col].apply(extract_category_from_url)

        cat_set = set(cfg["URL_CATEGORIES"])
        raw["page_category_in_config"] = raw["page_category"].isin(cat_set)

        utm_keys = cfg["UTM_COLS"]
        utm_df = raw[page_col].apply(lambda u: extract_utm_series(u, utm_keys))
        for k in utm_keys:
            if k not in raw.columns:
                raw[k] = utm_df[k]
            else:
                raw[k] = raw[k].fillna(utm_df[k])
    else:
        raw["page_category"] = None
        print("⚠️ No page_url column; category/UTM extraction skipped.")

    # ---- user agent parsing ----
    if ua_col in raw.columns:
        raw[ua_col] = raw[ua_col].fillna("").astype(str)
        ua_features = raw[ua_col].apply(lambda s: pd.Series(parse_user_agent(s)))
        for col in ["device_type", "device_family", "browser", "os_name"]:
            raw[col] = ua_features[col]
    else:
        for col in ["device_type", "device_family", "browser", "os_name"]:
            raw[col] = "unknown"
        print("ℹ️ No user_agent column; UA columns default to 'unknown'.")

    # ---- revenue / quantity / products from event_data JSON ----
    if data_col in raw.columns and event_col in raw.columns:
        try:
            metrics_df = raw.apply(
                lambda r: pd.Series(
                    extract_revenue_from_event_data(
                        r.get(data_col), r.get(event_col), cfg
                    )
                ),
                axis=1,
            )
            for col in ["order_revenue", "order_quantity", "order_currency", "order_products"]:
                if col in metrics_df.columns:
                    raw[col] = metrics_df[col]

            # NEW: additional columns on merged_puffy_etl: revenue, price, quantity, product
            try:
                basic_metrics_df = raw.apply(
                    lambda r: pd.Series(
                        extract_basic_product_metrics(
                            r.get(data_col), r.get(event_col), cfg
                        )
                    ),
                    axis=1,
                )
                for col in ["revenue", "price", "quantity", "product"]:
                    if col not in raw.columns:
                        raw[col] = basic_metrics_df[col]
                    else:
                        # only fill where original is NaN / None
                        raw[col] = raw[col].where(raw[col].notna(), basic_metrics_df[col])
            except Exception as e2:
                print(f"⚠️ Basic product metric extraction failed: {e2}")
                for col in ["revenue", "price", "quantity", "product"]:
                    if col not in raw.columns:
                        raw[col] = np.nan if col != "product" else None

        except Exception as e:
            print(f"⚠️ Revenue extraction failed: {e}")
            for col in ["order_revenue", "order_quantity", "order_currency", "order_products"]:
                if col not in raw.columns:
                    raw[col] = np.nan if "order_" in col else None
            for col in ["revenue", "price", "quantity", "product"]:
                if col not in raw.columns:
                    raw[col] = np.nan if col != "product" else None
    else:
        for col in ["order_revenue", "order_quantity", "order_currency", "order_products"]:
            if col not in raw.columns:
                raw[col] = np.nan if "order_" in col else None
        for col in ["revenue", "price", "quantity", "product"]:
            if col not in raw.columns:
                raw[col] = np.nan if col != "product" else None

    # ---- validity flags ----
    req = cfg["REQUIRED_COLUMNS"]
    for col in req:
        flag = f"missing_{col}"
        raw[flag] = ~raw[col].notna() if col in raw.columns else True

    raw["bad_page_url"] = False
    missing_any = raw[[f"missing_{c}" for c in req]].any(axis=1)
    raw["record_invalid"] = missing_any

    # ---- dedupe & clean ----
    print("🧹 Deduplicating rows…")
    compare_cols = [c for c in raw.columns if c != "source_file"]
    dedup = raw.drop_duplicates(subset=compare_cols, keep="first")

    clean = dedup[~dedup["record_invalid"]].copy()

    print(
        f"✅ Clean events: {len(clean)} / {len(dedup)} after dedup, {len(raw)} raw rows"
    )

    # ---- sessionization (30-minute inactivity window per client_id) ----
    if user_col in clean.columns and ts_col in clean.columns:
        clean = clean.sort_values([user_col, ts_col])
        session_gap = pd.Timedelta(minutes=30)

        new_session = (
            (clean[user_col].shift() != clean[user_col])
            | (clean[ts_col].isna())
            | (clean[ts_col].shift().isna())
            | ((clean[ts_col] - clean[ts_col].shift()) > session_gap)
        )

        clean["session_index"] = new_session.cumsum()
        clean["session_id"] = (
            clean[user_col].astype(str) + "_" + clean["session_index"].astype(str)
        )
    else:
        clean["session_index"] = np.nan
        clean["session_id"] = np.nan
        print("ℹ️ Sessionization skipped (missing client_id or timestamp).")

    # move flags to the end
    flag_cols = [
        c
        for c in clean.columns
        if c.startswith("missing_")
        or c.endswith("_invalid")
        or c in ["bad_page_url", "record_invalid"]
    ]
    non_flag_cols = [c for c in clean.columns if c not in flag_cols]
    clean = clean[non_flag_cols + flag_cols]

    # ---- ETL summary ----
    summary = pd.DataFrame(
        [
            {"metric": "raw_rows", "value": len(raw)},
            {"metric": "rows_after_dedup", "value": len(dedup)},
            {"metric": "clean_rows", "value": len(clean)},
            {
                "metric": "clean_rows_pct",
                "value": round(len(clean) / len(dedup) * 100, 2)
                if len(dedup)
                else 0.0,
            },
        ]
    )

    if "source_file" in raw.columns:
        per_file = (
            raw.groupby("source_file")
            .agg(raw_rows=("source_file", "size"))
            .reset_index()
        )
        dedup_pf = (
            dedup.groupby("source_file")
            .agg(rows_after_dedup=("source_file", "size"))
            .reset_index()
        )
        clean_pf = (
            clean.groupby("source_file")
            .agg(clean_rows=("source_file", "size"))
            .reset_index()
        )
        recon_pf = per_file.merge(dedup_pf, on="source_file", how="left").merge(
            clean_pf, on="source_file", how="left"
        )
        recon_pf["dropped_invalid"] = (
            recon_pf["rows_after_dedup"] - recon_pf["clean_rows"]
        )
    else:
        recon_pf = pd.DataFrame()

    # ---- save ETL outputs ----
    etl_csv = os.path.join(etl_out, "merged_puffy_etl.csv")
    clean.to_csv(etl_csv, index=False)

    etl_parquet = os.path.join(etl_out, "merged_puffy_etl.parquet")
    try:
        clean.to_parquet(etl_parquet, index=False)
    except Exception as e:
        print(f"⚠️ Could not write Parquet: {e}")

    etl_summary_csv = os.path.join(etl_out, "etl_summary.csv")
    summary.to_csv(etl_summary_csv, index=False)

    etl_summary_xlsx = etl_summary_csv.replace(".csv", ".xlsx")
    with pd.ExcelWriter(etl_summary_xlsx, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        if not recon_pf.empty:
            recon_pf.to_excel(writer, sheet_name="per_file", index=False)

    print(f"💾 ETL CSV → {etl_csv}")
    print(f"💾 ETL summary → {etl_summary_xlsx}")

    # Reconciliation export
    recon_out = ensure_folder(os.path.join(base_out, cfg["RECON_SUBFOLDER"]))
    recon_xlsx = os.path.join(recon_out, "reconciliation.xlsx")
    with pd.ExcelWriter(recon_xlsx, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="overall", index=False)
        if not recon_pf.empty:
            recon_pf.to_excel(writer, sheet_name="per_source_file", index=False)

    print(f"📊 Reconciliation workbook → {recon_xlsx}")

    return raw, dedup, clean, base_out


# ---------------- Journeys / funnel ----------------

def build_user_journeys(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    user_col = cfg["CLIENT_ID_COLUMN"]
    cat_col = "page_category"
    journeys: list[dict] = []

    for uid, grp in df.groupby(user_col):
        seq = list(grp[cat_col].astype(str))
        cleaned = [seq[i] for i in range(len(seq)) if i == 0 or seq[i] != seq[i - 1]]
        user_revenue = grp.get("order_revenue", pd.Series([0])).fillna(0).sum()
        journeys.append(
            {
                "user_id": uid,
                "journey": " → ".join(cleaned),
                "steps": len(cleaned),
                "conversions": (
                    grp[cfg["EVENT_NAME_COLUMN"]] == cfg["CONVERSION_EVENT"]
                ).sum(),
                "revenue": user_revenue,
            }
        )
    return pd.DataFrame(journeys)


def build_funnel_df(clean: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    user_col = cfg["CLIENT_ID_COLUMN"]
    cat_col = "page_category"
    stages = cfg["FUNNEL_STAGES"]
    stats: list[dict] = []

    for i, stage in enumerate(stages):
        users_reached = clean[clean[cat_col] == stage][user_col].nunique()
        if i < len(stages) - 1:
            next_stage = stages[i + 1]
            users_next = clean[clean[cat_col] == next_stage][user_col].nunique()
        else:
            next_stage = None
            users_next = None
        dropoff = users_reached - (users_next or 0)
        stats.append(
            {
                "category_stage": stage,
                "users_reached": users_reached,
                "next_stage": next_stage,
                "users_next_stage": users_next,
                "dropoff": dropoff,
            }
        )
    return pd.DataFrame(stats)


def build_category_transitions(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    user_col = cfg["CLIENT_ID_COLUMN"]
    cat_col = "page_category"
    records: list[tuple[str, str]] = []
    for _, grp in df.groupby(user_col):
        cats = list(grp[cat_col])
        for i in range(len(cats) - 1):
            src, dst = cats[i], cats[i + 1]
            if pd.isna(src) or pd.isna(dst) or src == dst:
                continue
            records.append((src, dst))
    if not records:
        return pd.DataFrame(columns=["source", "target", "count"])
    trans = pd.DataFrame(records, columns=["source", "target"])
    trans = (
        trans.groupby(["source", "target"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    return trans


def journey_and_funnel(clean: pd.DataFrame, base_out: str, cfg: dict) -> None:
    journey_out = ensure_folder(os.path.join(base_out, cfg["JOURNEY_SUBFOLDER"]))

    journeys = build_user_journeys(clean, cfg)
    funnel_df = build_funnel_df(clean, cfg)
    transitions = build_category_transitions(clean, cfg)

    jf_xlsx = os.path.join(journey_out, "journey_funnel_results.xlsx")
    with pd.ExcelWriter(jf_xlsx, engine="openpyxl") as writer:
        journeys.to_excel(writer, sheet_name="journeys", index=False)
        funnel_df.to_excel(writer, sheet_name="funnel", index=False)
        transitions.to_excel(writer, sheet_name="category_transitions", index=False)

        if not journeys.empty and "revenue" in journeys.columns:
            journey_rev = (
                journeys.groupby("journey")
                .agg(
                    users=("user_id", "nunique"),
                    total_conversions=("conversions", "sum"),
                    total_revenue=("revenue", "sum"),
                )
                .reset_index()
                .sort_values("total_revenue", ascending=False)
            )
            journey_rev.to_excel(writer, sheet_name="journey_revenue", index=False)

    if not journeys.empty and "revenue" in journeys.columns:
        journey_rev = (
            journeys.groupby("journey")
            .agg(
                users=("user_id", "nunique"),
                total_conversions=("conversions", "sum"),
                total_revenue=("revenue", "sum"),
            )
            .reset_index()
            .sort_values("total_revenue", ascending=False)
        )
        journey_rev.to_csv(
            os.path.join(journey_out, "journey_revenue_summary.csv"), index=False
        )

    if not journeys.empty:
        counts = journeys["journey"].value_counts().nlargest(cfg["TOP_N_JOURNEYS"])
        chart_df = counts.rename_axis("journey").reset_index(name="count")
        chart_df.to_csv(
            os.path.join(journey_out, "chart_top_journeys.csv"), index=False
        )
        plt.figure(figsize=(10, 8))
        counts.plot(kind="barh")
        plt.title("Top User Journeys")
        plt.xlabel("Count")
        plt.tight_layout()
        plt.savefig(os.path.join(journey_out, "top_journeys.png"))
        plt.close()

    if not funnel_df.empty:
        funnel_df.to_csv(os.path.join(journey_out, "chart_funnel.csv"), index=False)
        plt.figure(figsize=(8, 5))
        plt.plot(funnel_df["category_stage"], funnel_df["users_reached"], marker="o")
        plt.title("Funnel Dropoff (by category)")
        plt.xlabel("Category stage")
        plt.ylabel("Users reached")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(journey_out, "funnel_dropoff.png"))
        plt.close()

    transitions.to_csv(
        os.path.join(journey_out, "chart_sankey_transitions.csv"), index=False
    )
    try:
        import plotly.graph_objects as go

        if not transitions.empty:
            labels = sorted(
                set(transitions["source"]).union(set(transitions["target"]))
            )
            idx_map = {lab: i for i, lab in enumerate(labels)}
            sources = transitions["source"].map(idx_map).tolist()
            targets = transitions["target"].map(idx_map).tolist()
            values = transitions["count"].tolist()

            sankey_trace = go.Sankey(
                node=dict(label=labels, pad=15, thickness=20),
                link=dict(source=sources, target=targets, value=values),
            )
            fig = go.Figure(data=[sankey_trace])
            fig.update_layout(
                title_text="Category Path Sankey Diagram", font_size=10
            )
            html_path = os.path.join(journey_out, "journey_sankey.html")
            fig.write_html(html_path)
            try:
                png_path = os.path.join(journey_out, "journey_sankey.png")
                fig.write_image(png_path)
            except Exception:
                pass
    except ImportError:
        print("ℹ️ plotly not installed; Sankey HTML skipped.")

    print(f"📁 Journey/funnel outputs → {journey_out}")


# ---------------- Attribution ----------------

def build_channel_columns(clean: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = clean.copy()
    utm_cols = cfg["UTM_COLS"]
    ref_col = cfg["REFERRER_COLUMN"]

    def utm_channel(row):
        vals = [row.get(c) for c in utm_cols]
        if not any(pd.notna(v) and str(v).strip() for v in vals):
            return "direct"
        src = str(row.get("utm_source") or "").strip()
        med = str(row.get("utm_medium") or "").strip()
        camp = str(row.get("utm_campaign") or "").strip()
        parts = [p for p in [src, med, camp] if p]
        return " / ".join(parts) if parts else "direct"

    df["utm_channel"] = df.apply(utm_channel, axis=1)

    def cat_channel(row):
        cat = row.get("page_category")
        return str(cat).strip() if pd.notna(cat) and str(cat).strip() else "direct"

    df["category_channel"] = df.apply(cat_channel, axis=1)

    def ref_channel(row):
        dom = extract_referrer_domain(row.get(ref_col))
        return dom if dom else "direct"

    df["referrer_channel"] = df.apply(ref_channel, axis=1)

    df["utm_referrer_channel"] = (
        df["utm_channel"].astype(str) + " | " + df["referrer_channel"].astype(str)
    )

    return df


def run_attr_for_channel(
    df: pd.DataFrame, cfg: dict, channel_col: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    user_col = cfg["CLIENT_ID_COLUMN"]
    ts_col = cfg["TIMESTAMP_COLUMN"]
    ev_col = cfg["EVENT_NAME_COLUMN"]
    conv_event = cfg["CONVERSION_EVENT"]
    lookback = pd.Timedelta(days=cfg["ATTRIBUTION_LOOKBACK_DAYS"])

    df = df.sort_values([user_col, ts_col])

    first_records: list[dict] = []
    last_records: list[dict] = []

    for uid, grp in df.groupby(user_col):
        touches = grp[
            grp[channel_col].notna()
            & (grp[channel_col].astype(str).str.strip() != "")
        ]
        convs = grp[grp[ev_col] == conv_event]
        if touches.empty or convs.empty:
            continue

        for _, conv in convs.iterrows():
            t = conv[ts_col]
            window = touches[
                (touches[ts_col] >= t - lookback) & (touches[ts_col] <= t)
            ]
            if window.empty:
                continue

            ft = window.iloc[0]
            lt = window.iloc[-1]

            conv_revenue = _safe_float(conv.get("order_revenue"))

            first_records.append(
                {
                    "channel": ft[channel_col],
                    "device_type": ft["device_type"],
                    "device_family": ft["device_family"],
                    "browser": ft["browser"],
                    "os_name": ft["os_name"],
                    "user_id": uid,
                    "order_revenue": conv_revenue,
                }
            )
            last_records.append(
                {
                    "channel": lt[channel_col],
                    "device_type": lt["device_type"],
                    "device_family": lt["device_family"],
                    "browser": lt["browser"],
                    "os_name": lt["os_name"],
                    "user_id": uid,
                    "order_revenue": conv_revenue,
                }
            )

    def aggregate(records: list[dict]) -> pd.DataFrame:
        if not records:
            return pd.DataFrame(
                columns=[
                    "channel",
                    "device_type",
                    "device_family",
                    "browser",
                    "os_name",
                    "conversions",
                    "unique_users",
                    "total_revenue",
                    "avg_revenue",
                ]
            )
        df_rec = pd.DataFrame(records)
        agg = (
            df_rec.groupby(
                ["channel", "device_type", "device_family", "browser", "os_name"]
            )
            .agg(
                conversions=("user_id", "size"),
                unique_users=("user_id", "nunique"),
                total_revenue=("order_revenue", "sum"),
                avg_revenue=("order_revenue", "mean"),
            )
            .reset_index()
            .sort_values("conversions", ascending=False)
        )
        return agg

    return aggregate(first_records), aggregate(last_records)


def attribution(clean: pd.DataFrame, base_out: str, cfg: dict) -> None:
    attr_out = ensure_folder(os.path.join(base_out, cfg["ATTRIBUTION_SUBFOLDER"]))
    df = build_channel_columns(clean, cfg)

    print("🎯 Running attribution for UTM channel…")
    utm_first, utm_last = run_attr_for_channel(df, cfg, "utm_channel")

    print("🎯 Running attribution for category channel…")
    cat_first, cat_last = run_attr_for_channel(df, cfg, "category_channel")

    print("🎯 Running attribution for referrer channel…")
    ref_first, ref_last = run_attr_for_channel(df, cfg, "referrer_channel")

    print("🎯 Running combined UTM+referrer attribution…")
    combo_first, combo_last = run_attr_for_channel(df, cfg, "utm_referrer_channel")

    attr_xlsx = os.path.join(attr_out, "attribution_results.xlsx")
    with pd.ExcelWriter(attr_xlsx, engine="openpyxl") as writer:
        utm_first.to_excel(writer, sheet_name="utm_first_click", index=False)
        utm_last.to_excel(writer, sheet_name="utm_last_click", index=False)
        cat_first.to_excel(writer, sheet_name="category_first_click", index=False)
        cat_last.to_excel(writer, sheet_name="category_last_click", index=False)
        ref_first.to_excel(writer, sheet_name="referrer_first_click", index=False)
        ref_last.to_excel(writer, sheet_name="referrer_last_click", index=False)
        combo_first.to_excel(writer, sheet_name="utm_ref_first_click", index=False)
        combo_last.to_excel(writer, sheet_name="utm_ref_last_click", index=False)

    print(f"📁 Attribution outputs → {attr_out}")


# ===================================================================
# PART 3 – ADVANCED & REVENUE ANALYSIS
# ===================================================================

def build_utm_channel(df: pd.DataFrame, utm_cols: list[str]) -> pd.Series:
    """Create a utm_channel string from UTM cols; fall back to 'direct'."""
    def _utm(row):
        vals = [row.get(c) for c in utm_cols]
        if not any(pd.notna(v) and str(v).strip() for v in vals):
            return "direct"
        src = str(row.get("utm_source") or "").strip()
        med = str(row.get("utm_medium") or "").strip()
        camp = str(row.get("utm_campaign") or "").strip()
        parts = [p for p in [src, med, camp] if p]
        return " / ".join(parts) if parts else "direct"

    return df.apply(_utm, axis=1)


def run_revenue_analysis(clean: pd.DataFrame, base_out: str, cfg: dict) -> None:
    """
    Dedicated revenue + conversion path analysis, called from Advanced Analysis.

    Outputs (under BASE_OUTPUT_FOLDER/revenue):
    - revenue_overview.(csv/xlsx)
    - revenue_by_day.csv
    - revenue_by_utm_channel.csv
    - revenue_conversion_paths_by_journey.csv
    """
    rev_out = ensure_folder(os.path.join(base_out, "revenue"))

    df = clean.copy()
    ts_col = cfg["TIMESTAMP_COLUMN"]
    event_col = cfg["EVENT_NAME_COLUMN"]
    conv_event = cfg["CONVERSION_EVENT"]

    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df["event_date"] = df[ts_col].dt.date

    conv = df[df[event_col] == conv_event].copy()
    conv["order_revenue"] = _safe_float(conv.get("order_revenue", np.nan))
    conv["order_quantity"] = _safe_float(conv.get("order_quantity", np.nan))

    total_conv = len(conv)
    total_revenue = conv["order_revenue"].sum(skipna=True)
    total_quantity = conv["order_quantity"].sum(skipna=True)
    avg_order_value = total_revenue / total_conv if total_conv else 0.0

    overview = pd.DataFrame(
        [
            {"metric": "total_conversions", "value": total_conv},
            {"metric": "total_revenue", "value": total_revenue},
            {"metric": "total_quantity", "value": total_quantity},
            {"metric": "avg_order_value", "value": avg_order_value},
        ]
    )

    overview_csv = os.path.join(rev_out, "revenue_overview.csv")
    overview.to_csv(overview_csv, index=False)
    overview_xlsx = os.path.join(rev_out, "revenue_overview.xlsx")
    with pd.ExcelWriter(overview_xlsx, engine="openpyxl") as writer:
        overview.to_excel(writer, sheet_name="overview", index=False)

    print(f"💶 Revenue overview → {overview_csv}")

    if not conv.empty:
        rev_by_day = (
            conv.groupby("event_date")
            .agg(
                conversions=(event_col, "size"),
                revenue=("order_revenue", "sum"),
                quantity=("order_quantity", "sum"),
            )
            .reset_index()
            .sort_values("event_date")
        )
        rev_by_day_csv = os.path.join(rev_out, "revenue_by_day.csv")
        rev_by_day.to_csv(rev_by_day_csv, index=False)
        print(f"💶 Revenue by day → {rev_by_day_csv}")

        conv_channels = build_channel_columns(conv, cfg)
        rev_by_utm = (
            conv_channels.groupby("utm_channel")
            .agg(
                conversions=(event_col, "size"),
                revenue=("order_revenue", "sum"),
                avg_order_value=("order_revenue", "mean"),
            )
            .reset_index()
            .sort_values("revenue", ascending=False)
        )
        rev_by_utm_csv = os.path.join(rev_out, "revenue_by_utm_channel.csv")
        rev_by_utm.to_csv(rev_by_utm_csv, index=False)
        print(f"💶 Revenue by UTM channel → {rev_by_utm_csv}")

    journeys = build_user_journeys(df, cfg)
    if not journeys.empty:
        conv_paths = journeys[journeys["conversions"] > 0].copy()
        if "revenue" in conv_paths.columns:
            paths_summary = (
                conv_paths.groupby("journey")
                .agg(
                    users=("user_id", "nunique"),
                    conversions=("conversions", "sum"),
                    total_revenue=("revenue", "sum"),
                    avg_revenue_per_user=("revenue", "mean"),
                )
                .reset_index()
                .sort_values("total_revenue", ascending=False)
            )
            paths_csv = os.path.join(
                rev_out, "revenue_conversion_paths_by_journey.csv"
            )
            paths_summary.to_csv(paths_csv, index=False)
            print(f"🧭 Revenue by conversion path (journey) → {paths_csv}")


def run_advanced_analysis(clean: pd.DataFrame, base_out: str, cfg: dict) -> None:
    """
    Advanced + Revenue analysis combined:
    - Acquisition funnel
    - UTM funnel (top channels)
    - UTM next-step funnel
    - UTM attribution (first/last touch, with revenue)
    - UTM attribution by device (with revenue)
    - Device mix at conversion (with revenue)
    - Full revenue analysis (overview, by day, by UTM, by path)
    """
    out_dir = ensure_folder(os.path.join(base_out, cfg["ANALYSIS_SUBFOLDER"]))

    df = clean.copy()
    df["timestamp"] = pd.to_datetime(df[cfg["TIMESTAMP_COLUMN"]], errors="coerce")

    n_rows = len(df)
    n_users = df[cfg["CLIENT_ID_COLUMN"]].nunique(dropna=True)
    date_min = df["timestamp"].min()
    date_max = df["timestamp"].max()
    n_conversions = int((df[cfg["EVENT_NAME_COLUMN"]] == cfg["CONVERSION_EVENT"]).sum())

    print(
        f"[INFO] Advanced analysis on {n_rows:,} rows, {n_users:,} users, "
        f"{n_conversions} {cfg['CONVERSION_EVENT']} events "
        f"from {date_min.date()} to {date_max.date()}."
    )

    # Acquisition funnel
    df["page_category_for_funnel"] = df["page_category"]
    df.loc[
        df[cfg["EVENT_NAME_COLUMN"]] == cfg["CONVERSION_EVENT"],
        "page_category_for_funnel",
    ] = cfg["CONVERSION_EVENT"]

    funnel_order = cfg["FUNNEL_STAGES"]
    funnel_data = (
        df.groupby("page_category_for_funnel")[cfg["CLIENT_ID_COLUMN"]]
        .nunique()
        .reindex(funnel_order)
        .reset_index(name="unique_users")
    )

    funnel_csv_path = os.path.join(out_dir, "chart_funnel_data.csv")
    funnel_data.to_csv(funnel_csv_path, index=False)
    print(f"[INFO] Saved overall funnel data -> {funnel_csv_path}")

    plt.figure(figsize=(8, 5))
    plt.plot(funnel_data["page_category_for_funnel"], funnel_data["unique_users"], marker="o")
    plt.title("Puffy Acquisition Funnel (Unique Users by Stage)")
    plt.xlabel("Funnel stage")
    plt.ylabel("Unique users")
    plt.grid(True)
    plt.tight_layout()
    funnel_chart_path = os.path.join(out_dir, "chart_funnel_overall.png")
    plt.savefig(funnel_chart_path)
    plt.close()
    print(f"[INFO] Saved funnel chart -> {funnel_chart_path}")

    # UTM attribution 7-day window
    utm_cols = cfg["UTM_COLS"]
    df["utm_channel"] = build_utm_channel(df, utm_cols)

    lookback = pd.Timedelta(days=cfg["ATTRIBUTION_LOOKBACK_DAYS"])
    df_sorted = df.sort_values([cfg["CLIENT_ID_COLUMN"], "timestamp"])

    first_touches: list[dict] = []
    last_touches: list[dict] = []

    for client_id, grp in df_sorted.groupby(cfg["CLIENT_ID_COLUMN"]):
        grp = grp.sort_values("timestamp")
        convs = grp[grp[cfg["EVENT_NAME_COLUMN"]] == cfg["CONVERSION_EVENT"]]
        if convs.empty:
            continue

        for _, conv in convs.iterrows():
            t_conv = conv["timestamp"]
            window = grp[
                (grp["timestamp"] >= t_conv - lookback)
                & (grp["timestamp"] <= t_conv)
            ]
            window = window[
                window["utm_channel"].notna()
                & (window["utm_channel"].astype(str) != "")
            ]

            if window.empty:
                ch_first = "unknown"
                ch_last = "unknown"
                dev_type = conv.get("device_type")
                dev_family = conv.get("device_family")
                browser = conv.get("browser")
                os_name = conv.get("os_name")
            else:
                first_row = window.iloc[0]
                last_row = window.iloc[-1]
                ch_first = first_row["utm_channel"]
                ch_last = last_row["utm_channel"]
                dev_type = first_row.get("device_type")
                dev_family = first_row.get("device_family")
                browser = first_row.get("browser")
                os_name = first_row.get("os_name")

            revenue = _safe_float(conv.get("order_revenue"))
            quantity = _safe_float(conv.get("order_quantity"))
            currency = conv.get("order_currency")
            products = conv.get("order_products")

            common = {
                "client_id": client_id,
                "conversion_timestamp": t_conv,
                "device_type": dev_type,
                "device_family": dev_family,
                "browser": browser,
                "os_name": os_name,
                "order_revenue": revenue,
                "order_quantity": quantity,
                "order_currency": currency,
                "order_products": products,
            }
            first_touches.append(
                {**common, "channel": ch_first, "model": "first_touch"}
            )
            last_touches.append(
                {**common, "channel": ch_last, "model": "last_touch"}
            )

    attr_raw = pd.DataFrame(first_touches + last_touches)
    attr_raw_path = os.path.join(out_dir, "attribution_utm_first_last_raw.csv")
    attr_raw.to_csv(attr_raw_path, index=False)
    print(f"[INFO] Saved attribution raw -> {attr_raw_path}")

    attr_agg = (
        attr_raw.groupby(["model", "channel"])
        .agg(
            conversions=("client_id", "size"),
            total_revenue=("order_revenue", "sum"),
            avg_revenue=("order_revenue", "mean"),
        )
        .reset_index()
        .sort_values(["model", "conversions"], ascending=[True, False])
    )
    attr_agg_path = os.path.join(out_dir, "attribution_utm_first_last_agg.csv")
    attr_agg.to_csv(attr_agg_path, index=False)
    print(f"[INFO] Saved attribution aggregated -> {attr_agg_path}")

    top_n = cfg["TOP_CHANNELS_FOR_ATTR"]
    for model in ["first_touch", "last_touch"]:
        sub = attr_agg[attr_agg["model"] == model].head(top_n)
        if sub.empty:
            continue
        plt.figure(figsize=(9, 5))
        plt.barh(sub["channel"], sub["conversions"])
        plt.gca().invert_yaxis()
        plt.title(
            f"Puffy {model.replace('_', ' ').title()} Attribution – Top {top_n} UTM Channels"
        )
        plt.xlabel("Conversions")
        plt.tight_layout()
        out_path = os.path.join(out_dir, f"chart_attribution_{model}.png")
        plt.savefig(out_path)
        plt.close()
        print(f"[INFO] Saved {model} attribution chart -> {out_path}")

    # UTM-only attribution totals by device & device_family (with revenue)
    utm_attr_device = (
        attr_raw.groupby(
            ["model", "channel", "device_type", "device_family"], dropna=False
        )
        .agg(
            conversions=("client_id", "size"),
            total_revenue=("order_revenue", "sum"),
            avg_revenue=("order_revenue", "mean"),
        )
        .reset_index()
        .sort_values(["model", "conversions"], ascending=[True, False])
    )
    utm_attr_device_path = os.path.join(out_dir, "utm_attribution_by_device.csv")
    utm_attr_device.to_csv(utm_attr_device_path, index=False)
    print(f"[INFO] Saved UTM attribution by device -> {utm_attr_device_path}")

    # UTM funnel for top channels
    df["page_category_for_funnel"] = df["page_category"]
    df.loc[
        df[cfg["EVENT_NAME_COLUMN"]] == cfg["CONVERSION_EVENT"],
        "page_category_for_funnel",
    ] = cfg["CONVERSION_EVENT"]

    top_channels = (
        attr_raw[attr_raw["model"] == "last_touch"]
        .groupby("channel")["client_id"]
        .nunique()
        .sort_values(ascending=False)
        .head(cfg["TOP_UTM_CHANNELS_FOR_FUNNEL"])
        .index.tolist()
    )

    utm_funnel_records: list[dict] = []
    for ch in top_channels:
        sub = df[df["utm_channel"] == ch]
        for stage in funnel_order:
            users = (
                sub[sub["page_category_for_funnel"] == stage][cfg["CLIENT_ID_COLUMN"]]
                .nunique()
            )
            utm_funnel_records.append(
                {
                    "utm_channel": ch,
                    "funnel_stage": stage,
                    "unique_users": users,
                }
            )

    utm_funnel_df = pd.DataFrame(utm_funnel_records)
    utm_funnel_path = os.path.join(out_dir, "utm_funnel_top_channels.csv")
    utm_funnel_df.to_csv(utm_funnel_path, index=False)
    print(f"[INFO] Saved UTM funnel per top channels -> {utm_funnel_path}")

    # UTM next-step funnel
    utm_next_records: list[dict] = []
    lookahead = pd.Timedelta(days=cfg["ATTRIBUTION_LOOKBACK_DAYS"])

    for client_id, grp in df_sorted.groupby(cfg["CLIENT_ID_COLUMN"]):
        grp = grp.sort_values("timestamp")
        utm_events = grp[grp["utm_channel"] != "direct"]
        if utm_events.empty:
            continue

        for _, utm_row in utm_events.iterrows():
            t0 = utm_row["timestamp"]
            ch = utm_row["utm_channel"]

            future = grp[
                (grp["timestamp"] > t0) & (grp["timestamp"] <= t0 + lookahead)
            ]
            if future.empty:
                continue

            future = future.sort_values("timestamp")
            ev_seq = list(future[cfg["EVENT_NAME_COLUMN"]].astype(str).fillna("NA"))
            if not ev_seq:
                continue

            seq = [ev_seq[0]]
            for e in ev_seq[1:]:
                if e != seq[-1]:
                    seq.append(e)

            truncated: list[str] = []
            for e in seq:
                truncated.append(e)
                if e == cfg["CONVERSION_EVENT"]:
                    break
            seq = truncated
            if not seq:
                continue

            for idx, e in enumerate(seq, start=1):
                utm_next_records.append(
                    {
                        "client_id": client_id,
                        "utm_channel": ch,
                        "utm_start_ts": t0,
                        "step_index": idx,
                        "event_name": e,
                    }
                )

    utm_next_df = pd.DataFrame(utm_next_records)
    utm_next_df_path = os.path.join(out_dir, "utm_next_step_paths_raw.csv")
    utm_next_df.to_csv(utm_next_df_path, index=False)
    print(f"[INFO] Saved UTM next-step raw paths -> {utm_next_df_path}")

    if not utm_next_df.empty:
        utm_step_funnel = (
            utm_next_df.groupby(["step_index", "event_name"])["client_id"]
            .nunique()
            .reset_index(name="unique_clients")
            .sort_values(["step_index", "unique_clients"], ascending=[True, False])
        )
    else:
        utm_step_funnel = pd.DataFrame(
            columns=["step_index", "event_name", "unique_clients"]
        )

    utm_step_funnel_path = os.path.join(out_dir, "utm_next_step_funnel.csv")
    utm_step_funnel.to_csv(utm_step_funnel_path, index=False)
    print(f"[INFO] Saved UTM next-step funnel agg -> {utm_step_funnel_path}")

    if not utm_step_funnel.empty:
        per_step = (
            utm_step_funnel.groupby("step_index")["unique_clients"]
            .sum()
            .reset_index()
            .sort_values("step_index")
        )
        plt.figure(figsize=(8, 5))
        plt.plot(per_step["step_index"], per_step["unique_clients"], marker="o")
        plt.title("UTM Next-step Funnel – Unique Clients per Step (7-day window)")
        plt.xlabel("Step index after UTM event")
        plt.ylabel("Unique clients")
        plt.grid(True)
        plt.tight_layout()
        utm_next_step_chart_path = os.path.join(
            out_dir, "chart_utm_next_step_funnel.png"
        )
        plt.savefig(utm_next_step_chart_path)
        plt.close()
        print(f"[INFO] Saved UTM next-step funnel chart -> {utm_next_step_chart_path}")

    # Device mix at conversion (+ revenue)
    conversions = df[df[cfg["EVENT_NAME_COLUMN"]] == cfg["CONVERSION_EVENT"]].copy()
    conversions["order_revenue"] = conversions.get("order_revenue", np.nan)

    device_mix = (
        conversions.groupby("device_type")
        .agg(
            conversions=(cfg["EVENT_NAME_COLUMN"], "size"),
            revenue=("order_revenue", "sum"),
        )
        .reset_index()
    )
    device_mix_path = os.path.join(out_dir, "chart_device_mix_data.csv")
    device_mix.to_csv(device_mix_path, index=False)
    print(f"[INFO] Saved device mix data -> {device_mix_path}")

    plt.figure(figsize=(6, 6))
    plt.pie(device_mix["conversions"], labels=device_mix["device_type"], autopct="%1.1f%%")
    plt.title("Device Mix at Conversion")
    plt.tight_layout()
    device_chart_path = os.path.join(out_dir, "chart_device_mix.png")
    plt.savefig(device_chart_path)
    plt.close()
    print(f"[INFO] Saved device mix chart -> {device_chart_path}")

    # FINAL: run dedicated revenue analysis as part of Advanced Analysis
    run_revenue_analysis(clean, base_out, cfg)

    print("\n[DONE] Advanced + Revenue analysis finished. All artefacts in:")
    print(out_dir)


# ===================================================================
# PART 4 – PRODUCTION MONITORING
# ===================================================================

def run_monitoring(clean: pd.DataFrame, base_out: str, cfg: dict) -> None:
    """
    Production monitoring:
    - Build daily metrics for events, users, conversions, revenue.
    - Compute simple z-score based anomaly flags.
    - Export CSVs for alerting / dashboards.
    """
    monitor_out = ensure_folder(os.path.join(base_out, "monitoring"))

    ts_col = cfg["TIMESTAMP_COLUMN"]
    client_col = cfg["CLIENT_ID_COLUMN"]
    event_col = cfg["EVENT_NAME_COLUMN"]
    conv_event = cfg["CONVERSION_EVENT"]

    df = clean.copy()
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
    df["event_date"] = df[ts_col].dt.date

    daily = (
        df.groupby("event_date")
        .agg(
            total_events=(event_col, "size"),
            distinct_clients=(client_col, "nunique"),
            conversions=(event_col, lambda s: (s == conv_event).sum()),
        )
        .reset_index()
        .sort_values("event_date")
    )

    revenue_col = None
    for cand in ["order_revenue", "value", "revenue", "amount", "total", "order_value"]:
        if cand in df.columns and np.issubdtype(df[cand].dtype, np.number):
            revenue_col = cand
            break

    if revenue_col:
        rev_daily = (
            df[df[event_col] == conv_event]
            .groupby("event_date")[revenue_col]
            .sum()
            .reindex(daily["event_date"])
            .reset_index(drop=True)
        )
        daily["revenue"] = rev_daily.fillna(0.0)
    else:
        daily["revenue"] = np.nan

    metric_cols = ["total_events", "distinct_clients", "conversions"]
    if revenue_col:
        metric_cols.append("revenue")

    for col in metric_cols:
        series = daily[col].astype(float)
        mean = series.mean()
        std = series.std(ddof=0)
        if std == 0 or np.isnan(std):
            daily[f"{col}_zscore"] = 0.0
        else:
            daily[f"{col}_zscore"] = (series - mean) / std

    anomaly_mask = np.zeros(len(daily), dtype=bool)
    for col in metric_cols:
        anomaly_mask |= daily[f"{col}_zscore"].abs() >= 3.0

    daily["is_anomaly"] = anomaly_mask

    device_mix = (
        df[df[event_col] == conv_event]
        .groupby(["event_date", "device_type"])[event_col]
        .size()
        .reset_index(name="conversions")
    )
    total_per_day = (
        device_mix.groupby("event_date")["conversions"].transform("sum")
    )
    device_mix["conversion_share"] = device_mix["conversions"] / total_per_day

    daily_metrics_csv = os.path.join(monitor_out, "monitoring_daily_metrics.csv")
    anomalies_csv = os.path.join(monitor_out, "monitoring_anomalies_only.csv")
    device_mix_csv = os.path.join(monitor_out, "monitoring_device_mix.csv")

    daily.to_csv(daily_metrics_csv, index=False)
    daily[daily["is_anomaly"]].to_csv(anomalies_csv, index=False)
    device_mix.to_csv(device_mix_csv, index=False)

    print(f"📈 Monitoring daily metrics → {daily_metrics_csv}")
    print(f"⚠️ Monitoring anomalies → {anomalies_csv}")
    print(f"📊 Monitoring device mix → {device_mix_csv}")


# ===================================================================
# MAIN ORCHESTRATOR
# ===================================================================

def main():
    cfg = CONFIG
    print("🚀 PUFFY FULL PIPELINE START")

    # 1) Merge + QA
    merge_csv_files(
        folder_path=cfg["INPUT_FOLDER"],
        output_file=cfg["OUTPUT_FILE"],
        export_cleaned=True,
    )

    cfg["INPUT_MERGED_FILE"] = cfg["OUTPUT_FILE"]

    # 2) ETL + Journey + Attribution + Advanced+Revenue + Monitoring
    try:
        raw, dedup, clean, base_out = run_etl(cfg)
        journey_and_funnel(clean, base_out, cfg)
        attribution(clean, base_out, cfg)
        run_advanced_analysis(clean, base_out, cfg)
        run_monitoring(clean, base_out, cfg)
        print("\n🎉 Pipeline complete.")
        print(f"Base output folder: {base_out}")
    except Exception as e:
        print("\n❌ FATAL ERROR:")
        print(str(e))


if __name__ == "__main__":
    main()
