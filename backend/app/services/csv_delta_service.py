import pandas as pd
from pathlib import Path
from fastapi import HTTPException, UploadFile
from typing import List
import io
import tempfile
import os


def detect_key(df: pd.DataFrame) -> List[str]:
    """Detect key column - prioritize 'event_id' for TicketSqueeze."""
    for c in df.columns:
        if c.lower() in ["event_id", "id"]:
            return [c]
    return [df.columns[0]]


def compute_csv_delta(
    old_csv_content: bytes,
    new_csv_content: bytes,
    keys: str = "event_id",
    output_path: Path = None
) -> dict:
    """
    Compute delta between two CSV files.
    
    Returns dict with:
    - delta_df: pandas DataFrame with delta records
    - summary: dict with counts (added, removed, changed)
    - csv_path: path to saved CSV if output_path provided
    """
    # Read CSVs as strings to preserve exact values
    df_old = pd.read_csv(io.BytesIO(old_csv_content), dtype=str).fillna("")
    df_new = pd.read_csv(io.BytesIO(new_csv_content), dtype=str).fillna("")

    key_cols = [k.strip() for k in keys.split(",")] if keys else detect_key(df_old)

    # Validate keys
    for k in key_cols:
        if k not in df_old.columns or k not in df_new.columns:
            raise HTTPException(status_code=400, detail=f"Key '{k}' not in both files")

    # Index for fast lookup
    df_old_idx = df_old.set_index(key_cols, drop=False)
    df_new_idx = df_new.set_index(key_cols, drop=False)

    idx_old = df_old_idx.index
    idx_new = df_new_idx.index

    added_idx = idx_new.difference(idx_old)
    removed_idx = idx_old.difference(idx_new)
    common_idx = idx_old.intersection(idx_new)

    records = []

    def prefix_df(df: pd.DataFrame, prefix: str, keep_keys: List[str]) -> pd.DataFrame:
        df_copy = df.copy()
        cols = []
        for c in df_copy.columns:
            if c in keep_keys:
                cols.append(c)
            else:
                df_copy.rename(columns={c: f"{prefix}{c}"}, inplace=True)
                cols.append(f"{prefix}{c}")
        return df_copy[cols]

    # Added records
    if len(added_idx) > 0:
        added = df_new_idx.loc[added_idx].reset_index(drop=True)
        added_pref = prefix_df(added, "new_", key_cols)
        added_pref["delta_type"] = "added"
        records.append(added_pref)

    # Removed records
    if len(removed_idx) > 0:
        removed = df_old_idx.loc[removed_idx].reset_index(drop=True)
        removed_pref = prefix_df(removed, "old_", key_cols)
        removed_pref["delta_type"] = "removed"
        records.append(removed_pref)

    # Changed records
    diff_mask_sum = 0
    if len(common_idx) > 0:
        left = df_old_idx.loc[common_idx].sort_index()
        right = df_new_idx.loc[common_idx].sort_index()

        all_cols = list(dict.fromkeys(list(left.columns) + list(right.columns)))
        left = left.reindex(columns=all_cols, fill_value="")
        right = right.reindex(columns=all_cols, fill_value="")

        non_key_cols = [c for c in all_cols if c not in key_cols]
        diff_mask = (left[non_key_cols] != right[non_key_cols]).any(axis=1)
        diff_mask_sum = diff_mask.sum()
        
        if diff_mask.any():
            left_diff = left[diff_mask].reset_index(drop=True)
            right_diff = right[diff_mask].reset_index(drop=True)
            left_pref = prefix_df(left_diff, "old_", key_cols)
            right_pref = prefix_df(right_diff, "new_", key_cols)
            combined = pd.concat([left_pref, right_pref.drop(columns=key_cols, errors="ignore")], axis=1)
            combined["delta_type"] = "changed"
            records.append(combined)

    if not records:
        empty_df = pd.DataFrame(columns=["event_id", "delta_type"])
        return {
            "delta_df": empty_df,
            "summary": {"added": 0, "removed": 0, "changed": 0, "total": 0},
            "csv_path": None
        }

    # Combine and reorder
    df_all = pd.concat(records, ignore_index=True, sort=False)
    df_all = df_all.fillna("")
    
    rest_cols = [c for c in df_all.columns if c not in key_cols + ["delta_type"]]
    ordered = list(key_cols) + ["delta_type"] + rest_cols
    ordered = [c for c in ordered if c in df_all.columns]
    df_all = df_all[ordered]

    # ✅ FIX: Forziamo la conversione in int (Python native) per evitare errori di serializzazione JSON
    summary = {
        "added": int(len(added_idx)),
        "removed": int(len(removed_idx)),
        "changed": int(diff_mask_sum),
        "total": int(len(df_all))
    }

    result = {
        "delta_df": df_all,
        "summary": summary,
        "csv_path": None
    }

    # Save to file if path provided
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_all.to_csv(output_path, index=False)
        result["csv_path"] = str(output_path)

    return result