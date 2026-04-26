#!/usr/bin/env python3
"""
Recall Metrics Dashboard Generator
===================================
Generates a self-contained interactive HTML dashboard from a GCS experiment path.

Usage:
  python recall_dashboard_gen.py gs://bucket/path/to/experiment
  python recall_dashboard_gen.py gs://bucket/path/to/experiment --output ./reports/my_dashboard.html
  python recall_dashboard_gen.py gs://bucket/path/to/experiment --subfolder impacted
  python recall_dashboard_gen.py gs://bucket/path/to/experiment --local-parquet ./cached.parquet

The script:
  1. Downloads qip_scores.parquet from the GCS path (tries impacted/ first, then root).
  2. Auto-detects control vs variant engine names.
  3. Filters to impacted=True rows.
  4. Runs paired t-tests per label (1★–4★).
  5. Categorises queries: identical / diff-same-labels / significant-change.
  6. Analyses EM Classifier precision & recall using the stack column:
     - Precision issues: non-4 items new to variant recall that entered primary stack (stack 1)
     - Recall issues: label-4 items new to variant recall blocked from primary stack
  7. Generates a single self-contained HTML dashboard with:
     - Statistical significance table
     - Query distribution breakdown
     - Per-label significant-change tables (top driving + counteracting tabs)
     - Per-query item grid grouped by label section (Good/Bad/Concern signal)
     - EM Classifier Analysis tab (precision failures + recall failures)
     - Dual Preso deep-links on every item card (control vs variant side-by-side)
"""

import argparse
import sys
import subprocess
import re
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
from scipy import stats


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Generate Recall Metrics Dashboard from GCS path")
    p.add_argument("gcs_path", help="GCS bucket path, e.g. gs://bucket/path/1775127360")
    p.add_argument("--output", default=None,
                   help="Output HTML path (default: reports/{experiment_id}_recall_dashboard.html)")
    p.add_argument("--subfolder", default=None,
                   help="Subfolder within gcs_path to look for parquet, e.g. 'impacted' or 'sample-1000'."
                        " If omitted the script tries impacted/ then sample-1000/ then root.")
    p.add_argument("--local-parquet", default=None,
                   help="Skip download and use a locally cached parquet file.")
    p.add_argument("--cache-dir", default="temp/downloaded_files",
                   help="Directory to cache downloaded parquet files (default: temp/downloaded_files)")
    return p.parse_args()


# ─── GCS helpers ──────────────────────────────────────────────────────────────

def gcs_exists(gcs_uri: str) -> bool:
    r = subprocess.run(["gsutil", "ls", gcs_uri], capture_output=True)
    return r.returncode == 0


def gcs_download(gcs_uri: str, local_path: str):
    """Download a single file from GCS using single-process mode (macOS-safe)."""
    print(f"  Downloading: {gcs_uri} → {local_path}")
    r = subprocess.run(
        ["gsutil", "-o", "GSUtil:parallel_process_count=1", "cp", gcs_uri, local_path],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        raise RuntimeError(f"gsutil failed:\n{r.stderr}")
    size_mb = Path(local_path).stat().st_size / 1024 / 1024
    print(f"  ✅ Downloaded {size_mb:.1f} MB")


def resolve_parquet(gcs_path: str, subfolder: str | None, cache_dir: str, experiment_id: str) -> tuple[str, str]:
    """
    Find and download qip_scores.parquet. Tries (in order):
      1. --subfolder argument if given
      2. impacted/
      3. sample-1000/
      4. root of gcs_path
    Returns (local file path, resolved subfolder string).
    """
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    local_path = str(Path(cache_dir) / f"{experiment_id}_qip_scores.parquet")

    if Path(local_path).exists():
        size_mb = Path(local_path).stat().st_size / 1024 / 1024
        print(f"  Using cached parquet: {local_path} ({size_mb:.1f} MB)")
        # Best-effort: figure out which subfolder it came from
        for sf in (subfolder, "impacted", "sample-1000", ""):
            if sf is None: continue
            uri = f"{gcs_path.rstrip('/')}/{sf}/qip_scores.parquet" if sf else f"{gcs_path.rstrip('/')}/qip_scores.parquet"
            if gcs_exists(uri):
                return local_path, sf
        return local_path, subfolder or "impacted"

    candidates = []
    if subfolder:
        candidates = [(subfolder, f"{gcs_path.rstrip('/')}/{subfolder}/qip_scores.parquet")]
    else:
        candidates = [
            ("impacted",     f"{gcs_path.rstrip('/')}/impacted/qip_scores.parquet"),
            ("sample-1000",  f"{gcs_path.rstrip('/')}/sample-1000/qip_scores.parquet"),
            ("",             f"{gcs_path.rstrip('/')}/qip_scores.parquet"),
        ]

    for sf, uri in candidates:
        if gcs_exists(uri):
            gcs_download(uri, local_path)
            return local_path, sf

    raise FileNotFoundError(
        f"Could not find qip_scores.parquet in any of:\n" +
        "\n".join(f"  {uri}" for _, uri in candidates)
    )


def extract_ptss_trsp(gcs_path: str, subfolder: str) -> dict:
    """
    Read the first line of combined-polaris_crawl.jsonl from GCS and extract
    ptss / trsp for each non-control engine. Returns dict keyed by engine name.
    """
    from urllib.parse import urlparse, parse_qs, unquote as _unquote
    sf = subfolder.strip("/")
    jsonl_uri = f"{gcs_path.rstrip('/')}/{sf}/combined-polaris_crawl.jsonl" if sf \
                else f"{gcs_path.rstrip('/')}/combined-polaris_crawl.jsonl"

    print(f"  Reading config from: {jsonl_uri}")
    r = subprocess.run(
        ["gsutil", "cat", jsonl_uri],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        print(f"  ⚠️  Could not read JSONL ({r.stderr.strip()}) — dual preso links disabled")
        return {}

    try:
        import json as _json
        first_line = r.stdout.splitlines()[0]
        data = _json.loads(first_line)
        engines_data = data["contextualQueriesWithResponses"][0]["engines"]
        result = {}
        for engine_name, engine_info in engines_data.items():
            if engine_name == "control":
                continue
            url = engine_info.get("url", "")
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            raw_ptss = _unquote(params.get("ptss", [""])[0])
            trsp     = _unquote(params.get("trsp", [""])[0])
            # Strip api_rerank_v2:off from ptss
            clean_ptss = ";".join(p for p in raw_ptss.split(";") if p and "api_rerank_v2" not in p)
            result[engine_name] = {"ptss": clean_ptss, "trsp": trsp}
            print(f"  {engine_name}: ptss={clean_ptss!r}  trsp={trsp!r}")
        return result
    except Exception as e:
        print(f"  ⚠️  Failed to parse JSONL config: {e} — dual preso links disabled")
        return {}


# ─── Query clean helper ────────────────────────────────────────────────────────

def clean_query(q: str) -> str:
    return re.sub(r'\s*\((?:facet|stores|zipcode).*?\)', '', str(q)).strip()


# ─── Main pipeline ─────────────────────────────────────────────────────────────

def build_dashboard(parquet_path: str, output_path: str, experiment_id: str, gcs_path: str = "", resolved_subfolder: str = ""):

    # ── Load ──────────────────────────────────────────────────────────────────
    print(f"\nLoading parquet: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    print(f"  {len(df):,} rows × {df.shape[1]} columns, {df['contextualQuery'].nunique()} queries")

    # ── Filter impacted ────────────────────────────────────────────────────────
    if 'impacted' in df.columns:
        df_imp = df[df['impacted'] == True].copy()
        print(f"  Filtered to impacted=True: {len(df_imp):,} rows")
    else:
        print("  ⚠️  No 'impacted' column — using all rows")
        df_imp = df.copy()

    # ── Auto-detect engines ────────────────────────────────────────────────────
    engines = df_imp['engine'].dropna().unique().tolist()
    if 'control' in engines:
        CONTROL_ENGINE = 'control'
        VARIANT_ENGINE = next(e for e in engines if e != 'control')
    else:
        CONTROL_ENGINE, VARIANT_ENGINE = engines[0], engines[1]
    print(f"  Control: {CONTROL_ENGINE}  |  Variant: {VARIANT_ENGINE}")

    # ── Extract ptss / trsp from JSONL config ──────────────────────────────────
    ptss_trsp_config = {}
    if gcs_path and resolved_subfolder is not None:
        print("\nExtracting ptss/trsp from JSONL config...")
        ptss_trsp_config = extract_ptss_trsp(gcs_path, resolved_subfolder)
    variant_ptss = ptss_trsp_config.get(VARIANT_ENGINE, {}).get("ptss", "")
    variant_trsp = ptss_trsp_config.get(VARIANT_ENGINE, {}).get("trsp", "")
    has_dual_preso = bool(
        all(c in df_imp.columns for c in ['stores', 'state', 'zipcode']) and variant_ptss
    )

    # ── Common queries ─────────────────────────────────────────────────────────
    df_imp['cleanQuery'] = df_imp['contextualQuery'].apply(clean_query)
    q_ctrl = set(df_imp[df_imp['engine'] == CONTROL_ENGINE]['contextualQuery'].unique())
    q_var  = set(df_imp[df_imp['engine'] == VARIANT_ENGINE]['contextualQuery'].unique())
    common = q_ctrl & q_var
    print(f"  Queries in both engines: {len(common)}")
    df_c = df_imp[df_imp['contextualQuery'].isin(common)].copy()

    # ── Stratum map (head / torso / tail) ────────────────────────────────────
    stratum_map = {}
    if 'stratum' in df_c.columns:
        stratum_map = (
            df_c.groupby('contextualQuery')['stratum']
            .first()
            .str.lower()
            .to_dict()
        )
        print(f"  Stratum distribution: { {k: sum(1 for v in stratum_map.values() if v==k) for k in ['head','torso','tail']} }")
    else:
        print("  ⚠️  No 'stratum' column found — segment info unavailable")

    # ── Per-query stats ────────────────────────────────────────────────────────
    print("\nComputing per-query statistics...")
    query_stats = {}
    for query in common:
        c_rows = df_c[(df_c['contextualQuery'] == query) & (df_c['engine'] == CONTROL_ENGINE)]
        v_rows = df_c[(df_c['contextualQuery'] == query) & (df_c['engine'] == VARIANT_ENGINE)]
        ctrl_items = set(c_rows['pg_prod_id'].tolist())
        var_items  = set(v_rows['pg_prod_id'].tolist())
        ctrl_lbl = {l: int((c_rows['label'] == l).sum()) for l in [1,2,3,4]}
        var_lbl  = {l: int((v_rows['label']  == l).sum()) for l in [1,2,3,4]}
        same_items = ctrl_items == var_items
        same_dist  = all(ctrl_lbl[l] == var_lbl[l] for l in [1,2,3,4])
        if same_items:      category = "identical"
        elif same_dist:     category = "diff_same_labels"
        else:               category = "significant_change"
        query_stats[query] = {
            "ctrl_labels": ctrl_lbl, "var_labels": var_lbl,
            "category": category, "cleanQuery": clean_query(query),
            "stratum": stratum_map.get(query, "unknown"),
        }

    cat_counts = {k: sum(1 for v in query_stats.values() if v["category"] == k)
                  for k in ["identical", "diff_same_labels", "significant_change"]}
    total_q = len(common)
    for k, v in cat_counts.items():
        print(f"  {k:25s}: {v:4d}  ({v/total_q:.1%})")

    # ── Paired t-tests ─────────────────────────────────────────────────────────
    print("\nRunning paired t-tests...")
    queries_list = sorted(common)
    ttest = {}
    for label in [1,2,3,4]:
        c_arr = np.array([query_stats[q]["ctrl_labels"][label] for q in queries_list], dtype=float)
        v_arr = np.array([query_stats[q]["var_labels"][label]  for q in queries_list], dtype=float)
        _, p = stats.ttest_rel(c_arr, v_arr)
        ttest[label] = {
            "p_value": float(p), "ctrl_mean": float(c_arr.mean()),
            "var_mean": float(v_arr.mean()), "diff": float(v_arr.mean() - c_arr.mean()),
            "significant": bool(p < 0.05),
        }
        sig = "✅" if p < 0.05 else "❌"
        print(f"  Label {label}★  p={p:.6f} {sig}  ctrl={c_arr.mean():.2f}  var={v_arr.mean():.2f}  Δ={v_arr.mean()-c_arr.mean():+.2f}")

    sig_labels = [l for l in [1,2,3,4] if ttest[l]["significant"]]

    # ── Total counts per label ─────────────────────────────────────────────────
    label_totals = {
        l: {
            "control": int(df_c[df_c['engine'] == CONTROL_ENGINE]['label'].eq(l).sum()),
            "variant": int(df_c[df_c['engine'] == VARIANT_ENGINE]['label'].eq(l).sum()),
        }
        for l in [1,2,3,4]
    }

    # ── Per-label query tables (all 4 labels) ─────────────────────────────────
    print("\nBuilding query tables...")
    label_query_tables = {}
    for label in [1,2,3,4]:
        rows = []
        for q in queries_list:
            ctrl_cnt = query_stats[q]["ctrl_labels"][label]
            var_cnt  = query_stats[q]["var_labels"][label]
            diff     = var_cnt - ctrl_cnt
            rows.append({"query": query_stats[q]["cleanQuery"], "rawQuery": q,
                         "control": ctrl_cnt, "variant": var_cnt, "difference": diff,
                         "stratum": query_stats[q].get("stratum", "unknown")})
        rows.sort(key=lambda r: r["difference"], reverse=True)
        label_query_tables[label] = rows

    # ── Per-stratum significant-change breakdown ───────────────────────────────
    STRATA = ["head", "torso", "tail"]
    stratum_breakdown = {}
    for seg in STRATA:
        seg_queries = [q for q in queries_list if query_stats[q].get("stratum") == seg]
        seg_sig     = [q for q in seg_queries if query_stats[q]["category"] == "significant_change"]
        seg_ident   = [q for q in seg_queries if query_stats[q]["category"] == "identical"]
        seg_diff    = [q for q in seg_queries if query_stats[q]["category"] == "diff_same_labels"]
        stratum_breakdown[seg] = {
            "total": len(seg_queries),
            "significant_change": len(seg_sig),
            "identical": len(seg_ident),
            "diff_same_labels": len(seg_diff),
        }
        print(f"  {seg:6s}: {len(seg_queries):3d} queries  |  sig_change={len(seg_sig)}  identical={len(seg_ident)}  diff_same={len(seg_diff)}")

    # ── Item data per query ────────────────────────────────────────────────────
    print("Building item grid data...")
    extra_cols = [c for c in ['stores', 'state', 'zipcode'] if c in df_c.columns]
    base_cols  = ['contextualQuery', 'engine', 'id', 'pg_prod_id', 'title', 'image',
                  'l1_category', 'label', 'position', 'cleanQuery'] + extra_cols
    df_c['cleanQuery'] = df_c['contextualQuery'].apply(clean_query)
    df_items = (
        df_c[base_cols].sort_values('position')
        .drop_duplicates(subset=['contextualQuery', 'engine', 'pg_prod_id'])
        .copy()
    )

    def safe(v, cast=str):
        try:
            if pd.isna(v): return ""
        except Exception: pass
        try: return cast(int(v)) if cast == str and isinstance(v, float) else cast(v)
        except Exception: return ""

    query_items = {}
    for query in common:
        q_data = df_items[df_items['contextualQuery'] == query]
        ctrl_pids = set(q_data[q_data['engine'] == CONTROL_ENGINE]['pg_prod_id'])
        var_pids  = set(q_data[q_data['engine'] == VARIANT_ENGINE]['pg_prod_id'])
        items = []
        for _, row in q_data.sort_values(['engine', 'position']).iterrows():
            pid = row['pg_prod_id']
            if row['engine'] == CONTROL_ENGINE and pid in var_pids:  status = "both"
            elif row['engine'] == CONTROL_ENGINE:                     status = "ctrl_only"
            elif row['engine'] == VARIANT_ENGINE and pid not in ctrl_pids: status = "var_only"
            else: continue
            item = {
                "item_id":    safe(row.get('id')),
                "pg_prod_id": safe(row.get('pg_prod_id')),
                "title":      safe(row.get('title')),
                "image":      safe(row.get('image')),
                "l1_category":safe(row.get('l1_category')),
                "label":      int(row['label']) if not pd.isna(row['label']) else 0,
                "position":   int(row['position']) if not pd.isna(row['position']) else 0,
                "engine":     str(row['engine']),
                "status":     status,
            }
            for col in extra_cols:
                val = row.get(col, "")
                item[col] = str(int(val)) if col == 'stores' and not pd.isna(val) else safe(val)
            items.append(item)
        query_items[query] = items

    # ── Serialize ──────────────────────────────────────────────────────────────
    print("Serializing...")
    distribution_data = [
        {"category": "Identical items (same set, just re-ordered)",
         "count": cat_counts["identical"], "pct": round(cat_counts["identical"]/total_q*100, 1)},
        {"category": "Different items (swapped, same label counts)",
         "count": cat_counts["diff_same_labels"], "pct": round(cat_counts["diff_same_labels"]/total_q*100, 1)},
        {"category": "Queries with significant change",
         "count": cat_counts["significant_change"], "pct": round(cat_counts["significant_change"]/total_q*100, 1)},
    ]

    LABEL_COLOR = {1: "#ef4444", 2: "#f97316", 3: "#3b82f6", 4: "#10b981"}
    GOOD_LABEL  = {1: False, 2: False, 3: False, 4: True}

    sig_sections_info = []
    for label in sig_labels:
        t = ttest[label]
        direction_good = (t["diff"] > 0) == GOOD_LABEL[label]
        sig_sections_info.append({
            "label": label, "star": f"{label}★",
            "p_value": t["p_value"], "ctrl_mean": t["ctrl_mean"],
            "var_mean": t["var_mean"], "diff": t["diff"],
            "good": direction_good, "color": LABEL_COLOR[label],
        })

    ttest_json             = json.dumps(ttest)
    label_totals_json      = json.dumps({str(k): v for k, v in label_totals.items()})
    distribution_json      = json.dumps(distribution_data)
    label_tables_json      = json.dumps({str(k): v for k, v in label_query_tables.items()})
    query_items_json       = json.dumps(query_items)
    sig_sections_json      = json.dumps(sig_sections_info)
    stratum_breakdown_json = json.dumps(stratum_breakdown)

    # ── EM Classifier Analysis (stack-based) ─────────────────────────────────
    # Uses impacted-only rows (df_imp) so all analyses are scoped to queries
    # where control and variant diverge — consistent with the recall tab.
    print("\nComputing EM Classifier analysis...")
    has_stack = 'stack' in df_imp.columns
    em_stats = {}
    em_precision_queries = []
    em_precision_items = []
    em_recall_queries = []
    em_recall_items = []
    em_improvement_queries = []  # precision improvements (non-4s removed)
    em_improvement_items = []
    em_recall_improvement_queries = []  # recall improvements (4s added to primary)
    em_recall_improvement_items = []
    thresh_prec_loss_queries = []   # threshold: non-4s promoted (bad)
    thresh_prec_loss_items = []
    thresh_rec_gain_queries = []    # threshold: 4s promoted (good)
    thresh_rec_gain_items = []
    thresh_prec_gain_queries = []   # threshold: non-4s demoted (good)
    thresh_prec_gain_items = []
    thresh_rec_loss_queries = []    # threshold: 4s demoted (bad)
    thresh_rec_loss_items = []

    if has_stack:
        ctrl_all = df_imp[df_imp['engine'] == CONTROL_ENGINE]
        var_all  = df_imp[df_imp['engine'] == VARIANT_ENGINE]

        # Build recall sets (all stacks) and primary stack sets (stack 1)
        ctrl_recall_set  = set(zip(ctrl_all['contextualQuery'], ctrl_all['pg_prod_id']))
        var_recall_set   = set(zip(var_all['contextualQuery'], var_all['pg_prod_id']))
        ctrl_primary     = ctrl_all[ctrl_all['stack'] == 1]
        var_primary      = var_all[var_all['stack'] == 1]
        ctrl_primary_set = set(zip(ctrl_primary['contextualQuery'], ctrl_primary['pg_prod_id']))
        var_primary_set  = set(zip(var_primary['contextualQuery'], var_primary['pg_prod_id']))

        # ── Baseline metrics: primary stack precision & recall per engine ──
        ctrl_primary_total = len(ctrl_primary)
        ctrl_primary_4s    = int((ctrl_primary['label'] == 4).sum())
        ctrl_primary_non4s = ctrl_primary_total - ctrl_primary_4s
        ctrl_recall_4s     = int((ctrl_all['label'] == 4).sum())
        ctrl_primary_prec  = round(ctrl_primary_non4s / ctrl_primary_total * 100, 2) if ctrl_primary_total else 0
        ctrl_primary_rec   = round(ctrl_primary_4s / ctrl_recall_4s * 100, 2) if ctrl_recall_4s else 0

        var_primary_total  = len(var_primary)
        var_primary_4s     = int((var_primary['label'] == 4).sum())
        var_primary_non4s  = var_primary_total - var_primary_4s
        var_recall_4s      = int((var_all['label'] == 4).sum())
        var_primary_prec   = round(var_primary_non4s / var_primary_total * 100, 2) if var_primary_total else 0
        var_primary_rec    = round(var_primary_4s / var_recall_4s * 100, 2) if var_recall_4s else 0

        print(f"  Control primary stack: {ctrl_primary_total:,} items, {ctrl_primary_non4s:,} non-4 ({ctrl_primary_prec}%), recall of 4s: {ctrl_primary_rec}%")
        print(f"  Variant primary stack: {var_primary_total:,} items, {var_primary_non4s:,} non-4 ({var_primary_prec}%), recall of 4s: {var_primary_rec}%")

        # ── Delta analysis: what's NEW between engines ──
        new_to_var_recall  = var_recall_set - ctrl_recall_set   # new items variant retrieved
        lost_from_recall   = ctrl_recall_set - var_recall_set   # items variant dropped

        # Tag keys for efficient lookup
        vp_df = var_primary.copy()
        vp_df['_key'] = list(zip(vp_df['contextualQuery'], vp_df['pg_prod_id']))
        var_all_c = var_all.copy()
        var_all_c['_key'] = list(zip(var_all_c['contextualQuery'], var_all_c['pg_prod_id']))
        ctrl_primary_c = ctrl_primary.copy()
        ctrl_primary_c['_key'] = list(zip(ctrl_primary_c['contextualQuery'], ctrl_primary_c['pg_prod_id']))

        # 1) NEW PRECISION FAILURES: new to recall + in primary stack + label != 4
        new_in_primary = new_to_var_recall & var_primary_set
        pi_df = vp_df[vp_df['_key'].isin(new_in_primary) & (vp_df['label'] != 4)].drop(columns=['_key'])

        # 2) NEW RECALL FAILURES: new to recall + label=4 + NOT in primary stack
        new_recall_4s = var_all_c[var_all_c['_key'].isin(new_to_var_recall) & (var_all_c['label'] == 4)]
        ri_keys = set(zip(new_recall_4s['contextualQuery'], new_recall_4s['pg_prod_id'])) - var_primary_set
        ri_df = new_recall_4s[new_recall_4s['_key'].isin(ri_keys)].drop(columns=['_key'])

        # 3) NEW PRECISION IMPROVEMENTS: non-4s that were in control recall (and
        #    by extension primary stack) but removed from variant recall entirely
        pimprv_df = ctrl_primary_c[
            ctrl_primary_c['_key'].isin(lost_from_recall) & (ctrl_primary_c['label'] != 4)
        ].drop(columns=['_key'])

        # 4) NEW RECALL IMPROVEMENTS: 4s new to variant recall AND in primary stack
        new_4s_in_primary = new_to_var_recall & var_primary_set
        rimprv_df = vp_df[vp_df['_key'].isin(new_4s_in_primary) & (vp_df['label'] == 4)].drop(columns=['_key'])

        print(f"  New precision failures  (non-4 new→primary):       {len(pi_df):,} across {pi_df['contextualQuery'].nunique()} queries")
        print(f"  New precision improvements (non-4 ctrl→removed):   {len(pimprv_df):,} across {pimprv_df['contextualQuery'].nunique()} queries")
        print(f"  New recall failures (4s new→blocked):              {len(ri_df):,} across {ri_df['contextualQuery'].nunique()} queries")
        print(f"  New recall improvements (4s new→primary):          {len(rimprv_df):,} across {rimprv_df['contextualQuery'].nunique()} queries")

        # ── 5) STABLE RECALL: EM threshold effects on items in BOTH recall sets ──
        # Items that exist in both control and variant recall but changed stack
        stable_recall = ctrl_recall_set & var_recall_set

        # Also tag ctrl_all for lookup
        ctrl_all_c = ctrl_all.copy()
        ctrl_all_c['_key'] = list(zip(ctrl_all_c['contextualQuery'], ctrl_all_c['pg_prod_id']))

        # Stable items: promoted to stack 1 (not in ctrl primary, IS in var primary)
        stable_promoted = (stable_recall - ctrl_primary_set) & var_primary_set
        # Stable items: demoted from stack 1 (in ctrl primary, NOT in var primary)
        stable_demoted = (stable_recall & ctrl_primary_set) - var_primary_set

        # 5a) Threshold precision loss: stable + promoted + label != 4
        thresh_prec_loss_df = vp_df[vp_df['_key'].isin(stable_promoted) & (vp_df['label'] != 4)].drop(columns=['_key'])
        # 5b) Threshold recall gain: stable + promoted + label == 4
        thresh_rec_gain_df = vp_df[vp_df['_key'].isin(stable_promoted) & (vp_df['label'] == 4)].drop(columns=['_key'])
        # 5c) Threshold precision gain: stable + demoted + label != 4
        thresh_prec_gain_df = ctrl_primary_c[ctrl_primary_c['_key'].isin(stable_demoted) & (ctrl_primary_c['label'] != 4)].drop(columns=['_key'])
        # 5d) Threshold recall loss: stable + demoted + label == 4
        thresh_rec_loss_df = ctrl_primary_c[ctrl_primary_c['_key'].isin(stable_demoted) & (ctrl_primary_c['label'] == 4)].drop(columns=['_key'])

        thresh_net_prec = len(thresh_prec_gain_df) - len(thresh_prec_loss_df)
        thresh_net_rec  = len(thresh_rec_gain_df)  - len(thresh_rec_loss_df)

        print(f"\n  Stable recall (threshold effects):")
        print(f"    Items in both recall sets:      {len(stable_recall):,}")
        print(f"    Promoted to primary (non-4):    {len(thresh_prec_loss_df):,}  ← precision loss")
        print(f"    Promoted to primary (4s):       {len(thresh_rec_gain_df):,}  ← recall gain")
        print(f"    Demoted from primary (non-4):   {len(thresh_prec_gain_df):,}  ← precision gain")
        print(f"    Demoted from primary (4s):      {len(thresh_rec_loss_df):,}  ← recall loss")
        print(f"    Net threshold precision: {thresh_net_prec:+,}  |  Net threshold recall: {thresh_net_rec:+,}")

        # ── Net metrics ──
        net_precision = len(pimprv_df) - len(pi_df)   # positive = better
        net_recall    = len(rimprv_df) - len(ri_df)    # positive = better

        em_stats = {
            # Baseline comparison
            "ctrl_primary_total": ctrl_primary_total,
            "ctrl_primary_non4": ctrl_primary_non4s,
            "ctrl_primary_prec": ctrl_primary_prec,
            "ctrl_primary_4s": ctrl_primary_4s,
            "ctrl_recall_4s": ctrl_recall_4s,
            "ctrl_primary_rec": ctrl_primary_rec,
            "var_primary_total": var_primary_total,
            "var_primary_non4": var_primary_non4s,
            "var_primary_prec": var_primary_prec,
            "var_primary_4s": var_primary_4s,
            "var_recall_4s": var_recall_4s,
            "var_primary_rec": var_primary_rec,
            # New failures
            "precision_total": int(len(pi_df)),
            "precision_queries": int(pi_df['contextualQuery'].nunique()),
            "precision_label_1": int((pi_df['label'] == 1).sum()),
            "precision_label_2": int((pi_df['label'] == 2).sum()),
            "precision_label_3": int((pi_df['label'] == 3).sum()),
            "precision_in_top20": int((pi_df['position'] <= 20).sum()),
            "precision_in_top10": int((pi_df['position'] <= 10).sum()),
            "precision_mean_pos": round(float(pi_df['position'].mean()), 1) if len(pi_df) else 0,
            "recall_total": int(len(ri_df)),
            "recall_queries": int(ri_df['contextualQuery'].nunique()),
            # New improvements
            "prec_improvement_total": int(len(pimprv_df)),
            "prec_improvement_queries": int(pimprv_df['contextualQuery'].nunique()),
            "prec_improvement_label_1": int((pimprv_df['label'] == 1).sum()),
            "prec_improvement_label_2": int((pimprv_df['label'] == 2).sum()),
            "prec_improvement_label_3": int((pimprv_df['label'] == 3).sum()),
            "recall_improvement_total": int(len(rimprv_df)),
            "recall_improvement_queries": int(rimprv_df['contextualQuery'].nunique()),
            # Net
            "net_precision": net_precision,
            "net_recall": net_recall,
            "new_to_recall_total": int(len(new_to_var_recall)),
            "lost_from_recall_total": int(len(lost_from_recall)),
            # Threshold effects (stable recall)
            "stable_recall_total": int(len(stable_recall)),
            "thresh_prec_loss": int(len(thresh_prec_loss_df)),
            "thresh_prec_loss_queries": int(thresh_prec_loss_df['contextualQuery'].nunique()) if len(thresh_prec_loss_df) else 0,
            "thresh_prec_loss_l1": int((thresh_prec_loss_df['label'] == 1).sum()) if len(thresh_prec_loss_df) else 0,
            "thresh_prec_loss_l2": int((thresh_prec_loss_df['label'] == 2).sum()) if len(thresh_prec_loss_df) else 0,
            "thresh_prec_loss_l3": int((thresh_prec_loss_df['label'] == 3).sum()) if len(thresh_prec_loss_df) else 0,
            "thresh_rec_gain": int(len(thresh_rec_gain_df)),
            "thresh_rec_gain_queries": int(thresh_rec_gain_df['contextualQuery'].nunique()) if len(thresh_rec_gain_df) else 0,
            "thresh_prec_gain": int(len(thresh_prec_gain_df)),
            "thresh_prec_gain_queries": int(thresh_prec_gain_df['contextualQuery'].nunique()) if len(thresh_prec_gain_df) else 0,
            "thresh_rec_loss": int(len(thresh_rec_loss_df)),
            "thresh_rec_loss_queries": int(thresh_rec_loss_df['contextualQuery'].nunique()) if len(thresh_rec_loss_df) else 0,
            "thresh_net_prec": thresh_net_prec,
            "thresh_net_rec": thresh_net_rec,
        }

        # ── Aggregate per-query tables ──
        def _agg_queries(src_df, top_n=300, include_labels=True):
            """Aggregate query-level stats for a dataframe."""
            if include_labels:
                result = src_df.groupby('contextualQuery').agg(
                    count=('pg_prod_id', 'count'),
                    label_1=('label', lambda x: int((x == 1).sum())),
                    label_2=('label', lambda x: int((x == 2).sum())),
                    label_3=('label', lambda x: int((x == 3).sum())),
                    avg_pos=('position', 'mean'),
                    top20=('position', lambda x: int((x <= 20).sum())),
                ).sort_values('count', ascending=False).head(top_n)
            else:
                result = src_df.groupby('contextualQuery').agg(
                    count=('pg_prod_id', 'count'),
                    avg_pos=('position', 'mean'),
                ).sort_values('count', ascending=False).head(top_n)
            rows = []
            for q, row in result.iterrows():
                r = {"query": q, "cleanQuery": clean_query(q),
                     "count": int(row['count']),
                     "avg_pos": round(row['avg_pos'], 1),
                     "stratum": stratum_map.get(q, "unknown")}
                if include_labels:
                    r.update({"label_1": int(row['label_1']), "label_2": int(row['label_2']),
                              "label_3": int(row['label_3']), "top20": int(row['top20'])})
                rows.append(r)
            return rows

        em_precision_queries     = _agg_queries(pi_df, include_labels=True)
        em_recall_queries        = _agg_queries(ri_df, include_labels=False)
        em_improvement_queries   = _agg_queries(pimprv_df, include_labels=True)
        em_recall_improvement_queries = _agg_queries(rimprv_df, include_labels=False)

        # Threshold effect queries
        thresh_prec_loss_queries  = _agg_queries(thresh_prec_loss_df, include_labels=True) if len(thresh_prec_loss_df) else []
        thresh_rec_gain_queries   = _agg_queries(thresh_rec_gain_df, include_labels=False) if len(thresh_rec_gain_df) else []
        thresh_prec_gain_queries  = _agg_queries(thresh_prec_gain_df, include_labels=True) if len(thresh_prec_gain_df) else []
        thresh_rec_loss_queries   = _agg_queries(thresh_rec_loss_df, include_labels=False) if len(thresh_rec_loss_df) else []

        # ── Item-level data (top 200 queries per section) ──
        def _collect_items(src_df, query_list, max_queries=200, include_stack=False):
            top_qs = set(r['query'] for r in query_list[:max_queries])
            icols = ['contextualQuery', 'pg_prod_id', 'label', 'position', 'title',
                     'image', 'l1_category', 'id']
            if include_stack:
                icols.append('stack')
            icols = [c for c in icols if c in src_df.columns]
            items = []
            for _, row in src_df[src_df['contextualQuery'].isin(top_qs)][icols].iterrows():
                item = {
                    "query": str(row['contextualQuery']),
                    "pg_prod_id": safe(row.get('pg_prod_id')),
                    "label": int(row['label']) if not pd.isna(row['label']) else 0,
                    "position": int(row['position']) if not pd.isna(row['position']) else 0,
                    "title": safe(row.get('title')),
                    "image": safe(row.get('image')),
                    "l1_category": safe(row.get('l1_category')),
                }
                if include_stack and 'stack' in row.index:
                    item["stack"] = int(row['stack']) if not pd.isna(row['stack']) else 0
                items.append(item)
            return items

        em_precision_items    = _collect_items(pi_df, em_precision_queries)
        em_recall_items       = _collect_items(ri_df, em_recall_queries, include_stack=True)
        em_improvement_items  = _collect_items(pimprv_df, em_improvement_queries)
        em_recall_improvement_items = _collect_items(rimprv_df, em_recall_improvement_queries)

        # Threshold effect items
        thresh_prec_loss_items = _collect_items(thresh_prec_loss_df, thresh_prec_loss_queries, include_stack=True) if len(thresh_prec_loss_df) else []
        thresh_rec_gain_items  = _collect_items(thresh_rec_gain_df, thresh_rec_gain_queries, include_stack=True) if len(thresh_rec_gain_df) else []
        thresh_prec_gain_items = _collect_items(thresh_prec_gain_df, thresh_prec_gain_queries, include_stack=True) if len(thresh_prec_gain_df) else []
        thresh_rec_loss_items  = _collect_items(thresh_rec_loss_df, thresh_rec_loss_queries, include_stack=True) if len(thresh_rec_loss_df) else []

        print(f"  Serialized items: prec_fail={len(em_precision_items):,} prec_imprv={len(em_improvement_items):,} "
              f"recall_fail={len(em_recall_items):,} recall_imprv={len(em_recall_improvement_items):,}")
        print(f"  Threshold items: prec_loss={len(thresh_prec_loss_items):,} rec_gain={len(thresh_rec_gain_items):,} "
              f"prec_gain={len(thresh_prec_gain_items):,} rec_loss={len(thresh_rec_loss_items):,}")

        # Category breakdown
        if 'l1_category' in pi_df.columns:
            em_stats["precision_by_category"] = pi_df['l1_category'].value_counts().head(10).to_dict()
        if 'recall_strategy' in pi_df.columns:
            em_stats["precision_by_strategy"] = pi_df['recall_strategy'].value_counts().to_dict()
    else:
        print("  ⚠️  No 'stack' column — EM Classifier tab will be disabled")

    em_stats_json                      = json.dumps(em_stats)
    em_precision_queries_json          = json.dumps(em_precision_queries)
    em_precision_items_json            = json.dumps(em_precision_items)
    em_recall_queries_json             = json.dumps(em_recall_queries)
    em_recall_items_json               = json.dumps(em_recall_items)
    em_improvement_queries_json        = json.dumps(em_improvement_queries)
    em_improvement_items_json          = json.dumps(em_improvement_items)
    em_recall_improvement_queries_json = json.dumps(em_recall_improvement_queries)
    em_recall_improvement_items_json   = json.dumps(em_recall_improvement_items)
    thresh_prec_loss_queries_json  = json.dumps(thresh_prec_loss_queries)
    thresh_prec_loss_items_json    = json.dumps(thresh_prec_loss_items)
    thresh_rec_gain_queries_json   = json.dumps(thresh_rec_gain_queries)
    thresh_rec_gain_items_json     = json.dumps(thresh_rec_gain_items)
    thresh_prec_gain_queries_json  = json.dumps(thresh_prec_gain_queries)
    thresh_prec_gain_items_json    = json.dumps(thresh_prec_gain_items)
    thresh_rec_loss_queries_json   = json.dumps(thresh_rec_loss_queries)
    thresh_rec_loss_items_json     = json.dumps(thresh_rec_loss_items)

    # ── HTML ───────────────────────────────────────────────────────────────────
    print("Generating HTML...")
    sig_label_list = ', '.join(f'{l}★' for l in sig_labels) if sig_labels else 'None'
    em_tab_label = (
        f'🔬 EM Classifier Analysis <span class="tab-badge">{em_stats.get("precision_total", 0):,} issues</span>'
        if has_stack else '🔬 EM Classifier (N/A)'
    )
    em_no_stack_msg = '<div class="card"><h2 style="color:#9ca3af">⚠️ No stack column in data — EM Classifier analysis unavailable</h2></div>' if not has_stack else ''

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Recall Dashboard — {VARIANT_ENGINE} vs {CONTROL_ENGINE}</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
         background: #f0f2f5; color: #1a202c; padding: 24px; }}
  .container {{ max-width: 1400px; margin: 0 auto; }}

  /* Header */
  .header {{ background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%); color: white;
             padding: 32px 36px; border-radius: 16px; margin-bottom: 28px;
             box-shadow: 0 8px 24px rgba(59,130,246,0.3); }}
  .header h1 {{ font-size: 28px; font-weight: 700; margin-bottom: 10px; }}
  .header .meta {{ opacity: .9; font-size: 15px; line-height: 1.8; }}
  .engine-badge {{ display:inline-block; background:rgba(255,255,255,.2);
                   border:1px solid rgba(255,255,255,.4); border-radius:20px;
                   padding:3px 12px; font-weight:600; font-size:13px; margin-left:4px; }}

  /* Cards */
  .card {{ background:white; border-radius:12px; padding:24px 28px; margin-bottom:24px;
           box-shadow:0 2px 8px rgba(0,0,0,.06); }}
  .card h2 {{ font-size:18px; font-weight:700; color:#1e3a8a;
              border-bottom:3px solid #3b82f6; padding-bottom:10px; margin-bottom:20px; }}

  /* Stat boxes */
  .stats-row {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
                gap:16px; margin-bottom:24px; }}
  .stat-box {{ background:#eff6ff; border:1px solid #bfdbfe; border-left:5px solid #3b82f6;
               border-radius:10px; padding:16px 20px; }}
  .stat-box .lbl {{ font-size:11px; text-transform:uppercase; color:#6b7280; letter-spacing:.5px; }}
  .stat-box .val {{ font-size:32px; font-weight:700; color:#1e3a8a; margin-top:4px; }}

  /* Tables */
  table {{ width:100%; border-collapse:collapse; font-size:14px; }}
  thead th {{ background:#1e3a8a; color:white; padding:12px 14px; text-align:left; font-weight:600; }}
  tbody tr {{ border-bottom:1px solid #e5e7eb; transition:background .15s; }}
  tbody tr:last-child {{ border-bottom:none; }}
  tbody tr:hover {{ background:#eff6ff; cursor:pointer; }}
  tbody td {{ padding:11px 14px; }}
  .diff-pos {{ color:#10b981; font-weight:600; }}
  .diff-neg {{ color:#ef4444; font-weight:600; }}
  .diff-zero {{ color:#9ca3af; }}

  /* Distribution bar */
  .pct-bar-wrap {{ background:#e5e7eb; border-radius:4px; height:10px; overflow:hidden; }}
  .pct-bar {{ background:#3b82f6; height:100%; border-radius:4px; }}

  /* Significant sections */
  .sig-section {{ margin-bottom:28px; }}
  .sig-label-header {{ display:flex; align-items:center; gap:12px; margin-bottom:12px; }}
  .sig-star {{ width:40px;height:40px;border-radius:50%;display:flex;align-items:center;
               justify-content:center;font-size:16px;font-weight:700;color:white; }}
  .sig-info {{ font-size:13px; color:#6b7280; }}
  .sig-tabs {{ display:flex; gap:8px; margin-bottom:12px; }}
  .sig-tab {{ padding:6px 16px; border-radius:6px; border:1px solid #d1d5db; cursor:pointer;
              font-size:13px; background:white; color:#374151; transition:all .15s; }}
  .sig-tab.active {{ background:#1e3a8a; color:white; border-color:#1e3a8a; }}
  .query-table-wrap {{ max-height:320px; overflow-y:auto; border-radius:8px; border:1px solid #e5e7eb; }}

  /* Item panel */
  .item-panel {{ background:#f8faff; border:1px solid #bfdbfe; border-radius:12px;
                 padding:20px 24px; margin-top:24px; display:none; }}
  .item-panel.visible {{ display:block; }}
  .item-panel h3 {{ font-size:16px; font-weight:700; color:#1e3a8a; margin-bottom:4px; }}
  .item-panel .sub {{ font-size:13px; color:#6b7280; margin-bottom:16px; min-height:20px; }}
  .item-legend {{ display:flex; gap:16px; flex-wrap:wrap; margin-bottom:14px; font-size:12px; }}
  .legend-item {{ display:flex; align-items:center; gap:6px; }}
  .legend-dot {{ width:12px; height:12px; border-radius:3px; }}

  /* Label sections */
  .label-section {{ margin-bottom:28px; }}
  .label-section-title {{ font-size:14px; font-weight:700; color:#374151;
                          text-transform:uppercase; letter-spacing:.5px;
                          margin-bottom:10px; padding-bottom:6px; border-bottom:2px solid #e5e7eb; }}
  .label-section-cols {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
  .label-col {{ display:flex; flex-direction:column; }}
  .label-col-header {{ padding:8px 14px; border-radius:8px 8px 0 0; font-size:13px;
                       font-weight:700; border:2px solid transparent; border-bottom:none; }}
  .label-col-header.good    {{ background:#d1fae5; color:#065f46; border-color:#6ee7b7; }}
  .label-col-header.bad     {{ background:#fee2e2; color:#991b1b; border-color:#fca5a5; }}
  .label-col-header.concern {{ background:#fef3c7; color:#92400e; border-color:#fcd34d; }}
  .label-col-items {{ border:2px solid #e5e7eb; border-radius:0 0 8px 8px; min-height:80px;
                      max-height:520px; overflow-y:auto; display:grid;
                      grid-template-columns:repeat(auto-fill,minmax(220px,1fr));
                      gap:10px; padding:10px; background:#f9fafb; }}
  .label-col-items.good-border    {{ border-color:#6ee7b7; }}
  .label-col-items.bad-border     {{ border-color:#fca5a5; }}
  .label-col-items.concern-border {{ border-color:#fcd34d; }}
  .label-col-empty {{ grid-column:1/-1; text-align:center; color:#9ca3af;
                      font-size:13px; padding:20px; font-style:italic; }}

  /* Item cards */
  .item-card {{ background:white; border-radius:10px; padding:14px; border:2px solid #e5e7eb;
                transition:all .2s; position:relative; }}
  .item-card:hover {{ box-shadow:0 4px 16px rgba(0,0,0,.1); transform:translateY(-2px); }}
  .item-img {{ width:100%; height:120px; object-fit:contain; border-radius:6px;
               background:#f3f4f6; margin-bottom:10px; }}
  .item-img-placeholder {{ width:100%; height:120px; border-radius:6px; background:#f3f4f6;
                            display:flex; align-items:center; justify-content:center;
                            color:#9ca3af; font-size:12px; margin-bottom:10px; }}
  .item-id {{ font-family:monospace; font-size:11px; color:#3b82f6; margin-bottom:5px; }}
  .item-title {{ font-size:12px; line-height:1.4; color:#1a202c; margin-bottom:8px; }}
  .item-title .hl {{ background:#fef08a; border-radius:2px; padding:0 2px; font-weight:600; }}
  .item-meta {{ font-size:11px; color:#6b7280; line-height:1.6; }}
  .item-label {{ display:inline-block; padding:1px 7px; border-radius:4px;
                 font-size:11px; font-weight:700; color:white; margin-top:4px; }}
  .lbl-1 {{ background:#ef4444; }} .lbl-2 {{ background:#f97316; }}
  .lbl-3 {{ background:#3b82f6; }} .lbl-4 {{ background:#10b981; }}

  tbody tr.active-row {{ background:#dbeafe !important; }}
  .close-btn {{ float:right; background:none; border:none; font-size:20px;
                cursor:pointer; color:#6b7280; line-height:1; }}
  .close-btn:hover {{ color:#1a202c; }}

  /* Stratum badges */
  .stratum-badge {{ display:inline-block; padding:2px 8px; border-radius:10px;
                    font-size:11px; font-weight:700; letter-spacing:.3px;
                    text-transform:uppercase; white-space:nowrap; }}
  .stratum-head  {{ background:#dbeafe; color:#1e40af; border:1px solid #93c5fd; }}
  .stratum-torso {{ background:#fef3c7; color:#92400e; border:1px solid #fcd34d; }}
  .stratum-tail  {{ background:#f3f4f6; color:#4b5563; border:1px solid #d1d5db; }}
  .stratum-unknown {{ background:#f3f4f6; color:#9ca3af; border:1px solid #e5e7eb; }}

  /* Segment filter bar */
  .seg-filter-bar {{ display:flex; gap:8px; margin-bottom:12px; align-items:center; }}
  .seg-filter-bar span {{ font-size:12px; color:#6b7280; font-weight:600; }}
  .seg-btn {{ padding:4px 14px; border-radius:20px; border:1px solid #d1d5db;
              cursor:pointer; font-size:12px; font-weight:600; background:white;
              color:#374151; transition:all .15s; }}
  .seg-btn.active {{ color:white; border-color:transparent; }}
  .seg-btn[data-seg="all"].active   {{ background:#1e3a8a; }}
  .seg-btn[data-seg="head"].active  {{ background:#1e40af; }}
  .seg-btn[data-seg="torso"].active {{ background:#b45309; }}
  .seg-btn[data-seg="tail"].active  {{ background:#4b5563; }}

  /* Segment breakdown card */
  .seg-grid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:16px; margin-top:4px; }}
  .seg-card {{ border-radius:10px; padding:16px 18px; border:2px solid; }}
  .seg-card.head  {{ background:#eff6ff; border-color:#93c5fd; }}
  .seg-card.torso {{ background:#fffbeb; border-color:#fcd34d; }}
  .seg-card.tail  {{ background:#f9fafb; border-color:#d1d5db; }}
  .seg-card-title {{ font-size:13px; font-weight:700; text-transform:uppercase;
                     letter-spacing:.5px; margin-bottom:10px; }}
  .seg-card.head  .seg-card-title {{ color:#1e40af; }}
  .seg-card.torso .seg-card-title {{ color:#92400e; }}
  .seg-card.tail  .seg-card-title {{ color:#4b5563; }}
  .seg-stat-row {{ display:flex; justify-content:space-between; font-size:13px;
                   padding:3px 0; border-bottom:1px solid rgba(0,0,0,.06); }}
  .seg-stat-row:last-child {{ border-bottom:none; }}
  .seg-stat-val {{ font-weight:700; color:#1a202c; }}

  /* Top-level tabs */
  .top-tabs {{ display:flex; gap:4px; margin-bottom:24px; background:#e2e8f0;
               border-radius:12px; padding:4px; }}
  .top-tab {{ flex:1; padding:14px 20px; border:none; background:transparent;
              color:#64748b; font-size:15px; font-weight:600; cursor:pointer;
              border-radius:8px; transition:all .2s; text-align:center; }}
  .top-tab:hover {{ background:#cbd5e1; color:#1e293b; }}
  .top-tab.active {{ background:#1e3a8a; color:white; box-shadow:0 2px 8px rgba(30,58,138,.3); }}
  .top-tab.active-red {{ background:#dc2626; color:white; box-shadow:0 2px 8px rgba(220,38,38,.3); }}
  .top-tab-content {{ display:none; }}
  .top-tab-content.active {{ display:block; }}
  .top-tab .tab-badge {{ display:inline-block; background:rgba(255,255,255,.25);
                         padding:1px 8px; border-radius:10px; font-size:12px; margin-left:6px; }}

  /* EM Classifier styles */
  .em-stats-row {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr));
                   gap:12px; margin-bottom:20px; }}
  .em-stat {{ background:white; border-radius:10px; padding:16px 18px;
              border-left:4px solid #3b82f6; box-shadow:0 1px 4px rgba(0,0,0,.06); }}
  .em-stat.red {{ border-left-color:#ef4444; }}
  .em-stat.amber {{ border-left-color:#f59e0b; }}
  .em-stat.green {{ border-left-color:#10b981; }}
  .em-stat .em-lbl {{ font-size:11px; color:#6b7280; text-transform:uppercase; letter-spacing:.5px; }}
  .em-stat .em-val {{ font-size:26px; font-weight:700; color:#1e3a8a; margin-top:2px; }}
  .em-stat .em-sub {{ font-size:11px; color:#94a3b8; margin-top:2px; }}
  .em-insight {{ background:#fffbeb; border-left:4px solid #f59e0b;
                 padding:12px 16px; border-radius:0 8px 8px 0; margin:8px 0;
                 font-size:13px; color:#92400e; }}
  .em-split {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-top:16px; }}
  .em-query-list {{ max-height:480px; overflow-y:auto; }}
  .em-items-panel {{ max-height:480px; overflow-y:auto; }}
  .em-item-card {{ background:white; border:1px solid #e5e7eb; border-radius:8px;
                   padding:12px; margin-bottom:8px; display:flex; gap:10px;
                   align-items:flex-start; transition:all .15s; }}
  .em-item-card:hover {{ box-shadow:0 2px 8px rgba(0,0,0,.08); }}
  .em-item-img {{ width:50px; height:50px; border-radius:6px; object-fit:cover;
                  background:#f3f4f6; flex-shrink:0; }}
  .em-item-info {{ flex:1; min-width:0; }}
  .em-item-title {{ font-size:12px; color:#1a202c; white-space:nowrap;
                    overflow:hidden; text-overflow:ellipsis; margin-bottom:3px; }}
  .em-item-meta {{ font-size:11px; color:#6b7280; }}
  .em-item-meta span {{ margin-right:10px; }}
  .em-filter-row {{ display:flex; gap:6px; margin-bottom:10px; flex-wrap:wrap; }}
  .em-filter-btn {{ padding:5px 12px; border-radius:6px; border:1px solid #d1d5db;
                    background:white; color:#6b7280; cursor:pointer; font-size:12px;
                    transition:all .15s; }}
  .em-filter-btn:hover {{ background:#f1f5f9; color:#1e293b; }}
  .em-filter-btn.active {{ background:#1e3a8a; border-color:#1e3a8a; color:white; }}
  .em-chart-bar {{ display:flex; align-items:center; margin:3px 0; font-size:12px; }}
  .em-chart-label {{ width:130px; color:#6b7280; text-align:right; padding-right:8px;
                     white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
  .em-chart-track {{ flex:1; height:18px; background:#f1f5f9; border-radius:3px; overflow:hidden; }}
  .em-chart-fill {{ height:100%; border-radius:3px; display:flex; align-items:center;
                    padding-left:6px; font-size:10px; color:white; font-weight:600; }}
</style>
</head>
<body>
<div class="container">

<!-- HEADER -->
<div class="header">
  <h1>📊 Recall Metrics Analysis Dashboard</h1>
  <div class="meta">
    <div>Variant <span class="engine-badge">{VARIANT_ENGINE}</span>
         &nbsp;vs&nbsp; Control <span class="engine-badge">{CONTROL_ENGINE}</span></div>
    <div style="margin-top:6px">
      Experiment: <strong>{experiment_id}</strong> &nbsp;|&nbsp;
      GCS: <code style="font-size:12px;opacity:.85">{gcs_path}</code> &nbsp;|&nbsp;
      Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    </div>
  </div>
</div>

<!-- TOP-LEVEL TABS -->
<div class="top-tabs">
  <button class="top-tab active" onclick="switchTopTab('recall')">📊 Recall Analysis</button>
  <button class="top-tab" onclick="switchTopTab('em')" id="em-tab-btn">{em_tab_label}</button>
</div>

<!-- ═══ RECALL ANALYSIS TAB ═══ -->
<div class="top-tab-content active" id="tab-recall">

<!-- T-TEST TABLE -->
<div class="card">
  <h2>🧪 Statistical Significance — Label Counts (Paired t-test)</h2>
  <table>
    <thead>
      <tr>
        <th>Label</th>
        <th style="text-align:right">Control (total)</th>
        <th style="text-align:right">{VARIANT_ENGINE} (total)</th>
        <th style="text-align:right">mean_control</th>
        <th style="text-align:right">mean_{VARIANT_ENGINE}</th>
        <th style="text-align:right">Δ</th>
        <th style="text-align:right">p</th>
      </tr>
    </thead>
    <tbody id="ttest-tbody"></tbody>
  </table>
</div>

<!-- STAT CARDS -->
<div class="stats-row">
  <div class="stat-box"><div class="lbl">Total Queries</div><div class="val">{total_q}</div></div>
  <div class="stat-box"><div class="lbl">Impacted Queries</div>
    <div class="val">{len([q for q in common if query_stats[q]['category'] != 'identical'])}</div></div>
  <div class="stat-box"><div class="lbl">Significant Change</div>
    <div class="val">{cat_counts['significant_change']}</div></div>
  <div class="stat-box"><div class="lbl">Significant Labels</div>
    <div class="val" style="font-size:20px;padding-top:8px">{sig_label_list}</div></div>
</div>

<!-- DISTRIBUTION -->
<div class="card">
  <h2>📋 Query Distribution</h2>
  <table>
    <thead>
      <tr><th>Category</th><th style="text-align:right">Count</th>
          <th style="text-align:right">%</th><th style="width:200px">Distribution</th></tr>
    </thead>
    <tbody id="dist-tbody"></tbody>
  </table>
</div>

<!-- SEGMENT BREAKDOWN -->
<div class="card">
  <h2>🏷️ Query Segment Breakdown (Head / Torso / Tail)</h2>
  <div class="seg-grid" id="seg-grid"></div>
</div>

<!-- SIGNIFICANT CHANGE SECTIONS -->
<div class="card"><h2>📈 Significant Label Changes</h2>
  <div id="sig-sections-container"></div>
</div>

<!-- ITEM PANEL -->
<div class="item-panel" id="item-panel">
  <button class="close-btn" onclick="closeItemPanel()">✕</button>
  <h3 id="item-panel-title"></h3>
  <div class="sub" id="item-panel-sub"></div>
  <div class="item-legend">
    <div class="legend-item"><div class="legend-dot" style="background:#10b981"></div> Good signal</div>
    <div class="legend-item"><div class="legend-dot" style="background:#ef4444"></div> Bad signal</div>
    <div class="legend-item"><div class="legend-dot" style="background:#fcd34d"></div> Concern</div>
  </div>
  <div id="items-grid"></div>
</div>

</div><!-- /tab-recall -->

<!-- ═══ EM CLASSIFIER ANALYSIS TAB ═══ -->
<div class="top-tab-content" id="tab-em">
{em_no_stack_msg}

<!-- BASELINE COMPARISON -->
<div class="card">
  <h2>📊 Primary Stack: Control vs Variant Baseline</h2>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:16px;">
    <div style="background:#eff6ff;border:2px solid #93c5fd;border-radius:12px;padding:20px;">
      <div style="font-size:14px;font-weight:700;color:#1e40af;margin-bottom:12px;">🔵 Control</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
        <div><div style="font-size:11px;color:#6b7280;text-transform:uppercase">Precision (non-4 rate)</div>
          <div style="font-size:28px;font-weight:700;color:#1e3a8a" id="em-ctrl-prec"></div></div>
        <div><div style="font-size:11px;color:#6b7280;text-transform:uppercase">Recall (4s in primary)</div>
          <div style="font-size:28px;font-weight:700;color:#1e3a8a" id="em-ctrl-rec"></div></div>
        <div><div style="font-size:11px;color:#6b7280">Primary stack items</div>
          <div style="font-size:16px;font-weight:600;color:#374151" id="em-ctrl-total"></div></div>
        <div><div style="font-size:11px;color:#6b7280">4s in recall</div>
          <div style="font-size:16px;font-weight:600;color:#374151" id="em-ctrl-recall4s"></div></div>
      </div>
    </div>
    <div style="background:#fef2f2;border:2px solid #fca5a5;border-radius:12px;padding:20px;">
      <div style="font-size:14px;font-weight:700;color:#991b1b;margin-bottom:12px;">🔴 Variant</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">
        <div><div style="font-size:11px;color:#6b7280;text-transform:uppercase">Precision (non-4 rate)</div>
          <div style="font-size:28px;font-weight:700;color:#991b1b" id="em-var-prec"></div></div>
        <div><div style="font-size:11px;color:#6b7280;text-transform:uppercase">Recall (4s in primary)</div>
          <div style="font-size:28px;font-weight:700;color:#991b1b" id="em-var-rec"></div></div>
        <div><div style="font-size:11px;color:#6b7280">Primary stack items</div>
          <div style="font-size:16px;font-weight:600;color:#374151" id="em-var-total"></div></div>
        <div><div style="font-size:11px;color:#6b7280">4s in recall</div>
          <div style="font-size:16px;font-weight:600;color:#374151" id="em-var-recall4s"></div></div>
      </div>
    </div>
  </div>
</div>

<!-- NET VERDICT + DELTA OVERVIEW -->
<div class="em-stats-row">
  <div class="em-stat red"><div class="em-lbl">New Precision Failures</div>
    <div class="em-val" id="em-pi-total"></div>
    <div class="em-sub">Non-4s new to recall → primary stack</div></div>
  <div class="em-stat green"><div class="em-lbl">Precision Improvements</div>
    <div class="em-val" id="em-pimprv-total"></div>
    <div class="em-sub">Non-4s removed from control recall</div></div>
  <div class="em-stat" id="em-net-prec-card"><div class="em-lbl">Net Precision</div>
    <div class="em-val" id="em-net-prec"></div>
    <div class="em-sub" id="em-net-prec-sub"></div></div>
  <div class="em-stat amber"><div class="em-lbl">New Recall Failures</div>
    <div class="em-val" id="em-ri-total"></div>
    <div class="em-sub">4s new to recall → blocked from primary</div></div>
  <div class="em-stat green"><div class="em-lbl">Recall Improvements</div>
    <div class="em-val" id="em-rimprv-total"></div>
    <div class="em-sub">4s new to recall → promoted to primary</div></div>
  <div class="em-stat" id="em-net-rec-card"><div class="em-lbl">Net Recall</div>
    <div class="em-val" id="em-net-rec"></div>
    <div class="em-sub" id="em-net-rec-sub"></div></div>
</div>

<div class="em-insight" id="em-insight-1"></div>
<div class="em-insight" id="em-insight-2"></div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:20px;">
  <div class="card">
    <h2>Precision Issues by Label</h2>
    <div id="em-label-bars"></div>
  </div>
  <div class="card">
    <h2>Top Categories Affected</h2>
    <div id="em-category-bars"></div>
  </div>
</div>

<!-- PRECISION FAILURES SECTION -->
<div class="card">
  <h2>🔴 Precision Failures — Non-4s Entering Primary Stack</h2>
  <p style="color:#6b7280;font-size:13px;margin-bottom:14px;">
    Items <strong>not in control recall</strong>, newly retrieved by variant, promoted to
    primary stack (stack&nbsp;1) despite label&nbsp;≠&nbsp;4. The EM classifier should have blocked these.
  </p>
  <input type="text" class="search-bar" id="em-pi-search" placeholder="Search queries..."
         oninput="renderEmPrecisionQueries()" style="width:100%;padding:10px 14px;
         background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;font-size:14px;
         color:#1a202c;margin-bottom:10px;">
  <div class="em-filter-row">
    <button class="em-filter-btn active" onclick="emPiFilter='all';this.parentNode.querySelectorAll('.em-filter-btn').forEach(b=>b.classList.remove('active'));this.classList.add('active');renderEmPrecisionQueries()">All Labels</button>
    <button class="em-filter-btn" onclick="emPiFilter=1;this.parentNode.querySelectorAll('.em-filter-btn').forEach(b=>b.classList.remove('active'));this.classList.add('active');renderEmPrecisionQueries()">Label 1</button>
    <button class="em-filter-btn" onclick="emPiFilter=2;this.parentNode.querySelectorAll('.em-filter-btn').forEach(b=>b.classList.remove('active'));this.classList.add('active');renderEmPrecisionQueries()">Label 2</button>
    <button class="em-filter-btn" onclick="emPiFilter=3;this.parentNode.querySelectorAll('.em-filter-btn').forEach(b=>b.classList.remove('active'));this.classList.add('active');renderEmPrecisionQueries()">Label 3</button>
    <button class="em-filter-btn" onclick="emPiFilter='top20';this.parentNode.querySelectorAll('.em-filter-btn').forEach(b=>b.classList.remove('active'));this.classList.add('active');renderEmPrecisionQueries()">Top-20 Only</button>
  </div>
  <div class="em-split">
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px">Queries (click to explore)</h3>
      <div class="em-query-list" id="em-pi-query-list"></div></div>
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px" id="em-pi-items-header">Select a query</h3>
      <div class="em-items-panel" id="em-pi-items-list"></div></div>
  </div>
</div>

<!-- PRECISION IMPROVEMENTS SECTION -->
<div class="card">
  <h2>🟢 Precision Improvements — Non-4s Removed from Recall</h2>
  <p style="color:#6b7280;font-size:13px;margin-bottom:14px;">
    Items rated <strong>non-4</strong> that were in the control primary stack but have been
    <strong>removed entirely</strong> from variant recall. This is a win — fewer bad items in the stack.
  </p>
  <input type="text" class="search-bar" id="em-pimprv-search" placeholder="Search queries..."
         oninput="renderEmImprovementQueries()" style="width:100%;padding:10px 14px;
         background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;font-size:14px;
         color:#1a202c;margin-bottom:10px;">
  <div class="em-split">
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px">Queries (click to explore)</h3>
      <div class="em-query-list" id="em-pimprv-query-list"></div></div>
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px" id="em-pimprv-items-header">Select a query</h3>
      <div class="em-items-panel" id="em-pimprv-items-list"></div></div>
  </div>
</div>

<!-- RECALL FAILURES SECTION -->
<div class="card">
  <h2>🟡 Recall Failures — Label-4 Items Blocked from Primary Stack</h2>
  <p style="color:#6b7280;font-size:13px;margin-bottom:14px;">
    Items rated <strong>4★ (excellent)</strong>, newly retrieved by variant, but the EM classifier
    did <strong>not</strong> promote them to stack&nbsp;1. Stuck in stack&nbsp;2/3.
  </p>
  <input type="text" class="search-bar" id="em-ri-search" placeholder="Search queries..."
         oninput="renderEmRecallQueries()" style="width:100%;padding:10px 14px;
         background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;font-size:14px;
         color:#1a202c;margin-bottom:10px;">
  <div class="em-split">
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px">Queries (click to explore)</h3>
      <div class="em-query-list" id="em-ri-query-list"></div></div>
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px" id="em-ri-items-header">Select a query</h3>
      <div class="em-items-panel" id="em-ri-items-list"></div></div>
  </div>
</div>

<!-- RECALL IMPROVEMENTS SECTION -->
<div class="card">
  <h2>🟢 Recall Improvements — 4s New to Recall &amp; Promoted to Primary</h2>
  <p style="color:#6b7280;font-size:13px;margin-bottom:14px;">
    Label-4 items <strong>newly retrieved</strong> by variant and successfully
    <strong>promoted to primary stack</strong>. This is the best outcome — new excellent items surfaced to users.
  </p>
  <input type="text" class="search-bar" id="em-rimprv-search" placeholder="Search queries..."
         oninput="renderEmRecallImprovementQueries()" style="width:100%;padding:10px 14px;
         background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;font-size:14px;
         color:#1a202c;margin-bottom:10px;">
  <div class="em-split">
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px">Queries (click to explore)</h3>
      <div class="em-query-list" id="em-rimprv-query-list"></div></div>
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px" id="em-rimprv-items-header">Select a query</h3>
      <div class="em-items-panel" id="em-rimprv-items-list"></div></div>
  </div>
</div>

<!-- ═══ EM THRESHOLD EFFECTS (STABLE RECALL) ═══ -->
<div class="card" style="border-top:4px solid #8b5cf6;">
  <h2>⚡ EM Threshold Effects — Stable Recall Items</h2>
  <p style="color:#6b7280;font-size:13px;margin-bottom:14px;">
    Items that exist in <strong>both</strong> control and variant recall sets, but changed primary stack status.
    These changes are caused by <strong>dynamic threshold shifts</strong> in the EM classifier — when the recall
    set composition changes, the classifier&rsquo;s decision boundary moves, promoting or demoting items that were always retrievable.
  </p>

  <div style="display:grid;grid-template-columns:1fr auto 1fr;gap:16px;margin-bottom:20px;align-items:center;">
    <div style="background:#f5f3ff;border:2px solid #c4b5fd;border-radius:12px;padding:16px;">
      <div style="font-size:12px;font-weight:700;color:#7c3aed;text-transform:uppercase;margin-bottom:8px">Precision (non-4 stack changes)</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div>
          <div style="font-size:11px;color:#ef4444;font-weight:600">🔺 Promoted (bad)</div>
          <div style="font-size:22px;font-weight:700;color:#ef4444" id="thresh-prec-loss-val"></div>
          <div style="font-size:11px;color:#6b7280" id="thresh-prec-loss-q"></div>
        </div>
        <div>
          <div style="font-size:11px;color:#10b981;font-weight:600">🔻 Demoted (good)</div>
          <div style="font-size:22px;font-weight:700;color:#10b981" id="thresh-prec-gain-val"></div>
          <div style="font-size:11px;color:#6b7280" id="thresh-prec-gain-q"></div>
        </div>
      </div>
    </div>
    <div style="text-align:center;padding:0 8px;">
      <div style="font-size:11px;color:#6b7280;text-transform:uppercase;margin-bottom:4px">Stable items</div>
      <div style="font-size:24px;font-weight:700;color:#7c3aed" id="thresh-stable-total"></div>
      <div style="font-size:11px;color:#6b7280">in both recall sets</div>
    </div>
    <div style="background:#f5f3ff;border:2px solid #c4b5fd;border-radius:12px;padding:16px;">
      <div style="font-size:12px;font-weight:700;color:#7c3aed;text-transform:uppercase;margin-bottom:8px">Recall (4★ stack changes)</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
        <div>
          <div style="font-size:11px;color:#10b981;font-weight:600">🔺 Promoted (good)</div>
          <div style="font-size:22px;font-weight:700;color:#10b981" id="thresh-rec-gain-val"></div>
          <div style="font-size:11px;color:#6b7280" id="thresh-rec-gain-q"></div>
        </div>
        <div>
          <div style="font-size:11px;color:#ef4444;font-weight:600">🔻 Demoted (bad)</div>
          <div style="font-size:22px;font-weight:700;color:#ef4444" id="thresh-rec-loss-val"></div>
          <div style="font-size:11px;color:#6b7280" id="thresh-rec-loss-q"></div>
        </div>
      </div>
    </div>
  </div>

  <div class="em-stats-row">
    <div class="em-stat" id="thresh-net-prec-card" style="border-left-color:#8b5cf6;">
      <div class="em-lbl">Net Threshold Precision</div>
      <div class="em-val" id="thresh-net-prec"></div>
      <div class="em-sub" id="thresh-net-prec-sub"></div></div>
    <div class="em-stat" id="thresh-net-rec-card" style="border-left-color:#8b5cf6;">
      <div class="em-lbl">Net Threshold Recall</div>
      <div class="em-val" id="thresh-net-rec"></div>
      <div class="em-sub" id="thresh-net-rec-sub"></div></div>
  </div>

  <div class="em-insight" id="thresh-insight" style="border-left-color:#8b5cf6;background:#f5f3ff;color:#5b21b6;"></div>

  <!-- Sub-tabs for the 4 threshold buckets -->
  <div style="display:flex;gap:4px;margin:16px 0 12px;background:#ede9fe;border-radius:8px;padding:3px;" id="thresh-subtabs">
    <button class="em-filter-btn active" style="flex:1;text-align:center" onclick="switchThreshTab('prec_loss',this)">🔴 Non-4 Promoted</button>
    <button class="em-filter-btn" style="flex:1;text-align:center" onclick="switchThreshTab('prec_gain',this)">🟢 Non-4 Demoted</button>
    <button class="em-filter-btn" style="flex:1;text-align:center" onclick="switchThreshTab('rec_gain',this)">🟢 4s Promoted</button>
    <button class="em-filter-btn" style="flex:1;text-align:center" onclick="switchThreshTab('rec_loss',this)">🔴 4s Demoted</button>
  </div>

  <input type="text" class="search-bar" id="thresh-search" placeholder="Search queries..."
         oninput="renderThreshQueries()" style="width:100%;padding:10px 14px;
         background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;font-size:14px;
         color:#1a202c;margin-bottom:10px;">
  <div class="em-split">
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px" id="thresh-queries-header">Queries (click to explore)</h3>
      <div class="em-query-list" id="thresh-query-list"></div></div>
    <div><h3 style="font-size:14px;color:#6b7280;margin-bottom:8px" id="thresh-items-header">Select a query</h3>
      <div class="em-items-panel" id="thresh-items-list"></div></div>
  </div>
</div>

</div><!-- /tab-em -->

</div><!-- /container -->

<script>
const DISTRIBUTION       = {distribution_json};
const TTEST              = {ttest_json};
const LABEL_TOTALS       = {label_totals_json};
const LABEL_TABLES       = {label_tables_json};
const QUERY_ITEMS        = {query_items_json};
const SIG_SECTIONS       = {sig_sections_json};
const STRATUM_BREAKDOWN  = {stratum_breakdown_json};
const HAS_DUAL_PRESO     = {'true' if has_dual_preso else 'false'};
const VARIANT_PTSS       = {json.dumps(variant_ptss)};
const VARIANT_TRSP       = {json.dumps(variant_trsp)};

// ── EM Classifier data ────────────────────────────────────────────────────
const EM_STATS             = {em_stats_json};
const EM_PRECISION_QUERIES = {em_precision_queries_json};
const EM_PRECISION_ITEMS   = {em_precision_items_json};
const EM_RECALL_QUERIES    = {em_recall_queries_json};
const EM_RECALL_ITEMS      = {em_recall_items_json};
const EM_IMPROVEMENT_QUERIES        = {em_improvement_queries_json};
const EM_IMPROVEMENT_ITEMS          = {em_improvement_items_json};
const EM_RECALL_IMPROVEMENT_QUERIES = {em_recall_improvement_queries_json};
const EM_RECALL_IMPROVEMENT_ITEMS   = {em_recall_improvement_items_json};
const THRESH_PREC_LOSS_QUERIES  = {thresh_prec_loss_queries_json};
const THRESH_PREC_LOSS_ITEMS    = {thresh_prec_loss_items_json};
const THRESH_REC_GAIN_QUERIES   = {thresh_rec_gain_queries_json};
const THRESH_REC_GAIN_ITEMS     = {thresh_rec_gain_items_json};
const THRESH_PREC_GAIN_QUERIES  = {thresh_prec_gain_queries_json};
const THRESH_PREC_GAIN_ITEMS    = {thresh_prec_gain_items_json};
const THRESH_REC_LOSS_QUERIES   = {thresh_rec_loss_queries_json};
const THRESH_REC_LOSS_ITEMS     = {thresh_rec_loss_items_json};
const HAS_STACK            = {'true' if has_stack else 'false'};

// ── Utilities ──────────────────────────────────────────────────────────────
function diffClassLabel(d, label) {{
  if (d === 0) return 'diff-zero';
  const posGood = (label === 4);
  if (d > 0) return posGood ? 'diff-pos' : 'diff-neg';
  return posGood ? 'diff-neg' : 'diff-pos';
}}
function diffText(d) {{ return d > 0 ? '+' + d : String(d); }}

function highlightTitle(title, query) {{
  if (!title || !query) return title || '';
  const stop = new Set(["a","an","and","are","as","at","be","but","by","for","if","in",
    "into","is","it","no","not","of","on","or","s","such","that","the","their","then",
    "there","these","they","this","to","was","will","with","fresh","cheap","sale",
    "price","discount","cute","cool","online","review","coupon"]);
  const tokens = query.toLowerCase().split(/\\s+/).filter(t => t.length > 1 && !stop.has(t));
  let result = title;
  tokens.forEach(tok => {{
    const esc = tok.replace(/[.*+?^${{}}()|[\\]\\\\]/g, '\\\\$&');
    result = result.replace(new RegExp('(' + esc + ')', 'gi'), '<span class="hl">$1</span>');
  }});
  return result;
}}

function buildDualPresoUrl(item, cleanQuery) {{
  if (!HAS_DUAL_PRESO || !item.stores) return null;
  const PRESO = 'http://preso-usgm-wcnp.prod.walmart.com/v2/search';
  const HEADERS = JSON.stringify({{"tenant-id": "elh9ie", "accept-language": "en-US"}});
  const base = PRESO + '?prg=desktop&stores=' + item.stores
    + '&stateOrProvinceCode=' + item.state + '&zipcode=' + item.zipcode;
  const varUrl = base
    + (VARIANT_PTSS ? '&ptss=' + VARIANT_PTSS : '')
    + (VARIANT_TRSP ? '&trsp=' + VARIANT_TRSP : '');
  const q = cleanQuery.trim().replace(/\\s+/g, '+');
  return 'https://sunlight.walmart.com/dual_preso_view'
    + '?eng_a=' + encodeURIComponent(base)
    + '&headers_a=' + encodeURIComponent(HEADERS)
    + '&eng_b=' + encodeURIComponent(varUrl)
    + '&headers_b=' + encodeURIComponent(HEADERS)
    + '&query=' + q
    + '&page=1';
}}

// ── Stratum badge helper ───────────────────────────────────────────────────
function stratumBadge(s) {{
  const cls = s ? 'stratum-' + s.toLowerCase() : 'stratum-unknown';
  return `<span class="stratum-badge ${{cls}}">${{s || '?'}}</span>`;
}}

// ── Segment breakdown card ─────────────────────────────────────────────────
function renderSegmentBreakdown() {{
  const cfg = {{
    head:  {{label:'Head',  cls:'head',  emoji:'🔵'}},
    torso: {{label:'Torso', cls:'torso', emoji:'🟡'}},
    tail:  {{label:'Tail',  cls:'tail',  emoji:'⚪'}},
  }};
  document.getElementById('seg-grid').innerHTML = ['head','torso','tail'].map(seg => {{
    const d = STRATUM_BREAKDOWN[seg] || {{total:0,significant_change:0,identical:0,diff_same_labels:0}};
    const sigPct = d.total ? Math.round(d.significant_change/d.total*100) : 0;
    const c = cfg[seg];
    return `
      <div class="seg-card ${{c.cls}}">
        <div class="seg-card-title">${{c.emoji}} ${{c.label}} Queries</div>
        <div class="seg-stat-row"><span>Total</span><span class="seg-stat-val">${{d.total}}</span></div>
        <div class="seg-stat-row"><span>Significant change</span><span class="seg-stat-val">${{d.significant_change}} (${{sigPct}}%)</span></div>
        <div class="seg-stat-row"><span>Identical</span><span class="seg-stat-val">${{d.identical}}</span></div>
        <div class="seg-stat-row"><span>Diff same labels</span><span class="seg-stat-val">${{d.diff_same_labels}}</span></div>
      </div>`;
  }}).join('');
}}

// ── T-test table ───────────────────────────────────────────────────────────
function renderTtestTable() {{
  const names = {{'1':'class_1','2':'class_2','3':'class_3','4':'class_4'}};
  document.getElementById('ttest-tbody').innerHTML = ['1','2','3','4'].map(lbl => {{
    const t = TTEST[lbl], tot = LABEL_TOTALS[lbl];
    const sig = t.significant;
    const good = lbl === '4' ? t.diff > 0 : t.diff < 0;
    let rowStyle='', dStyle='', pStyle='';
    if (sig && good)  {{ rowStyle='background:#d1fae5;'; dStyle='color:#065f46;font-weight:700'; pStyle='background:#059669;color:white;font-weight:700;border-radius:4px;padding:2px 8px'; }}
    if (sig && !good) {{ rowStyle='background:#fee2e2;'; dStyle='color:#991b1b;font-weight:700'; pStyle='background:#dc2626;color:white;font-weight:700;border-radius:4px;padding:2px 8px'; }}
    const dText = (t.diff > 0 ? '+' : '') + t.diff.toFixed(6);
    return `<tr style="${{rowStyle}}">
      <td style="font-weight:600">${{names[lbl]}}</td>
      <td style="text-align:right">${{tot.control.toLocaleString()}}</td>
      <td style="text-align:right">${{tot.variant.toLocaleString()}}</td>
      <td style="text-align:right">${{t.ctrl_mean.toFixed(6)}}</td>
      <td style="text-align:right">${{t.var_mean.toFixed(6)}}</td>
      <td style="text-align:right"><span style="${{dStyle}}">${{dText}}</span></td>
      <td style="text-align:right"><span style="${{pStyle}}">${{t.p_value.toFixed(6)}}</span></td>
    </tr>`;
  }}).join('');
}}

// ── Distribution table ─────────────────────────────────────────────────────
function renderDistribution() {{
  document.getElementById('dist-tbody').innerHTML = DISTRIBUTION.map(r => `
    <tr>
      <td style="font-weight:500">${{r.category}}</td>
      <td style="text-align:right;font-size:18px;font-weight:700;color:#1e3a8a">${{r.count}}</td>
      <td style="text-align:right;color:#6b7280">${{r.pct}}%</td>
      <td style="width:200px">
        <div class="pct-bar-wrap"><div class="pct-bar" style="width:${{r.pct}}%"></div></div>
      </td>
    </tr>`).join('');
}}

// ── Significant sections ───────────────────────────────────────────────────
let activeSigTab = {{}};
let activeSigSeg = {{}};
let activeRow = null;

function renderSigSections() {{
  const container = document.getElementById('sig-sections-container');
  if (!SIG_SECTIONS.length) {{
    container.innerHTML = '<p style="color:#6b7280">No labels reached p &lt; 0.05.</p>';
    return;
  }}
  container.innerHTML = SIG_SECTIONS.map(s => {{
    activeSigTab[s.label] = 'top';
    activeSigSeg[s.label] = 'all';
    const verdict = s.good
      ? '<span style="color:#10b981;font-weight:600">✅ Good for quality</span>'
      : '<span style="color:#ef4444;font-weight:600">⚠️ Needs attention</span>';
    return `
      <div class="sig-section" id="sig-section-${{s.label}}">
        <div class="sig-label-header">
          <div class="sig-star" style="background:${{s.color}}">${{s.star}}</div>
          <div>
            <strong style="font-size:15px">${{s.star}} Count Change</strong> ${{verdict}}
            <div class="sig-info">p = ${{s.p_value.toFixed(6)}} &nbsp;|&nbsp; Control avg: ${{s.ctrl_mean.toFixed(2)}} → Variant avg: ${{s.var_mean.toFixed(2)}} (${{s.diff > 0 ? '+' : ''}}${{s.diff.toFixed(2)}})</div>
          </div>
        </div>
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:12px;">
          <div class="sig-tabs" style="margin:0">
            <button class="sig-tab active" onclick="switchTab(${{s.label}},'top',this)">Top driving queries</button>
            <button class="sig-tab" onclick="switchTab(${{s.label}},'counter',this)">Counteracting queries</button>
          </div>
          <div class="seg-filter-bar" style="margin:0">
            <span>Segment:</span>
            <button class="seg-btn active" data-seg="all"  onclick="switchSeg(${{s.label}},'all',this)">All</button>
            <button class="seg-btn"        data-seg="head" onclick="switchSeg(${{s.label}},'head',this)">🔵 Head</button>
            <button class="seg-btn"        data-seg="torso" onclick="switchSeg(${{s.label}},'torso',this)">🟡 Torso</button>
            <button class="seg-btn"        data-seg="tail" onclick="switchSeg(${{s.label}},'tail',this)">⚪ Tail</button>
          </div>
        </div>
        <div class="query-table-wrap">
          <table>
            <thead><tr><th>Query</th><th style="width:90px">Segment</th>
              <th style="text-align:right;width:90px">Control</th>
              <th style="text-align:right;width:90px">Variant</th>
              <th style="text-align:right;width:90px">Difference</th></tr></thead>
            <tbody id="sig-tbody-${{s.label}}"></tbody>
          </table>
        </div>
      </div>`;
  }}).join('<hr style="border:none;border-top:1px solid #e5e7eb;margin:24px 0">');
  SIG_SECTIONS.forEach(s => renderSigTable(s.label));
}}

function switchTab(label, mode, btn) {{
  activeSigTab[label] = mode;
  document.getElementById('sig-section-' + label).querySelectorAll('.sig-tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  renderSigTable(label);
}}

function switchSeg(label, seg, btn) {{
  activeSigSeg[label] = seg;
  document.getElementById('sig-section-' + label).querySelectorAll('.seg-btn').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  renderSigTable(label);
}}

function renderSigTable(label) {{
  const tbody = document.getElementById('sig-tbody-' + label);
  let rows = LABEL_TABLES[String(label)] || [];
  const overall = SIG_SECTIONS.find(s => s.label === label).diff;
  const seg = activeSigSeg[label] || 'all';

  // Filter by segment
  if (seg !== 'all') rows = rows.filter(r => r.stratum === seg);

  let display;
  if (activeSigTab[label] === 'top') {{
    display = overall > 0 ? rows.filter(r => r.difference > 0).slice(0,20)
                          : rows.filter(r => r.difference < 0).slice(-20).reverse();
  }} else {{
    display = overall > 0 ? rows.filter(r => r.difference < 0).slice(-20).reverse()
                          : rows.filter(r => r.difference > 0).slice(0,20);
  }}
  if (!display.length) {{ tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#9ca3af;padding:20px">No queries in this category</td></tr>'; return; }}
  tbody.innerHTML = display.map(r => `
    <tr onclick="showItemGrid('${{r.rawQuery.replace(/'/g,"\\\\'")}}','${{r.query.replace(/'/g,"\\\\'")}}',this)">
      <td style="max-width:360px;word-break:break-word">${{r.query}}</td>
      <td>${{stratumBadge(r.stratum)}}</td>
      <td style="text-align:right">${{r.control}}</td>
      <td style="text-align:right">${{r.variant}}</td>
      <td style="text-align:right" class="${{diffClassLabel(r.difference,label)}}">${{diffText(r.difference)}}</td>
    </tr>`).join('');
}}

// ── Item grid ──────────────────────────────────────────────────────────────
function renderItemCard(item, cleanQuery) {{
  const imgHtml = item.image
    ? `<img class="item-img" src="${{item.image}}" alt="" loading="lazy" onerror="this.style.display='none';this.nextSibling.style.display='flex'"><div class="item-img-placeholder" style="display:none">No image</div>`
    : `<div class="item-img-placeholder">No image</div>`;
  const titleHtml = highlightTitle(item.title, cleanQuery);
  const borderCol = item.status === 'var_only' ? '#10b981' : '#ef4444';
  const dualUrl = buildDualPresoUrl(item, cleanQuery);
  const clickAttr = dualUrl ? `onclick="window.open('${{dualUrl}}','_blank')" style="border-color:${{borderCol}};cursor:pointer;" title="Open Dual Preso view"` : `style="border-color:${{borderCol}};"`;
  const sunBadge = dualUrl ? `<span style="font-size:11px;background:#f0fdf4;color:#166534;border:1px solid #86efac;border-radius:4px;padding:1px 7px;white-space:nowrap">⚡ Dual Preso</span>` : '';
  return `
    <div class="item-card" ${{clickAttr}}>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
        <div class="item-id">${{item.item_id || item.pg_prod_id}}</div>
        ${{sunBadge}}
      </div>
      ${{imgHtml}}
      <div class="item-title">${{titleHtml}}</div>
      <div class="item-meta">
        <div><strong>Category:</strong> ${{item.l1_category || 'N/A'}}</div>
        <div><strong>Position:</strong> ${{item.position}}</div>
        <div><strong>pg_prod_id:</strong> ${{item.pg_prod_id}}</div>
        <span class="item-label lbl-${{item.label}}">${{item.label}}★</span>
      </div>
    </div>`;
}}

function showItemGrid(rawQuery, cleanQuery, rowEl) {{
  if (activeRow) activeRow.classList.remove('active-row');
  activeRow = rowEl;
  rowEl.classList.add('active-row');

  const allItems = QUERY_ITEMS[rawQuery] || [];
  const delta = allItems.filter(i => i.status !== 'both');
  const panel = document.getElementById('item-panel');

  document.getElementById('item-panel-title').textContent = '🔍 ' + cleanQuery;

  // Sub-header: per-label deltas
  const sigSet = new Set(SIG_SECTIONS.map(s => s.label));
  const parts = [];
  [1,2,3,4].forEach(lbl => {{
    const rows = LABEL_TABLES[String(lbl)] || [];
    const row = rows.find(r => r.rawQuery === rawQuery);
    const d = row ? row.difference : 0;
    if (d === 0 && lbl !== 4) return;
    const isSig = sigSet.has(lbl);
    const isGood = lbl === 4 ? d > 0 : d < 0;
    const dir = d > 0 ? 'gained' : 'dropped';
    const color = isSig ? (isGood ? '#059669' : '#dc2626') : '#374151';
    const suffix = isSig ? '' : ' (Neutral)';
    parts.push(`<span style="color:${{color}};font-weight:600">${{Math.abs(d)}} ${{lbl}}★ ${{dir}}</span>${{suffix}}`);
  }});
  document.getElementById('item-panel-sub').innerHTML = parts.join('&nbsp;&nbsp;|&nbsp;&nbsp;') || '&nbsp;';

  // Group by label
  const groups = {{}};
  [4,3,2,1].forEach(l => {{ groups[l] = {{gained:[],removed:[]}}; }});
  delta.forEach(item => {{
    if (!groups[item.label]) return;
    if (item.status === 'var_only') groups[item.label].gained.push(item);
    else if (item.status === 'ctrl_only') groups[item.label].removed.push(item);
  }});

  const cfg = {{
    4: {{gs:'good', gl:'✅ Good Signal — 4★ Gained', rs:'bad',     rl:'❌ Bad Signal — 4★ Dropped'}},
    3: {{gs:'concern',gl:'⚠️ Concern — 3★ Gained',  rs:'good',    rl:'✅ Good Signal — 3★ Removed'}},
    2: {{gs:'concern',gl:'⚠️ Concern — 2★ Gained',  rs:'good',    rl:'✅ Good Signal — 2★ Removed'}},
    1: {{gs:'concern',gl:'⚠️ Concern — 1★ Gained',  rs:'good',    rl:'✅ Good Signal — 1★ Removed'}},
  }};

  let html = '';
  [4,3,2,1].forEach(lbl => {{
    const g = groups[lbl];
    if (!g.gained.length && !g.removed.length) return;
    const c = cfg[lbl];
    const gc = g.gained.length  ? g.gained.map(i  => renderItemCard(i, cleanQuery)).join('') : '<div class="label-col-empty">None</div>';
    const rc = g.removed.length ? g.removed.map(i => renderItemCard(i, cleanQuery)).join('') : '<div class="label-col-empty">None</div>';
    html += `
      <div class="label-section">
        <div class="label-section-title">Label ${{lbl}}★</div>
        <div class="label-section-cols">
          <div class="label-col">
            <div class="label-col-header ${{c.gs}}">${{c.gl}} (${{g.gained.length}})</div>
            <div class="label-col-items ${{c.gs}}-border">${{gc}}</div>
          </div>
          <div class="label-col">
            <div class="label-col-header ${{c.rs}}">${{c.rl}} (${{g.removed.length}})</div>
            <div class="label-col-items ${{c.rs}}-border">${{rc}}</div>
          </div>
        </div>
      </div>`;
  }});

  if (!html) html = '<div style="text-align:center;color:#9ca3af;padding:40px;font-style:italic">No delta items for this query</div>';
  document.getElementById('items-grid').innerHTML = html;
  panel.classList.add('visible');
  panel.scrollIntoView({{behavior:'smooth',block:'start'}});
}}

function closeItemPanel() {{
  document.getElementById('item-panel').classList.remove('visible');
  if (activeRow) {{ activeRow.classList.remove('active-row'); activeRow = null; }}
}}

// ── Top-level tab switching ────────────────────────────────────────────────
function switchTopTab(tab) {{
  document.querySelectorAll('.top-tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.top-tab').forEach(el => {{ el.classList.remove('active','active-red'); }});
  document.getElementById('tab-' + tab).classList.add('active');
  const btns = document.querySelectorAll('.top-tab');
  if (tab === 'recall') btns[0].classList.add('active');
  else {{ btns[1].classList.add('active-red'); if (!emInitialized) initEmTab(); }}
}}

// ── EM Classifier tab ─────────────────────────────────────────────────────
let emInitialized = false;
let emPiFilter = 'all';

function initEmTab() {{
  if (!HAS_STACK) return;
  emInitialized = true;

  // Baseline comparison
  document.getElementById('em-ctrl-prec').textContent = EM_STATS.ctrl_primary_prec + '%';
  document.getElementById('em-ctrl-rec').textContent = EM_STATS.ctrl_primary_rec + '%';
  document.getElementById('em-ctrl-total').textContent = (EM_STATS.ctrl_primary_total || 0).toLocaleString();
  document.getElementById('em-ctrl-recall4s').textContent = (EM_STATS.ctrl_recall_4s || 0).toLocaleString();
  document.getElementById('em-var-prec').textContent = EM_STATS.var_primary_prec + '%';
  document.getElementById('em-var-rec').textContent = EM_STATS.var_primary_rec + '%';
  document.getElementById('em-var-total').textContent = (EM_STATS.var_primary_total || 0).toLocaleString();
  document.getElementById('em-var-recall4s').textContent = (EM_STATS.var_recall_4s || 0).toLocaleString();

  // Delta stat cards
  document.getElementById('em-pi-total').textContent = (EM_STATS.precision_total || 0).toLocaleString();
  document.getElementById('em-ri-total').textContent = (EM_STATS.recall_total || 0).toLocaleString();
  document.getElementById('em-pimprv-total').textContent = (EM_STATS.prec_improvement_total || 0).toLocaleString();
  document.getElementById('em-rimprv-total').textContent = (EM_STATS.recall_improvement_total || 0).toLocaleString();

  // Net verdict
  const netP = EM_STATS.net_precision || 0;
  const netR = EM_STATS.net_recall || 0;
  document.getElementById('em-net-prec').textContent = (netP > 0 ? '+' : '') + netP.toLocaleString();
  document.getElementById('em-net-rec').textContent = (netR > 0 ? '+' : '') + netR.toLocaleString();
  document.getElementById('em-net-prec-sub').textContent = netP >= 0 ? 'Fewer bad items (good)' : 'More bad items (bad)';
  document.getElementById('em-net-rec-sub').textContent = netR >= 0 ? 'More good items surfaced' : 'Good items lost';
  const npCard = document.getElementById('em-net-prec-card');
  const nrCard = document.getElementById('em-net-rec-card');
  npCard.classList.add(netP >= 0 ? 'green' : 'red');
  nrCard.classList.add(netR >= 0 ? 'green' : 'red');

  // Insights
  const l1pct = EM_STATS.precision_total ? (EM_STATS.precision_label_1/EM_STATS.precision_total*100).toFixed(1) : 0;
  document.getElementById('em-insight-1').innerHTML =
    '<strong>Net Verdict:</strong> ' +
    (netP >= 0 ? '✅ Precision improved by ' + Math.abs(netP).toLocaleString() + ' items' : '⚠️ Precision degraded by ' + Math.abs(netP).toLocaleString() + ' items') +
    ' &nbsp;|&nbsp; ' +
    (netR >= 0 ? '✅ Recall improved by ' + Math.abs(netR).toLocaleString() + ' items' : '⚠️ Recall degraded by ' + Math.abs(netR).toLocaleString() + ' items') +
    '. Variant recall set has ' + (EM_STATS.new_to_recall_total || 0).toLocaleString() + ' new items and lost ' +
    (EM_STATS.lost_from_recall_total || 0).toLocaleString() + '.';
  document.getElementById('em-insight-2').innerHTML =
    '<strong>Position Impact:</strong> ' + (EM_STATS.precision_in_top10 || 0).toLocaleString() +
    ' precision failures in top-10 positions (most visible). ' + l1pct + '% of failures are label-1 (irrelevant). Mean failure position: ' + (EM_STATS.precision_mean_pos || 0) + '.';

  // Label bars
  const piTotal = EM_STATS.precision_total || 1;
  const bars = [
    {{label:'Label 1 (Irrelevant)', count:EM_STATS.precision_label_1 || 0, color:'#ef4444'}},
    {{label:'Label 2 (Poor)',       count:EM_STATS.precision_label_2 || 0, color:'#f97316'}},
    {{label:'Label 3 (Fair)',       count:EM_STATS.precision_label_3 || 0, color:'#84cc16'}},
  ];
  document.getElementById('em-label-bars').innerHTML = bars.map(b => `
    <div class="em-chart-bar">
      <div class="em-chart-label">${{b.label}}</div>
      <div class="em-chart-track"><div class="em-chart-fill" style="width:${{b.count/piTotal*100}}%;background:${{b.color}}">${{b.count.toLocaleString()}}</div></div>
    </div>`).join('');

  // Category bars
  const cats = EM_STATS.precision_by_category || {{}};
  const catEntries = Object.entries(cats).slice(0, 10);
  const catMax = catEntries.length ? catEntries[0][1] : 1;
  document.getElementById('em-category-bars').innerHTML = catEntries.map(([k,v]) => `
    <div class="em-chart-bar">
      <div class="em-chart-label">${{k}}</div>
      <div class="em-chart-track"><div class="em-chart-fill" style="width:${{v/catMax*100}}%;background:#8b5cf6">${{v.toLocaleString()}}</div></div>
    </div>`).join('');

  renderEmPrecisionQueries();
  renderEmRecallQueries();
  renderEmImprovementQueries();
  renderEmRecallImprovementQueries();
  initThreshSection();
}}

// ── Precision Failures ─────────────────────────────────────────────────
function renderEmPrecisionQueries() {{
  const search = (document.getElementById('em-pi-search').value || '').toLowerCase();
  let qs = EM_PRECISION_QUERIES.filter(q => q.cleanQuery.toLowerCase().includes(search));
  const container = document.getElementById('em-pi-query-list');
  let html = '<table class="query-table" style="font-size:13px"><thead><tr>' +
    '<th>Query</th><th style="width:60px">Count</th><th style="width:50px">L1</th>' +
    '<th style="width:50px">L2</th><th style="width:50px">L3</th><th style="width:60px">Top20</th></tr></thead><tbody>';
  qs.forEach(q => {{
    html += `<tr onclick="showEmPrecisionItems('${{q.query.replace(/'/g,"\\\\'")}}')">
      <td style="max-width:240px;word-break:break-word">${{q.cleanQuery}}</td>
      <td><span style="background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:4px;font-weight:600;font-size:12px">${{q.count}}</span></td>
      <td>${{q.label_1}}</td><td>${{q.label_2}}</td><td>${{q.label_3}}</td>
      <td>${{q.top20 > 0 ? '<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:4px;font-weight:600;font-size:12px">' + q.top20 + '</span>' : '-'}}</td>
    </tr>`;
  }});
  html += '</tbody></table>';
  container.innerHTML = html;
}}

function showEmPrecisionItems(query) {{
  let items = EM_PRECISION_ITEMS.filter(i => i.query === query);
  if (emPiFilter === 'top20') items = items.filter(i => i.position <= 20);
  else if (emPiFilter !== 'all') items = items.filter(i => i.label === emPiFilter);
  items.sort((a,b) => a.position - b.position);
  const cq = EM_PRECISION_QUERIES.find(q => q.query === query);
  document.getElementById('em-pi-items-header').textContent =
    items.length + ' items for: ' + (cq ? cq.cleanQuery : query.split(' (')[0]);
  document.getElementById('em-pi-items-list').innerHTML = items.length
    ? items.map(renderEmItemCard).join('')
    : '<p style="color:#9ca3af;padding:20px;text-align:center">No items match filter.</p>';
}}

// ── Precision Improvements ──────────────────────────────────────────────
function renderEmImprovementQueries() {{
  const search = (document.getElementById('em-pimprv-search').value || '').toLowerCase();
  let qs = EM_IMPROVEMENT_QUERIES.filter(q => q.cleanQuery.toLowerCase().includes(search));
  const container = document.getElementById('em-pimprv-query-list');
  let html = '<table class="query-table" style="font-size:13px"><thead><tr>' +
    '<th>Query</th><th style="width:60px">Count</th><th style="width:50px">L1</th>' +
    '<th style="width:50px">L2</th><th style="width:50px">L3</th></tr></thead><tbody>';
  qs.forEach(q => {{
    html += `<tr onclick="showEmImprovementItems('${{q.query.replace(/'/g,"\\\\'")}}')">
      <td style="max-width:240px;word-break:break-word">${{q.cleanQuery}}</td>
      <td><span style="background:#d1fae5;color:#065f46;padding:2px 8px;border-radius:4px;font-weight:600;font-size:12px">${{q.count}}</span></td>
      <td>${{q.label_1 || 0}}</td><td>${{q.label_2 || 0}}</td><td>${{q.label_3 || 0}}</td>
    </tr>`;
  }});
  html += '</tbody></table>';
  container.innerHTML = html;
}}

function showEmImprovementItems(query) {{
  const items = EM_IMPROVEMENT_ITEMS.filter(i => i.query === query).sort((a,b) => a.position - b.position);
  const cq = EM_IMPROVEMENT_QUERIES.find(q => q.query === query);
  document.getElementById('em-pimprv-items-header').textContent =
    items.length + ' removed non-4s for: ' + (cq ? cq.cleanQuery : query.split(' (')[0]);
  document.getElementById('em-pimprv-items-list').innerHTML = items.length
    ? items.map(renderEmItemCard).join('')
    : '<p style="color:#9ca3af;padding:20px;text-align:center">No items.</p>';
}}

// ── Recall Failures ─────────────────────────────────────────────────────
function renderEmRecallQueries() {{
  const search = (document.getElementById('em-ri-search').value || '').toLowerCase();
  let qs = EM_RECALL_QUERIES.filter(q => q.cleanQuery.toLowerCase().includes(search));
  const container = document.getElementById('em-ri-query-list');
  let html = '<table class="query-table" style="font-size:13px"><thead><tr>' +
    '<th>Query</th><th style="width:80px">Blocked 4s</th><th style="width:80px">Segment</th></tr></thead><tbody>';
  qs.forEach(q => {{
    html += `<tr onclick="showEmRecallItems('${{q.query.replace(/'/g,"\\\\'")}}')">
      <td style="max-width:280px;word-break:break-word">${{q.cleanQuery}}</td>
      <td><span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:4px;font-weight:600;font-size:12px">${{q.count}}</span></td>
      <td style="font-size:11px">${{q.stratum || ''}}</td>
    </tr>`;
  }});
  html += '</tbody></table>';
  container.innerHTML = html;
}}

function showEmRecallItems(query) {{
  const items = EM_RECALL_ITEMS.filter(i => i.query === query).sort((a,b) => a.position - b.position);
  const cq = EM_RECALL_QUERIES.find(q => q.query === query);
  document.getElementById('em-ri-items-header').textContent =
    items.length + ' blocked 4s for: ' + (cq ? cq.cleanQuery : query.split(' (')[0]);
  document.getElementById('em-ri-items-list').innerHTML = items.length
    ? items.map(i => renderEmItemCard(i, true)).join('')
    : '<p style="color:#9ca3af;padding:20px;text-align:center">No items.</p>';
}}

// ── Recall Improvements ─────────────────────────────────────────────────
function renderEmRecallImprovementQueries() {{
  const search = (document.getElementById('em-rimprv-search').value || '').toLowerCase();
  let qs = EM_RECALL_IMPROVEMENT_QUERIES.filter(q => q.cleanQuery.toLowerCase().includes(search));
  const container = document.getElementById('em-rimprv-query-list');
  let html = '<table class="query-table" style="font-size:13px"><thead><tr>' +
    '<th>Query</th><th style="width:80px">New 4s</th><th style="width:80px">Segment</th></tr></thead><tbody>';
  qs.forEach(q => {{
    html += `<tr onclick="showEmRecallImprovementItems('${{q.query.replace(/'/g,"\\\\'")}}')">
      <td style="max-width:280px;word-break:break-word">${{q.cleanQuery}}</td>
      <td><span style="background:#d1fae5;color:#065f46;padding:2px 8px;border-radius:4px;font-weight:600;font-size:12px">${{q.count}}</span></td>
      <td style="font-size:11px">${{q.stratum || ''}}</td>
    </tr>`;
  }});
  html += '</tbody></table>';
  container.innerHTML = html;
}}

function showEmRecallImprovementItems(query) {{
  const items = EM_RECALL_IMPROVEMENT_ITEMS.filter(i => i.query === query).sort((a,b) => a.position - b.position);
  const cq = EM_RECALL_IMPROVEMENT_QUERIES.find(q => q.query === query);
  document.getElementById('em-rimprv-items-header').textContent =
    items.length + ' new 4s promoted for: ' + (cq ? cq.cleanQuery : query.split(' (')[0]);
  document.getElementById('em-rimprv-items-list').innerHTML = items.length
    ? items.map(renderEmItemCard).join('')
    : '<p style="color:#9ca3af;padding:20px;text-align:center">No items.</p>';
}}

// ── Shared item card renderer ───────────────────────────────────────────
function renderEmItemCard(item, isRecall) {{
  const lblCls = 'lbl-' + item.label;
  const posHot = item.position <= 20 ? 'background:#fee2e2;color:#991b1b;' : '';
  return `<div class="em-item-card">
    ${{item.image ? '<img class="em-item-img" src="' + item.image + '" onerror="this.style.display=\\'none\\'">' : ''}}
    <div class="em-item-info">
      <div class="em-item-title">${{item.title || item.pg_prod_id}}</div>
      <div class="em-item-meta">
        <span><strong>ID:</strong> ${{item.pg_prod_id}}</span>
        <span style="padding:1px 6px;border-radius:3px;font-size:11px;font-weight:600;${{posHot}}">${{isRecall ? 'Stack ' + item.stack + ' / Pos #' + item.position : 'Pos #' + item.position}}</span>
        <span><strong>Cat:</strong> ${{item.l1_category || '-'}}</span>
      </div>
    </div>
    <span class="item-label ${{lblCls}}">${{item.label}}★</span>
  </div>`;
}}

// ── Threshold Effects tab ────────────────────────────────────────────────
let activeThreshTab = 'prec_loss';
const THRESH_DATA = {{
  prec_loss: {{ queries: THRESH_PREC_LOSS_QUERIES, items: THRESH_PREC_LOSS_ITEMS, hasLabels: true }},
  prec_gain: {{ queries: THRESH_PREC_GAIN_QUERIES, items: THRESH_PREC_GAIN_ITEMS, hasLabels: true }},
  rec_gain:  {{ queries: THRESH_REC_GAIN_QUERIES,  items: THRESH_REC_GAIN_ITEMS,  hasLabels: false }},
  rec_loss:  {{ queries: THRESH_REC_LOSS_QUERIES,  items: THRESH_REC_LOSS_ITEMS,  hasLabels: false }},
}};

function initThreshSection() {{
  // Summary cards
  document.getElementById('thresh-stable-total').textContent = (EM_STATS.stable_recall_total || 0).toLocaleString();
  document.getElementById('thresh-prec-loss-val').textContent = (EM_STATS.thresh_prec_loss || 0).toLocaleString();
  document.getElementById('thresh-prec-loss-q').textContent = (EM_STATS.thresh_prec_loss_queries || 0) + ' queries';
  document.getElementById('thresh-prec-gain-val').textContent = (EM_STATS.thresh_prec_gain || 0).toLocaleString();
  document.getElementById('thresh-prec-gain-q').textContent = (EM_STATS.thresh_prec_gain_queries || 0) + ' queries';
  document.getElementById('thresh-rec-gain-val').textContent = (EM_STATS.thresh_rec_gain || 0).toLocaleString();
  document.getElementById('thresh-rec-gain-q').textContent = (EM_STATS.thresh_rec_gain_queries || 0) + ' queries';
  document.getElementById('thresh-rec-loss-val').textContent = (EM_STATS.thresh_rec_loss || 0).toLocaleString();
  document.getElementById('thresh-rec-loss-q').textContent = (EM_STATS.thresh_rec_loss_queries || 0) + ' queries';

  // Net threshold verdict
  const tnp = EM_STATS.thresh_net_prec || 0;
  const tnr = EM_STATS.thresh_net_rec || 0;
  document.getElementById('thresh-net-prec').textContent = (tnp > 0 ? '+' : '') + tnp.toLocaleString();
  document.getElementById('thresh-net-rec').textContent = (tnr > 0 ? '+' : '') + tnr.toLocaleString();
  document.getElementById('thresh-net-prec-sub').textContent = tnp >= 0 ? 'More non-4s demoted than promoted' : 'More non-4s promoted than demoted';
  document.getElementById('thresh-net-rec-sub').textContent = tnr >= 0 ? 'More 4s promoted than demoted' : 'More 4s demoted than promoted';
  document.getElementById('thresh-net-prec-card').classList.add(tnp >= 0 ? 'green' : 'red');
  document.getElementById('thresh-net-rec-card').classList.add(tnr >= 0 ? 'green' : 'red');

  // Insight
  document.getElementById('thresh-insight').innerHTML =
    '<strong>Threshold Shift Analysis:</strong> Of ' + (EM_STATS.stable_recall_total || 0).toLocaleString() +
    ' items in both recall sets, ' +
    ((EM_STATS.thresh_prec_loss || 0) + (EM_STATS.thresh_rec_gain || 0)).toLocaleString() + ' were promoted and ' +
    ((EM_STATS.thresh_prec_gain || 0) + (EM_STATS.thresh_rec_loss || 0)).toLocaleString() + ' were demoted from primary stack. ' +
    'Net precision: ' + (tnp >= 0 ? '✅ +' : '⚠️ ') + Math.abs(tnp).toLocaleString() + ' &nbsp;|&nbsp; ' +
    'Net recall: ' + (tnr >= 0 ? '✅ +' : '⚠️ ') + Math.abs(tnr).toLocaleString();

  renderThreshQueries();
}}

function switchThreshTab(tab, btn) {{
  activeThreshTab = tab;
  document.getElementById('thresh-subtabs').querySelectorAll('.em-filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('thresh-items-list').innerHTML = '';
  document.getElementById('thresh-items-header').textContent = 'Select a query';
  renderThreshQueries();
}}

function renderThreshQueries() {{
  const d = THRESH_DATA[activeThreshTab];
  const search = (document.getElementById('thresh-search').value || '').toLowerCase();
  let qs = d.queries.filter(q => q.cleanQuery.toLowerCase().includes(search));
  const container = document.getElementById('thresh-query-list');

  let headers = '<th>Query</th><th style="width:60px">Count</th>';
  if (d.hasLabels) headers += '<th style="width:50px">L1</th><th style="width:50px">L2</th><th style="width:50px">L3</th>';
  else headers += '<th style="width:80px">Segment</th>';

  let html = '<table class="query-table" style="font-size:13px"><thead><tr>' + headers + '</tr></thead><tbody>';
  qs.forEach(q => {{
    const badge = activeThreshTab.includes('gain') || activeThreshTab === 'prec_gain'
      ? 'background:#d1fae5;color:#065f46' : 'background:#fee2e2;color:#991b1b';
    html += `<tr onclick="showThreshItems('${{q.query.replace(/'/g,"\\\\'")}}')">
      <td style="max-width:240px;word-break:break-word">${{q.cleanQuery}}</td>
      <td><span style="${{badge}};padding:2px 8px;border-radius:4px;font-weight:600;font-size:12px">${{q.count}}</span></td>`;
    if (d.hasLabels) html += `<td>${{q.label_1 || 0}}</td><td>${{q.label_2 || 0}}</td><td>${{q.label_3 || 0}}</td>`;
    else html += `<td style="font-size:11px">${{q.stratum || ''}}</td>`;
    html += '</tr>';
  }});
  html += '</tbody></table>';
  container.innerHTML = html;
}}

function showThreshItems(query) {{
  const d = THRESH_DATA[activeThreshTab];
  const items = d.items.filter(i => i.query === query).sort((a,b) => a.position - b.position);
  const cq = d.queries.find(q => q.query === query);
  document.getElementById('thresh-items-header').textContent =
    items.length + ' items for: ' + (cq ? cq.cleanQuery : query.split(' (')[0]);
  document.getElementById('thresh-items-list').innerHTML = items.length
    ? items.map(i => renderEmItemCard(i, true)).join('')
    : '<p style="color:#9ca3af;padding:20px;text-align:center">No items.</p>';
}}

// Boot
renderTtestTable();
renderDistribution();
renderSegmentBreakdown();
renderSigSections();
</script>
</body>
</html>"""

    # ── Write ──────────────────────────────────────────────────────────────────
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    size_mb = Path(output_path).stat().st_size / 1024 / 1024
    print(f"\n✅  Dashboard → {output_path}  ({size_mb:.1f} MB)")
    print(f"    Open: file://{Path(output_path).absolute()}")
    return output_path


# ─── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    gcs_path = args.gcs_path.rstrip('/')

    # Extract experiment ID (last path component)
    experiment_id = gcs_path.split('/')[-1]
    print(f"\n🔬 Experiment: {experiment_id}")
    print(f"   GCS path:   {gcs_path}")

    # Resolve parquet
    if args.local_parquet:
        parquet_path = args.local_parquet
        resolved_subfolder = args.subfolder or ""
        print(f"\nUsing local parquet: {parquet_path}")
    else:
        print(f"\nResolving parquet from GCS...")
        parquet_path, resolved_subfolder = resolve_parquet(
            gcs_path, args.subfolder, args.cache_dir, experiment_id
        )

    # Output path
    output_path = args.output or f"reports/{experiment_id}_recall_dashboard.html"

    # Build dashboard
    build_dashboard(parquet_path, output_path, experiment_id, gcs_path, resolved_subfolder)
    return 0


if __name__ == '__main__':
    sys.exit(main())
