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
  6. Generates a single self-contained HTML dashboard with:
     - Statistical significance table
     - Query distribution breakdown
     - Per-label significant-change tables (top driving + counteracting tabs)
     - Per-query item grid grouped by label section (Good/Bad/Concern signal)
     - Sunlight deep-links on every item card
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


def resolve_parquet(gcs_path: str, subfolder: str | None, cache_dir: str, experiment_id: str) -> str:
    """
    Find and download qip_scores.parquet. Tries (in order):
      1. --subfolder argument if given
      2. impacted/
      3. sample-1000/
      4. root of gcs_path
    Returns local file path.
    """
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    local_path = str(Path(cache_dir) / f"{experiment_id}_qip_scores.parquet")

    if Path(local_path).exists():
        size_mb = Path(local_path).stat().st_size / 1024 / 1024
        print(f"  Using cached parquet: {local_path} ({size_mb:.1f} MB)")
        return local_path

    candidates = []
    if subfolder:
        candidates = [f"{gcs_path.rstrip('/')}/{subfolder}/qip_scores.parquet"]
    else:
        candidates = [
            f"{gcs_path.rstrip('/')}/impacted/qip_scores.parquet",
            f"{gcs_path.rstrip('/')}/sample-1000/qip_scores.parquet",
            f"{gcs_path.rstrip('/')}/qip_scores.parquet",
        ]

    for uri in candidates:
        if gcs_exists(uri):
            gcs_download(uri, local_path)
            return local_path

    raise FileNotFoundError(
        f"Could not find qip_scores.parquet in any of:\n" +
        "\n".join(f"  {c}" for c in candidates)
    )


# ─── Query clean helper ────────────────────────────────────────────────────────

def clean_query(q: str) -> str:
    return re.sub(r'\s*\((?:facet|stores|zipcode).*?\)', '', str(q)).strip()


# ─── Main pipeline ─────────────────────────────────────────────────────────────

def build_dashboard(parquet_path: str, output_path: str, experiment_id: str, gcs_path: str = ""):

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

    # ── Common queries ─────────────────────────────────────────────────────────
    df_imp['cleanQuery'] = df_imp['contextualQuery'].apply(clean_query)
    q_ctrl = set(df_imp[df_imp['engine'] == CONTROL_ENGINE]['contextualQuery'].unique())
    q_var  = set(df_imp[df_imp['engine'] == VARIANT_ENGINE]['contextualQuery'].unique())
    common = q_ctrl & q_var
    print(f"  Queries in both engines: {len(common)}")
    df_c = df_imp[df_imp['contextualQuery'].isin(common)].copy()

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
                         "control": ctrl_cnt, "variant": var_cnt, "difference": diff})
        rows.sort(key=lambda r: r["difference"], reverse=True)
        label_query_tables[label] = rows

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

    ttest_json         = json.dumps(ttest)
    label_totals_json  = json.dumps({str(k): v for k, v in label_totals.items()})
    distribution_json  = json.dumps(distribution_data)
    label_tables_json  = json.dumps({str(k): v for k, v in label_query_tables.items()})
    query_items_json   = json.dumps(query_items)
    sig_sections_json  = json.dumps(sig_sections_info)

    # ── HTML ───────────────────────────────────────────────────────────────────
    print("Generating HTML...")
    has_sunlight = all(c in df_c.columns for c in ['stores', 'state', 'zipcode'])
    sig_label_list = ', '.join(f'{l}★' for l in sig_labels) if sig_labels else 'None'

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

</div><!-- /container -->

<script>
const DISTRIBUTION  = {distribution_json};
const TTEST         = {ttest_json};
const LABEL_TOTALS  = {label_totals_json};
const LABEL_TABLES  = {label_tables_json};
const QUERY_ITEMS   = {query_items_json};
const SIG_SECTIONS  = {sig_sections_json};
const HAS_SUNLIGHT  = {'true' if has_sunlight else 'false'};

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

function buildSunlightUrl(item, cleanQuery) {{
  if (!HAS_SUNLIGHT || !item.stores) return null;
  const q = cleanQuery.trim().replace(/\\s+/g, '+');
  const ep = 'preso-usgm-wcnp.prod.walmart.com/v1/search?prg=desktop'
    + '&stores=' + item.stores + '&stateOrProvinceCode=' + item.state + '&zipcode=' + item.zipcode;
  return 'https://sunlight.walmart.com/debugReport?q=' + q
    + '&cat_id=&endpoint=' + encodeURIComponent(ep)
    + '&items_affStack1_SBE_EMM=' + item.item_id;
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
let activeRow = null;

function renderSigSections() {{
  const container = document.getElementById('sig-sections-container');
  if (!SIG_SECTIONS.length) {{
    container.innerHTML = '<p style="color:#6b7280">No labels reached p &lt; 0.05.</p>';
    return;
  }}
  container.innerHTML = SIG_SECTIONS.map(s => {{
    activeSigTab[s.label] = 'top';
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
        <div class="sig-tabs">
          <button class="sig-tab active" onclick="switchTab(${{s.label}},'top',this)">Top driving queries</button>
          <button class="sig-tab" onclick="switchTab(${{s.label}},'counter',this)">Counteracting queries</button>
        </div>
        <div class="query-table-wrap">
          <table>
            <thead><tr><th>Query</th><th style="text-align:right;width:100px">Control</th>
              <th style="text-align:right;width:100px">Variant</th>
              <th style="text-align:right;width:100px">Difference</th></tr></thead>
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

function renderSigTable(label) {{
  const tbody = document.getElementById('sig-tbody-' + label);
  let rows = LABEL_TABLES[String(label)] || [];
  const overall = SIG_SECTIONS.find(s => s.label === label).diff;
  let display;
  if (activeSigTab[label] === 'top') {{
    display = overall > 0 ? rows.filter(r => r.difference > 0).slice(0,20)
                          : rows.filter(r => r.difference < 0).slice(-20).reverse();
  }} else {{
    display = overall > 0 ? rows.filter(r => r.difference < 0).slice(-20).reverse()
                          : rows.filter(r => r.difference > 0).slice(0,20);
  }}
  if (!display.length) {{ tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:#9ca3af;padding:20px">No queries in this category</td></tr>'; return; }}
  tbody.innerHTML = display.map(r => `
    <tr onclick="showItemGrid('${{r.rawQuery.replace(/'/g,"\\\\'")}}','${{r.query.replace(/'/g,"\\\\'")}}',this)">
      <td style="max-width:400px;word-break:break-word">${{r.query}}</td>
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
  const sunUrl = buildSunlightUrl(item, cleanQuery);
  const clickAttr = sunUrl ? `onclick="window.open('${{sunUrl}}','_blank')" style="border-color:${{borderCol}};cursor:pointer;" title="Open in Sunlight"` : `style="border-color:${{borderCol}};"`;
  const sunBadge = sunUrl ? `<span style="font-size:11px;background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe;border-radius:4px;padding:1px 7px;white-space:nowrap">🔗 Sunlight</span>` : '';
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

// Boot
renderTtestTable();
renderDistribution();
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
        print(f"\nUsing local parquet: {parquet_path}")
    else:
        print(f"\nResolving parquet from GCS...")
        parquet_path = resolve_parquet(
            gcs_path, args.subfolder, args.cache_dir, experiment_id
        )

    # Output path
    output_path = args.output or f"reports/{experiment_id}_recall_dashboard.html"

    # Build dashboard
    build_dashboard(parquet_path, output_path, experiment_id, gcs_path)
    return 0


if __name__ == '__main__':
    sys.exit(main())
