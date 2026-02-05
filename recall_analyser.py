"""
Recall Analyser - Analysis tool for comparing recall between control and variant engines.

This module provides:
- Label distribution comparison between engines
- Statistical testing (paired t-tests)
- Query-level analysis of gains/losses
- Interactive HTML visualization

Usage:
    # As a library
    from retrieval_lib.recall_analyser import RecallAnalyser
    analyser = RecallAnalyser("path/to/qip_scores.parquet")
    analyser.run_analysis(output_dir="output")

    # As a CLI
    recall-analyser analyse path/to/qip_scores.parquet --output-dir output
"""

import argparse
import json
import os
import uuid
from pathlib import Path
from typing import Optional, List
import urllib.parse

def extract_polaris_host(polaris_url: str) -> str:
    """
    Extract the host from a Polaris URL for _seh parameter.
    Returns None if it's a production URL (no _seh needed).
    Returns just the host (no http://, no trailing slash) for pre-prod URLs.
    """
    if not polaris_url or str(polaris_url) == 'nan':
        return None
    
    try:
        parsed = urllib.parse.urlparse(str(polaris_url))
        host = parsed.netloc
        
        # Check if this is a pre-prod URL (contains staging/k8s patterns)
        preprod_patterns = ['cluster.k8s', '.stage', '-stage', 'westus', 'eastus', 'uswest', 'useast']
        is_preprod = any(pattern in host.lower() for pattern in preprod_patterns)
        
        if is_preprod:
            return host  # Return just the host without http:// and no trailing slash
        return None  # Production URL, no _seh needed
    except:
        return None

def build_sunlight_url(query: str, stores: int, zipcode: str, state: str, 
                       item_ids: list, engine: str = None, seh_host: str = None) -> str:
    """Build a Sunlight debug URL."""
    encoded_query = urllib.parse.quote_plus(query)
    
    # Base endpoint - _seh goes inside the endpoint URL for pre-prod
    if engine and engine != 'control':
        # Variation URL with experiment ID
        endpoint = (
            f"http://preso-usgm-wcnp.prod.walmart.com/v1/search?"
            f"prg=desktop&stores={stores}&stateOrProvinceCode={state}&zipcode={zipcode}"
            f"&ptss=l1_ranker_use_legacy_config:on;l1_ranker_unified_config:on"
            f"&trsp=l1_ranker_unified_config.expt_id:{engine}"
        )
    else:
        # Control URL
        endpoint = (
            f"http://preso-usgm-wcnp.prod.walmart.com/v1/search?"
            f"prg=desktop&stores={stores}&stateOrProvinceCode={state}&zipcode={zipcode}"
        )
    
    # Add _seh inside the endpoint for pre-prod URLs
    if seh_host:
        endpoint += f"&_seh={seh_host}"
    
    encoded_endpoint = urllib.parse.quote(endpoint, safe='')
    
    # Build item IDs parameter
    items_param = urllib.parse.quote(','.join(item_ids[:10]), safe='')
    
    url = (
        f"https://sunlight.walmart.com/debugReport?"
        f"q={encoded_query}&cat_id=&endpoint={encoded_endpoint}"
        f"&header_tenant-id=elh9ie&header_accept-language=en-US"
        f"&headers_initialized=true&recall=500"
        f"&items_affStack1_SBE_EMM={items_param}"
    )
    
    return url

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import recall_analysis_lib as ral


class RecallAnalyser:
    """Main class for Recall analysis pipeline."""

    def __init__(
        self,
        qip_scores_path: str,
        control_engine: str = 'control',
        variant_engine: Optional[str] = None,
        min_total: int = 400,
        max_total_diff: int = 5,
        top_queries: int = 100,
    ):
        """
        Initialize RecallAnalyser.

        Args:
            qip_scores_path: Path to qip_scores.parquet
            control_engine: Name of control engine
            variant_engine: Name of variant engine (auto-detected if None, or 'all' for all engines)
            min_total: Minimum total items required per query
            max_total_diff: Maximum difference in total items allowed
            top_queries: Number of top queries to show in visualization
        """
        self.qip_scores_path = qip_scores_path
        self.control_engine = control_engine
        self.variant_engine = variant_engine
        self.min_total = min_total
        self.max_total_diff = max_total_diff
        self.top_queries = top_queries

        self.df: Optional[pd.DataFrame] = None
        self.df_ctrl: Optional[pd.DataFrame] = None
        self.df_var: Optional[pd.DataFrame] = None
        self.comparison: Optional[pd.DataFrame] = None
        self.comparison_unfiltered: Optional[pd.DataFrame] = None
        self.ttest_overall: Optional[pd.DataFrame] = None
        self.ttest_unfiltered: Optional[pd.DataFrame] = None
        self.ttest_by_qlen: Optional[pd.DataFrame] = None
        self.missing_4s: Optional[pd.DataFrame] = None
        self.extra_4s: Optional[pd.DataFrame] = None
        self.missing_non4s: Optional[pd.DataFrame] = None
        self.extra_non4s: Optional[pd.DataFrame] = None

        # For multi-engine analysis
        self.all_engines: List[str] = []
        self.variant_engines: List[str] = []

    def load_data(self) -> pd.DataFrame:
        """Load QIP data and split by engine."""
        print(f"Loading QIP scores from {self.qip_scores_path}...")
        self.df = pd.read_parquet(self.qip_scores_path)
        
        self.all_engines = self.df['engine'].unique().tolist()
        print(f"Found engines: {self.all_engines}")
        
        # Identify variant engines
        self.variant_engines = [e for e in self.all_engines if e != self.control_engine]
        
        if self.variant_engine is None or self.variant_engine == 'all':
            print(f"Will analyze all variant engines: {self.variant_engines}")
        else:
            self.variant_engines = [self.variant_engine]
            print(f"Will analyze variant engine: {self.variant_engine}")
        
        self.df_ctrl = self.df[self.df['engine'] == self.control_engine]
        print(f"Control ({self.control_engine}): {len(self.df_ctrl)} items")
        
        return self.df

    def set_variant_engine(self, variant_engine: str):
        """Set the current variant engine for analysis."""
        self.variant_engine = variant_engine
        self.df_var = self.df[self.df['engine'] == variant_engine]
        print(f"Variant ({variant_engine}): {len(self.df_var)} items")
        
        # Reset computed values
        self.comparison = None
        self.comparison_unfiltered = None
        self.ttest_overall = None
        self.ttest_unfiltered = None
        self.ttest_by_qlen = None
        self.missing_4s = None
        self.extra_4s = None
        self.missing_non4s = None
        self.extra_non4s = None

    def compute_comparison(self) -> pd.DataFrame:
        """Compute label distribution comparison."""
        if self.df is None:
            self.load_data()
        
        print("Computing label distributions...")
        ctrl_dist = ral.compute_distribution_by_query(self.df_ctrl)
        var_dist = ral.compute_distribution_by_query(self.df_var)
        
        # Unfiltered comparison (for overall stats)
        self.comparison_unfiltered = ral.merge_control_variant_distributions(
            ctrl_dist, var_dist,
            min_total=0,
            max_total_diff=float('inf')
        )
        print(f"Unfiltered comparison: {len(self.comparison_unfiltered)} queries")
        
        # Filtered comparison
        self.comparison = ral.merge_control_variant_distributions(
            ctrl_dist, var_dist,
            min_total=self.min_total,
            max_total_diff=self.max_total_diff
        )
        
        print(f"Filtered comparison: {len(self.comparison)} queries after filtering")
        return self.comparison

    def run_statistical_tests(self) -> tuple:
        """Run paired t-tests overall and by query length."""
        if self.comparison is None:
            self.compute_comparison()
        
        print("Running statistical tests...")
        # Unfiltered t-test (all queries)
        self.ttest_unfiltered = ral.run_paired_ttest(self.comparison_unfiltered)
        
        # Filtered t-test
        self.ttest_overall = ral.run_paired_ttest(self.comparison)
        self.ttest_by_qlen = ral.run_ttest_by_query_length(self.comparison)
        
        return self.ttest_overall, self.ttest_by_qlen

    def find_missing_extra_items(self) -> tuple:
        """Find missing and extra items for both 4s and non-4s."""
        if self.df is None:
            self.load_data()
        
        print("Finding missing/extra items...")
        # 4-rated items
        self.missing_4s = ral.find_missing_items(self.df_ctrl, self.df_var, label_filter=4)
        self.extra_4s = ral.find_missing_items(self.df_var, self.df_ctrl, label_filter=4)
        
        # Non-4 rated items (labels 1, 2, 3)
        df_ctrl_non4 = self.df_ctrl[self.df_ctrl['label'] != 4]
        df_var_non4 = self.df_var[self.df_var['label'] != 4]
        
        self.missing_non4s = ral.find_missing_items(df_ctrl_non4, df_var_non4, label_filter=None)
        self.extra_non4s = ral.find_missing_items(df_var_non4, df_ctrl_non4, label_filter=None)
        
        print(f"Missing 4s in variant: {len(self.missing_4s)} items")
        print(f"Extra 4s in variant: {len(self.extra_4s)} items")
        print(f"Missing non-4s in variant (removed): {len(self.missing_non4s)} items")
        print(f"Extra non-4s in variant (added): {len(self.extra_non4s)} items")
        
        return self.missing_4s, self.extra_4s

    def plot_label_comparison(
        self,
        output_path: Optional[str] = None,
        show: bool = False
    ) -> plt.Figure:
        """Generate bar chart comparing label counts."""
        if self.ttest_overall is None:
            self.run_statistical_tests()
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        labels = self.ttest_overall['label'].values
        x = np.arange(len(labels))
        width = 0.35
        
        ctrl_means = self.ttest_overall['mean_ctrl'].values
        var_means = self.ttest_overall['mean_var'].values
        
        bars1 = ax.bar(x - width/2, ctrl_means, width, label=f'Control ({self.control_engine})', color='C0', alpha=0.7)
        bars2 = ax.bar(x + width/2, var_means, width, label=f'Variant ({self.variant_engine})', color='C1', alpha=0.7)
        
        ax.set_xlabel('Label')
        ax.set_ylabel('Mean Count per Query')
        ax.set_title(f'Label Distribution: {self.variant_engine} vs {self.control_engine}')
        ax.set_xticks(x)
        ax.set_xticklabels([f'Label {l}' for l in labels])
        ax.legend()
        
        # Add significance stars
        for i, row in self.ttest_overall.iterrows():
            if row['p_value'] < 0.001:
                sig = '***'
            elif row['p_value'] < 0.01:
                sig = '**'
            elif row['p_value'] < 0.05:
                sig = '*'
            else:
                sig = ''
            
            if sig:
                max_height = max(ctrl_means[i], var_means[i])
                ax.annotate(sig, xy=(i, max_height), ha='center', va='bottom', fontsize=12)
        
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
        
        if show:
            plt.show()
        
        plt.close(fig)
        return fig

    def plot_gains_by_query_length(
        self,
        output_path: Optional[str] = None,
        show: bool = False
    ) -> plt.Figure:
        """Generate chart showing gains by query length."""
        if self.ttest_by_qlen is None:
            self.run_statistical_tests()
        
        # Focus on label 4 gains
        label4_data = self.ttest_by_qlen[self.ttest_by_qlen['label'] == 4]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        qlen_groups = label4_data['query_len_group'].values
        mean_diffs = label4_data['mean_diff'].values
        
        colors = ['green' if d > 0 else 'red' for d in mean_diffs]
        bars = ax.bar(qlen_groups, mean_diffs, color=colors, alpha=0.7)
        
        ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax.set_xlabel('Query Length')
        ax.set_ylabel('Mean Difference in 4-rated Items (Variant - Control)')
        ax.set_title(f'4-rated Item Gains by Query Length: {self.variant_engine}')
        
        # Add significance indicators
        for i, (bar, row) in enumerate(zip(bars, label4_data.itertuples())):
            if row.p_value < 0.05:
                height = bar.get_height()
                offset = 0.1 if height >= 0 else -0.3
                ax.annotate(f'p={row.p_value:.3f}', 
                           xy=(bar.get_x() + bar.get_width()/2, height + offset),
                           ha='center', va='bottom' if height >= 0 else 'top',
                           fontsize=8)
        
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
        
        if show:
            plt.show()
        
        plt.close(fig)
        return fig

    def generate_html_visualization(self, output_path: str) -> str:
        """Generate interactive HTML visualization."""
        if self.comparison is None:
            self.compute_comparison()
        if self.ttest_overall is None:
            self.run_statistical_tests()
        if self.missing_4s is None:
            self.find_missing_extra_items()
        
        # Get top queries by 4s gain
        top_gain = self.comparison.sort_values('4s_gain', ascending=False).head(self.top_queries)
        top_loss = self.comparison.sort_values('4s_gain', ascending=True).head(self.top_queries)
        
        # Prepare data for HTML
        html_id = str(uuid.uuid4()).replace("-", "")
        
        # Convert DataFrames to JSON
        comparison_json = self.comparison.to_json(orient='records')
        ttest_json = self.ttest_overall.to_json(orient='records')
        ttest_unfiltered_json = self.ttest_unfiltered.to_json(orient='records')
        ttest_qlen_json = self.ttest_by_qlen.to_json(orient='records')
        
        # For HTML template
        n_queries_unfiltered = len(self.comparison_unfiltered)
        n_queries_filtered = len(self.comparison)
        min_total = self.min_total
        max_total_diff = self.max_total_diff
        
        # Query options - split into good (gained 4s) and bad (lost 4s)
        good_queries_df = self.comparison[self.comparison['4s_gain'] > 0].sort_values('4s_gain', ascending=False)
        bad_queries_df = self.comparison[self.comparison['4s_gain'] < 0].sort_values('4s_gain', ascending=True)
        
        good_queries_list = good_queries_df['contextualQuery'].head(100).tolist()
        bad_queries_list = bad_queries_df['contextualQuery'].head(100).tolist()
        top_queries_list = good_queries_list + bad_queries_list
        
        good_query_options = "".join(
            f'<option value="{q}">{q} (+{int(good_queries_df[good_queries_df.contextualQuery==q]["4s_gain"].values[0])})</option>' 
            for q in good_queries_list
        )
        bad_query_options = "".join(
            f'<option value="{q}">{q} ({int(bad_queries_df[bad_queries_df.contextualQuery==q]["4s_gain"].values[0])})</option>' 
            for q in bad_queries_list
        )
        
        # Filter items to ONLY queries in the dropdown - show all items per query
        missing_4s_filtered = self.missing_4s[
            self.missing_4s['contextualQuery'].isin(top_queries_list)
        ]
        
        extra_4s_filtered = self.extra_4s[
            self.extra_4s['contextualQuery'].isin(top_queries_list)
        ]
        
        missing_non4s_filtered = self.missing_non4s[
            self.missing_non4s['contextualQuery'].isin(top_queries_list)
        ]
        
        extra_non4s_filtered = self.extra_non4s[
            self.extra_non4s['contextualQuery'].isin(top_queries_list)
        ]
        
        missing_4s_json = missing_4s_filtered.to_json(orient='records', default_handler=str)
        extra_4s_json = extra_4s_filtered.to_json(orient='records', default_handler=str)
        missing_non4s_json = missing_non4s_filtered.to_json(orient='records', default_handler=str)
        extra_non4s_json = extra_non4s_filtered.to_json(orient='records', default_handler=str)
        
        # Build sunlight URLs for each query
        # Good queries: use gained 4s + removed non-4s (sorted by position)
        # Bad queries: use lost 4s + added non-4s (sorted by position)
        sunlight_urls = {}
        for cq in top_queries_list:
            # Get query metadata from comparison df
            query_row = self.comparison[self.comparison['contextualQuery'] == cq].iloc[0]
            query_text = query_row['query']
            is_good_query = query_row['4s_gain'] > 0
            
            # Get stores, zipcode, state, and polaris host from the original data
            ctrl_rows = self.df_ctrl[self.df_ctrl['contextualQuery'] == cq]
            if len(ctrl_rows) > 0:
                sample_row = ctrl_rows.iloc[0]
            else:
                sample_row = self.df_var[self.df_var['contextualQuery'] == cq].iloc[0]
            stores = int(sample_row['stores']) if pd.notna(sample_row['stores']) else 100
            zipcode = str(sample_row['zipcode']) if pd.notna(sample_row['zipcode']) else '72712'
            state = str(sample_row['state']) if pd.notna(sample_row['state']) else 'AR'
            
            # Extract polaris host for _seh parameter (only for pre-prod)
            seh_host = None
            if 'polarisUrl' in sample_row.index:
                seh_host = extract_polaris_host(sample_row['polarisUrl'])
            
            if is_good_query:
                # Good query: gained 4s (extra_4s) + removed non-4s (missing_non4s)
                gained_4s = extra_4s_filtered[extra_4s_filtered['contextualQuery'] == cq].copy()
                if 'position' in gained_4s.columns:
                    gained_4s = gained_4s.sort_values('position')
                gained_4s_ids = gained_4s['pg_prod_id'].dropna().head(10).tolist()
                
                removed_non4s = missing_non4s_filtered[missing_non4s_filtered['contextualQuery'] == cq].copy()
                if 'position' in removed_non4s.columns:
                    removed_non4s = removed_non4s.sort_values('position')
                removed_non4s_ids = removed_non4s['pg_prod_id'].dropna().head(10).tolist()
                
                combined_item_ids = gained_4s_ids + removed_non4s_ids
            else:
                # Bad query: lost 4s (missing_4s) + added non-4s (extra_non4s)
                lost_4s = missing_4s_filtered[missing_4s_filtered['contextualQuery'] == cq].copy()
                if 'position' in lost_4s.columns:
                    lost_4s = lost_4s.sort_values('position')
                lost_4s_ids = lost_4s['pg_prod_id'].dropna().head(10).tolist()
                
                added_non4s = extra_non4s_filtered[extra_non4s_filtered['contextualQuery'] == cq].copy()
                if 'position' in added_non4s.columns:
                    added_non4s = added_non4s.sort_values('position')
                added_non4s_ids = added_non4s['pg_prod_id'].dropna().head(10).tolist()
                
                combined_item_ids = lost_4s_ids + added_non4s_ids
            
            sunlight_urls[cq] = {
                'control_pre_config_release': build_sunlight_url(query_text, stores, zipcode, state, combined_item_ids, None, seh_host),
                'variant_pre_config_release': build_sunlight_url(query_text, stores, zipcode, state, combined_item_ids, self.variant_engine, seh_host),
                'control_config_released': build_sunlight_url(query_text, stores, zipcode, state, combined_item_ids, None, None),
                'variant_config_released': build_sunlight_url(query_text, stores, zipcode, state, combined_item_ids, self.variant_engine, None)
            }
        
        sunlight_urls_json = json.dumps(sunlight_urls)
        
        html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Recall Ranker Comparison: {self.variant_engine} vs {self.control_engine}</title>
<style>
  body {{ font-family: Arial, sans-serif; padding: 20px; max-width: 1400px; margin: 0 auto; }}
  h1, h2, h3 {{ color: #333; }}
  .summary-box {{ background: #f0f4fa; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
  .stats-table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
  .stats-table th, .stats-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
  .stats-table th {{ background: #4a90d9; color: white; }}
  .stats-table tr:nth-child(even) {{ background: #f9f9f9; }}
  .positive {{ color: green; font-weight: bold; }}
  .negative {{ color: red; font-weight: bold; }}
  .significant {{ background: #fffacd !important; }}
  .container {{ display: flex; gap: 20px; margin-top: 20px; }}
  .panel {{ flex: 1; border: 1px solid #ddd; border-radius: 8px; padding: 15px; }}
  .panel h3 {{ margin-top: 0; }}
  .item-list {{ max-height: 400px; overflow-y: auto; }}
  .item-card {{ background: #fff; border: 1px solid #eee; border-radius: 6px; padding: 10px; margin-bottom: 8px; }}
  .item-card:hover {{ box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
  .query-selector {{ margin-bottom: 15px; }}
  .query-selector select {{ width: 100%; padding: 8px; font-size: 14px; }}
  .tabs {{ display: flex; gap: 10px; margin-bottom: 15px; }}
  .tab {{ padding: 10px 20px; background: #eee; border-radius: 6px 6px 0 0; cursor: pointer; }}
  .tab.active {{ background: #4a90d9; color: white; }}
  .tab-content {{ display: none; }}
  .tab-content.active {{ display: block; }}
  .gain-bar {{ height: 20px; background: linear-gradient(90deg, #4CAF50 0%, #4CAF50 100%); border-radius: 3px; }}
  .loss-bar {{ height: 20px; background: linear-gradient(90deg, #f44336 0%, #f44336 100%); border-radius: 3px; }}
  .back-link {{ margin-bottom: 20px; }}
  .back-link a {{ color: #4a90d9; text-decoration: none; }}
  .back-link a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>

<div class="back-link">
  <a href="../index.html">← Back to Index</a>
</div>

<h1>Recall Ranker Comparison Deep Dive</h1>
<h2>{self.variant_engine} vs {self.control_engine}</h2>

<div class="summary-box">
  <h3>Summary</h3>
  <p><strong>Total queries (unfiltered):</strong> {len(self.comparison_unfiltered)}</p>
  <p><strong>Filtered queries:</strong> {len(self.comparison)}</p>
  <p><strong>Control engine:</strong> {self.control_engine}</p>
  <p><strong>Variant engine:</strong> {self.variant_engine}</p>
  <p><strong>Filters applied:</strong> min_total={self.min_total}, max_total_diff={self.max_total_diff}</p>
</div>

<h2>Overall Statistical Comparison (Unfiltered - All {n_queries_unfiltered} Queries)</h2>
<table class="stats-table" id="stats-table-unfiltered-{html_id}">
  <thead>
    <tr>
      <th>Label</th>
      <th>Mean (Control)</th>
      <th>Mean (Variant)</th>
      <th>Mean Diff</th>
      <th>t-statistic</th>
      <th>p-value</th>
    </tr>
  </thead>
  <tbody id="stats-body-unfiltered-{html_id}"></tbody>
</table>

<h2>Statistical Comparison (Filtered - {n_queries_filtered} Queries)</h2>
<p><em>Filters: min_total={min_total}, max_total_diff={max_total_diff}</em></p>
<table class="stats-table" id="stats-table-{html_id}">
  <thead>
    <tr>
      <th>Label</th>
      <th>Mean (Control)</th>
      <th>Mean (Variant)</th>
      <th>Mean Diff</th>
      <th>t-statistic</th>
      <th>p-value</th>
    </tr>
  </thead>
  <tbody id="stats-body-{html_id}"></tbody>
</table>



<h2>🟢 Good Queries (Gained 4s)</h2>
<p>Queries where variant has MORE 4-rated items than control</p>
<div class="query-selector">
  <label><strong>Select Query:</strong></label>
  <select id="good-query-select-{html_id}" onchange="updateGoodQueryView_{html_id}()">
    {good_query_options}
  </select>
</div>

<div id="good-query-details-{html_id}"></div>

<div class="container">
  <div class="panel">
    <h3>🟢 Gained 4s (New in Variant)</h3>
    <p>Items with label=4 that variant added</p>
    <div class="item-list" id="good-extra-4s-{html_id}"></div>
  </div>
  <div class="panel">
    <h3>🟢 Removed Non-4s from Control</h3>
    <p>Items with label 1,2,3 that were in control but removed by variant</p>
    <div class="item-list" id="good-removed-non4s-{html_id}"></div>
  </div>
</div>

<h2>🔴 Bad Queries (Lost 4s)</h2>
<p>Queries where variant has FEWER 4-rated items than control</p>
<div class="query-selector">
  <label><strong>Select Query:</strong></label>
  <select id="bad-query-select-{html_id}" onchange="updateBadQueryView_{html_id}()">
    {bad_query_options}
  </select>
</div>

<div id="bad-query-details-{html_id}"></div>

<div class="container">
  <div class="panel">
    <h3>🔴 Lost 4s (Were in Control)</h3>
    <p>Items with label=4 that variant lost</p>
    <div class="item-list" id="bad-missing-4s-{html_id}"></div>
  </div>
  <div class="panel">
    <h3>🔴 Added Non-4s to Variant</h3>
    <p>Items with label 1,2,3 that variant added but weren't in control</p>
    <div class="item-list" id="bad-added-non4s-{html_id}"></div>
  </div>
</div>

<h2>Top Queries by 4s Gain</h2>
<table class="stats-table">
  <thead>
    <tr>
      <th>Query</th>
      <th>4s (Control)</th>
      <th>4s (Variant)</th>
      <th>Gain</th>
      <th>Query Length</th>
    </tr>
  </thead>
  <tbody id="top-gain-{html_id}"></tbody>
</table>

<h2>Top Queries by 4s Loss</h2>
<table class="stats-table">
  <thead>
    <tr>
      <th>Query</th>
      <th>4s (Control)</th>
      <th>4s (Variant)</th>
      <th>Loss</th>
      <th>Query Length</th>
    </tr>
  </thead>
  <tbody id="top-loss-{html_id}"></tbody>
</table>

<script>
const comparison_{html_id} = {comparison_json};
const ttest_{html_id} = {ttest_json};
const ttestUnfiltered_{html_id} = {ttest_unfiltered_json};
const ttestQlen_{html_id} = {ttest_qlen_json};
const missing4s_{html_id} = {missing_4s_json};
const extra4s_{html_id} = {extra_4s_json};
const missingNon4s_{html_id} = {missing_non4s_json};
const extraNon4s_{html_id} = {extra_non4s_json};
const sunlightUrls_{html_id} = {sunlight_urls_json};

function renderStatsTableGeneric(data, tbodyId) {{
  const tbody = document.getElementById(tbodyId);
  let html = '';
  
  data.forEach(row => {{
    const isSig = row.p_value < 0.05;
    // For labels 1,2,3: negative diff is good (green), positive is bad (red)
    // For label 4: positive diff is good (green), negative is bad (red)
    let diffClass = '';
    if (isSig) {{
      if (row.label === 4) {{
        diffClass = row.mean_diff > 0 ? 'positive' : (row.mean_diff < 0 ? 'negative' : '');
      }} else {{
        diffClass = row.mean_diff < 0 ? 'positive' : (row.mean_diff > 0 ? 'negative' : '');
      }}
    }}
    const sigClass = isSig ? 'significant' : '';
    
    html += `<tr class="${{sigClass}}">
      <td>Label ${{row.label}}</td>
      <td>${{row.mean_ctrl.toFixed(2)}}</td>
      <td>${{row.mean_var.toFixed(2)}}</td>
      <td class="${{diffClass}}">${{row.mean_diff > 0 ? '+' : ''}}${{row.mean_diff.toFixed(2)}}</td>
      <td>${{row.t_stat.toFixed(3)}}</td>
      <td>${{row.p_value.toFixed(4)}}</td>
    </tr>`;
  }});
  
  tbody.innerHTML = html;
}}

function renderStatsTable_{html_id}() {{
  renderStatsTableGeneric(ttest_{html_id}, 'stats-body-{html_id}');
  renderStatsTableGeneric(ttestUnfiltered_{html_id}, 'stats-body-unfiltered-{html_id}');
}}

function renderQueryDetails(queryData, containerId, sunlightLinks) {{
  if (queryData) {{
    const linksHtml = sunlightLinks ? `
      <div class="sunlight-links-section">
        <div class="sunlight-row">
          <strong>Pre-config release:</strong>
          <a href="${{sunlightLinks.control_pre_config_release}}" target="_blank" class="sunlight-btn control-btn">Control</a>
          <a href="${{sunlightLinks.variant_pre_config_release}}" target="_blank" class="sunlight-btn variant-btn">Variant</a>
        </div>
        <div class="sunlight-row">
          <strong>Config released:</strong>
          <a href="${{sunlightLinks.control_config_released}}" target="_blank" class="sunlight-btn control-btn">Control</a>
          <a href="${{sunlightLinks.variant_config_released}}" target="_blank" class="sunlight-btn variant-btn">Variant</a>
        </div>
      </div>
    ` : '';
    
    const detailsHtml = `
      <div class="summary-box">
        <p><strong>Query:</strong> ${{queryData.query}}</p>
        <p><strong>Query Length:</strong> ${{queryData.query_len}} words</p>
        ${{linksHtml}}
        <table class="stats-table">
          <tr><th>Label</th><th>Control</th><th>Variant</th><th>Diff</th></tr>
          <tr><td>1</td><td>${{queryData.count_1_ctrl}}</td><td>${{queryData.count_1_var}}</td><td class="${{queryData['1s_gain'] < 0 ? 'positive' : (queryData['1s_gain'] > 0 ? 'negative' : '')}}">${{queryData['1s_gain']}}</td></tr>
          <tr><td>2</td><td>${{queryData.count_2_ctrl}}</td><td>${{queryData.count_2_var}}</td><td class="${{queryData['2s_gain'] < 0 ? 'positive' : (queryData['2s_gain'] > 0 ? 'negative' : '')}}">${{queryData['2s_gain']}}</td></tr>
          <tr><td>3</td><td>${{queryData.count_3_ctrl}}</td><td>${{queryData.count_3_var}}</td><td class="${{queryData['3s_gain'] < 0 ? 'positive' : (queryData['3s_gain'] > 0 ? 'negative' : '')}}">${{queryData['3s_gain']}}</td></tr>
          <tr><td>4</td><td>${{queryData.count_4_ctrl}}</td><td>${{queryData.count_4_var}}</td><td class="${{queryData['4s_gain'] > 0 ? 'positive' : (queryData['4s_gain'] < 0 ? 'negative' : '')}}">${{queryData['4s_gain']}}</td></tr>
        </table>
      </div>
    `;
    document.getElementById(containerId).innerHTML = detailsHtml;
  }}
}}

function updateGoodQueryView_{html_id}() {{
  const query = document.getElementById('good-query-select-{html_id}').value;
  const queryData = comparison_{html_id}.find(r => r.contextualQuery === query);
  
  renderQueryDetails(queryData, 'good-query-details-{html_id}', sunlightUrls_{html_id}[query]);
  
  const extra4s = extra4s_{html_id}.filter(r => r.contextualQuery === query);
  const removedNon4s = missingNon4s_{html_id}.filter(r => r.contextualQuery === query);
  
  document.getElementById('good-extra-4s-{html_id}').innerHTML = renderItemList(extra4s);
  document.getElementById('good-removed-non4s-{html_id}').innerHTML = renderItemList(removedNon4s);
}}

function updateBadQueryView_{html_id}() {{
  const query = document.getElementById('bad-query-select-{html_id}').value;
  const queryData = comparison_{html_id}.find(r => r.contextualQuery === query);
  
  renderQueryDetails(queryData, 'bad-query-details-{html_id}', sunlightUrls_{html_id}[query]);
  
  const missing4s = missing4s_{html_id}.filter(r => r.contextualQuery === query);
  const addedNon4s = extraNon4s_{html_id}.filter(r => r.contextualQuery === query);
  
  document.getElementById('bad-missing-4s-{html_id}').innerHTML = renderItemList(missing4s);
  document.getElementById('bad-added-non4s-{html_id}').innerHTML = renderItemList(addedNon4s);
}}

function renderItemList(items) {{
  if (items.length === 0) return '<p>No items found</p>';
  
  // Sort by position and show all items
  const sorted = [...items].sort((a, b) => (a.position || 999) - (b.position || 999));
  
  return `<p><strong>Total: ${{sorted.length}} items</strong></p>` + sorted.map(item => `
    <div class="item-card">
      <strong>Product ID:</strong> ${{item.pg_prod_id || 'N/A'}}<br>
      <strong>Label:</strong> ${{item.label}}<br>
      <strong>Position:</strong> ${{item.position || 'N/A'}}
    </div>
  `).join('');
}}

function renderTopGain_{html_id}() {{
  const tbody = document.getElementById('top-gain-{html_id}');
  const sorted = [...comparison_{html_id}].sort((a, b) => b['4s_gain'] - a['4s_gain']).slice(0, 20);
  
  tbody.innerHTML = sorted.map(row => `
    <tr>
      <td>${{row.query}}</td>
      <td>${{row.count_4_ctrl}}</td>
      <td>${{row.count_4_var}}</td>
      <td class="positive">+${{row['4s_gain']}}</td>
      <td>${{row.query_len}}</td>
    </tr>
  `).join('');
}}

function renderTopLoss_{html_id}() {{
  const tbody = document.getElementById('top-loss-{html_id}');
  const sorted = [...comparison_{html_id}].sort((a, b) => a['4s_gain'] - b['4s_gain']).slice(0, 20);
  
  tbody.innerHTML = sorted.map(row => `
    <tr>
      <td>${{row.query}}</td>
      <td>${{row.count_4_ctrl}}</td>
      <td>${{row.count_4_var}}</td>
      <td class="negative">${{row['4s_gain']}}</td>
      <td>${{row.query_len}}</td>
    </tr>
  `).join('');
}}

// Initialize
renderStatsTable_{html_id}();
renderTopGain_{html_id}();
renderTopLoss_{html_id}();
updateGoodQueryView_{html_id}();
updateBadQueryView_{html_id}();
</script>

</body>
</html>
"""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"HTML visualization saved to {output_path}")
        return output_path

    def _generate_index_html(self, output_dir: str, results: dict) -> str:
        """Generate an index HTML page linking to all variant analyses."""
        
        html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Recall Ranker Comparison Index</title>
<style>
  body {{ font-family: Arial, sans-serif; padding: 20px; max-width: 1200px; margin: 0 auto; }}
  h1 {{ color: #333; }}
  .summary-box {{ background: #f0f4fa; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
  .engine-list {{ list-style: none; padding: 0; }}
  .engine-card {{ 
    background: #fff; 
    border: 1px solid #ddd; 
    border-radius: 8px; 
    padding: 20px; 
    margin-bottom: 15px;
    transition: box-shadow 0.2s;
  }}
  .engine-card:hover {{ box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
  .engine-card h3 {{ margin-top: 0; color: #4a90d9; }}
  .engine-card a {{ 
    display: inline-block;
    background: #4a90d9;
    color: white;
    padding: 8px 16px;
    border-radius: 4px;
    text-decoration: none;
    margin-right: 10px;
  }}
  .engine-card a:hover {{ background: #357abd; }}
  .stats {{ color: #666; margin: 10px 0; }}
</style>
</head>
<body>

<h1>Recall Ranker Comparison Index</h1>

<div class="summary-box">
  <h3>Summary</h3>
  <p><strong>Control Engine:</strong> {self.control_engine}</p>
  <p><strong>Number of Variant Engines:</strong> {len(results)}</p>
  <p><strong>Data Source:</strong> {self.qip_scores_path}</p>
</div>

<h2>Variant Engine Reports</h2>
<ul class="engine-list">
"""
        
        for variant_engine, result in results.items():
            variant_dir = variant_engine.replace("/", "_")
            html += f"""
  <li class="engine-card">
    <h3>{variant_engine}</h3>
    <p class="stats">Queries analyzed: {result['n_queries']}</p>
    <a href="{variant_dir}/recall_ranker_comparison.html">View Report</a>
    <a href="{variant_dir}/comparison.csv">Download CSV</a>
    <a href="{variant_dir}/label_comparison.png">Label Chart</a>
  </li>
"""
        
        html += """
</ul>

</body>
</html>
"""
        
        index_path = os.path.join(output_dir, "index.html")
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"Index HTML saved to {index_path}")
        return index_path

    def run_analysis(self, output_dir: str, show_plots: bool = False) -> dict:
        """
        Run full analysis pipeline.

        Args:
            output_dir: Directory to save outputs
            show_plots: If True, display plots interactively

        Returns:
            Dictionary with analysis results and output paths
        """
        os.makedirs(output_dir, exist_ok=True)

        print("=" * 60)
        print("Recall Analysis Pipeline")
        print("=" * 60)

        # Step 1: Load data
        print("\n[1/5] Loading data...")
        self.load_data()

        results = {}

        # Analyze each variant engine
        for variant_engine in self.variant_engines:
            print("\n" + "=" * 60)
            print(f"Analyzing: {variant_engine} vs {self.control_engine}")
            print("=" * 60)

            self.set_variant_engine(variant_engine)

            # Create subdirectory for this variant
            variant_dir = os.path.join(output_dir, variant_engine.replace("/", "_"))
            os.makedirs(variant_dir, exist_ok=True)

            # Step 2: Compute comparison
            print("\n[2/5] Computing comparison...")
            self.compute_comparison()

            # Step 3: Statistical tests
            print("\n[3/5] Running statistical tests...")
            self.run_statistical_tests()

            # Step 4: Find missing/extra items
            print("\n[4/5] Finding missing/extra items...")
            self.find_missing_extra_items()

            # Step 5: Generate outputs
            print("\n[5/5] Generating outputs...")

            # Save comparison
            comparison_path = os.path.join(variant_dir, "comparison.csv")
            self.comparison.to_csv(comparison_path, index=False)
            print(f"Comparison saved to {comparison_path}")

            # Save t-test results
            ttest_path = os.path.join(variant_dir, "ttest_overall.csv")
            self.ttest_overall.to_csv(ttest_path, index=False)
            print(f"T-test results saved to {ttest_path}")

            ttest_qlen_path = os.path.join(variant_dir, "ttest_by_query_length.csv")
            self.ttest_by_qlen.to_csv(ttest_qlen_path, index=False)
            print(f"T-test by query length saved to {ttest_qlen_path}")

            # Generate plots
            self.plot_label_comparison(
                os.path.join(variant_dir, "label_comparison.png"),
                show=show_plots
            )
            self.plot_gains_by_query_length(
                os.path.join(variant_dir, "gains_by_query_length.png"),
                show=show_plots
            )

            # Generate HTML
            html_path = os.path.join(variant_dir, "recall_ranker_comparison.html")
            self.generate_html_visualization(html_path)

            results[variant_engine] = {
                "output_dir": variant_dir,
                "html_path": html_path,
                "comparison_path": comparison_path,
                "n_queries": len(self.comparison),
            }

        # Generate index HTML with links to all variant analyses
        self._generate_index_html(output_dir, results)

        print("\n" + "=" * 60)
        print("Analysis complete!")
        print(f"Generated reports for {len(self.variant_engines)} variant engine(s)")
        print("=" * 60)

        return results


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Recall Analyser - Compare recall between control and variant engines",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Analyse command
    analyse_parser = subparsers.add_parser("analyse", help="Run full analysis pipeline")
    analyse_parser.add_argument("qip_scores", type=str, help="Path to qip_scores.parquet")
    analyse_parser.add_argument("--output-dir", type=str, default="./recall_analysis_output", help="Output directory")
    analyse_parser.add_argument("--control-engine", type=str, default="control", help="Control engine name")
    analyse_parser.add_argument("--variant-engine", type=str, default="all", help="Variant engine name ('all' for all engines)")
    analyse_parser.add_argument("--min-total", type=int, default=400, help="Minimum total items per query")
    analyse_parser.add_argument("--max-total-diff", type=int, default=5, help="Maximum total items difference allowed")
    analyse_parser.add_argument("--show-plots", action="store_true", help="Display plots interactively")
    analyse_parser.add_argument("--top-queries", type=int, default=100, help="Number of top queries for visualization")

    # Visualize command
    viz_parser = subparsers.add_parser("visualize", help="Generate only HTML visualization")
    viz_parser.add_argument("qip_scores", type=str, help="Path to qip_scores.parquet")
    viz_parser.add_argument("--output", type=str, default="./recall_analysis.html", help="Output HTML path")
    viz_parser.add_argument("--control-engine", type=str, default="control", help="Control engine name")
    viz_parser.add_argument("--variant-engine", type=str, default="all", help="Variant engine name ('all' for all engines)")
    viz_parser.add_argument("--min-total", type=int, default=400, help="Minimum total items per query")
    viz_parser.add_argument("--max-total-diff", type=int, default=5, help="Maximum total items difference")
    viz_parser.add_argument("--top-queries", type=int, default=100, help="Number of top queries")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    if args.command == "analyse":
        analyser = RecallAnalyser(
            qip_scores_path=args.qip_scores,
            control_engine=args.control_engine,
            variant_engine=args.variant_engine,
            min_total=args.min_total,
            max_total_diff=args.max_total_diff,
            top_queries=args.top_queries,
        )
        analyser.run_analysis(args.output_dir, show_plots=args.show_plots)

    elif args.command == "visualize":
        analyser = RecallAnalyser(
            qip_scores_path=args.qip_scores,
            control_engine=args.control_engine,
            variant_engine=args.variant_engine,
            min_total=args.min_total,
            max_total_diff=args.max_total_diff,
            top_queries=args.top_queries,
        )
        analyser.run_analysis(os.path.dirname(args.output) or ".", show_plots=False)


if __name__ == "__main__":
    main()