#!/usr/bin/env python3
"""
Generate HTML Report for 4s Added Analysis.

Creates an interactive HTML report showing queries where variant added 4-rated items,
with attribute matching analysis and insights.

Usage:
    python generate_4s_report.py
    python generate_4s_report.py --output ./reports/4s_added_report.html
"""

import argparse
import pandas as pd
import json
import uuid
from pathlib import Path
from datetime import datetime


def parse_intent_json(intent_str, top_n=5):
    """Parse intent JSON and return formatted string with top N values."""
    if pd.isna(intent_str) or not intent_str or intent_str == '[]':
        return None

    try:
        intents = json.loads(intent_str)
        if not intents or len(intents) == 0:
            return None

        formatted = []
        for intent in intents[:top_n]:
            value = intent.get('value', '')
            score = intent.get('score', 0)
            formatted.append(f"{value} ({score:.2f})")

        result = ', '.join(formatted)
        if len(intents) > top_n:
            result += f" +{len(intents) - top_n} more"

        return result
    except:
        return None


def generate_html_report(
    pairs_df: pd.DataFrame,
    summary_stats: dict,
    insights: list,
    output_path: str,
    control_engine: str,
    variant_engine: str,
    rating_distributions: dict = None
):
    """Generate interactive HTML report for 4s added."""

    html_id = str(uuid.uuid4()).replace("-", "")

    # Filter to 4s_gained and non4_removed
    items_of_interest = pairs_df[pairs_df['change_type'].isin(['4_gained', 'non4_removed'])].copy()

    # Get unique queries
    query_col = 'contextualQuery' if 'contextualQuery' in items_of_interest.columns else 'query'

    # Keep only columns we need for the HTML (reduce file size!)
    cols_to_keep = [
        query_col, 'pg_prod_id', 'change_type',
        'label_ctrl', 'label_var',  # Keep labels to show rating for non4_removed
        # All intent types from Perceive API
        'product_type_intent', 'brand_intent', 'color_intent', 'gender_intent',
        'size_intent', 'material_intent', 'pattern_intent', 'style_intent',
        'age_group_intent', 'occasion_intent', 'category_intent',
        # Item attributes (from joined item_attributes.jsonl)
        'item_title', 'item_product_type', 'item_brand', 'item_color', 'item_gender',
        # Legacy columns (if they exist from old pipeline)
        'title_var', 'product_type_var', 'brand_var', 'color_var', 'gender_var',
        'title_ctrl', 'product_type_ctrl', 'brand_ctrl', 'color_ctrl', 'gender_ctrl',
        # Matching scores
        'overall_match', 'pt_exact_match', 'brand_exact_match', 'title_match'
    ]
    # Only keep columns that actually exist
    cols_to_keep = [col for col in cols_to_keep if col in items_of_interest.columns]
    items_of_interest = items_of_interest[cols_to_keep].copy()

    # Separate for statistics (4s_gained only for query summary)
    fours_gained = items_of_interest[items_of_interest['change_type'] == '4_gained'].copy()

    query_summary = fours_gained.groupby(query_col).agg({
        'pg_prod_id': 'count',
        'overall_match': 'mean',
        'pt_exact_match': 'mean',
        'brand_exact_match': 'mean',
        'title_match': 'mean'
    }).rename(columns={
        'pg_prod_id': 'count_4s_added',
        'overall_match': 'avg_match',
        'pt_exact_match': 'avg_pt_match',
        'brand_exact_match': 'avg_brand_match',
        'title_match': 'avg_title_match'
    }).sort_values('count_4s_added', ascending=False)

    # Add query intents and net 4s gain to summary
    intent_fields = [
        'product_type_intent', 'brand_intent', 'color_intent', 'gender_intent',
        'size_intent', 'material_intent', 'pattern_intent', 'style_intent',
        'age_group_intent', 'occasion_intent', 'category_intent'
    ]

    for idx in query_summary.index:
        sample = fours_gained[fours_gained[query_col] == idx].iloc[0]

        # Add ALL intent types as raw JSON (JavaScript will parse them for scoring)
        for intent_field in intent_fields:
            query_summary.loc[idx, intent_field] = sample.get(intent_field, '[]')

        # Calculate net 4s gain from rating distribution
        if rating_distributions and idx in rating_distributions:
            dist = rating_distributions[idx]
            control_4s = dist.get('control', {}).get(4, 0) if dist.get('control') else 0
            variant_4s = dist.get('variant', {}).get(4, 0) if dist.get('variant') else 0
            net_gain = variant_4s - control_4s
            query_summary.loc[idx, 'net_4s_gain'] = net_gain
        else:
            # Fallback: use count of new items added
            query_summary.loc[idx, 'net_4s_gain'] = query_summary.loc[idx, 'count_4s_added']

    # Sort by net 4s gain (highest first)
    query_summary = query_summary.sort_values('net_4s_gain', ascending=False)

    # Create query dropdown options (show all queries, not just top 50)
    query_options = ""
    for query in query_summary.index:
        count = int(query_summary.loc[query, 'count_4s_added'])
        net_gain = int(query_summary.loc[query, 'net_4s_gain'])
        avg_match = query_summary.loc[query, 'avg_match']

        # Handle NaN values
        if pd.isna(avg_match):
            match_str = "N/A"
        else:
            match_str = f"{avg_match:.0%}"

        query_options += f'<option value="{query}">{query[:60]}... (+{net_gain} net 4s, {count} new, {match_str} match)</option>\n'

    # Convert data to JSON (include both 4s_gained and non4_removed)
    items_json = items_of_interest.to_json(orient='records', default_handler=str)
    query_summary_json = query_summary.reset_index().to_json(orient='records')
    rating_distributions_json = json.dumps(rating_distributions or {})

    # Get overall stats
    if '4_gained' in summary_stats:
        stats_4g = summary_stats['4_gained']
    else:
        stats_4g = {
            'count': len(fours_gained),
            'avg_overall_match': fours_gained['overall_match'].mean() if 'overall_match' in fours_gained.columns else 0,
            'avg_pt_match': fours_gained['pt_exact_match'].mean() if 'pt_exact_match' in fours_gained.columns else 0,
            'avg_brand_match': fours_gained['brand_exact_match'].mean() if 'brand_exact_match' in fours_gained.columns else 0,
            'avg_title_match': fours_gained['title_match'].mean() if 'title_match' in fours_gained.columns else 0,
        }

    # Format insights
    insights_html = ""
    for insight in insights:
        insights_html += f"<li>{insight}</li>\n"

    # Generate HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>4s Added Analysis: {variant_engine} vs {control_engine}</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
    padding: 20px;
    max-width: 1400px;
    margin: 0 auto;
    background: #f5f7fa;
  }}

  h1, h2, h3 {{
    color: #2c3e50;
  }}

  .header {{
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 30px;
    border-radius: 12px;
    margin-bottom: 30px;
    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
  }}

  .header h1 {{
    margin: 0 0 10px 0;
    color: white;
  }}

  .header p {{
    margin: 5px 0;
    opacity: 0.95;
  }}

  .summary-box {{
    background: white;
    padding: 25px;
    border-radius: 12px;
    margin-bottom: 25px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
  }}

  .summary-box h2 {{
    margin-top: 0;
    border-bottom: 3px solid #667eea;
    padding-bottom: 10px;
  }}

  .stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 15px;
    margin-top: 20px;
  }}

  .stat-card {{
    background: #f8f9fa;
    padding: 15px;
    border-radius: 8px;
    border-left: 4px solid #667eea;
  }}

  .stat-card .label {{
    font-size: 12px;
    color: #6c757d;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }}

  .stat-card .value {{
    font-size: 28px;
    font-weight: bold;
    color: #2c3e50;
    margin-top: 5px;
  }}

  .insights-list {{
    list-style: none;
    padding: 0;
  }}

  .insights-list li {{
    background: #e8f5e9;
    padding: 12px 15px;
    margin: 8px 0;
    border-radius: 6px;
    border-left: 4px solid #4caf50;
  }}

  .query-selector {{
    background: white;
    padding: 25px;
    border-radius: 12px;
    margin-bottom: 25px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
  }}

  select {{
    width: 100%;
    padding: 12px;
    font-size: 14px;
    border: 2px solid #e0e0e0;
    border-radius: 6px;
    background: white;
    cursor: pointer;
    margin-top: 10px;
  }}

  select:focus {{
    outline: none;
    border-color: #667eea;
  }}

  .query-details {{
    background: white;
    padding: 25px;
    border-radius: 12px;
    margin-bottom: 25px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
  }}

  .intent-section {{
    background: #f8f9fa;
    padding: 15px;
    border-radius: 8px;
    margin: 15px 0;
  }}

  .intent-section h4 {{
    margin-top: 0;
    color: #667eea;
  }}

  .intent-item {{
    padding: 5px 0;
    color: #495057;
  }}

  .intent-label {{
    font-weight: 600;
    min-width: 120px;
    display: inline-block;
  }}

  .items-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
    gap: 15px;
    margin-top: 15px;
    max-height: 600px;
    overflow-y: auto;
    padding-right: 10px;
  }}

  .item-card {{
    background: white;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    padding: 15px;
    transition: all 0.2s;
  }}

  .item-card:hover {{
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    transform: translateY(-2px);
  }}

  .item-id {{
    font-family: monospace;
    color: #667eea;
    font-weight: bold;
    margin-bottom: 10px;
  }}

  .item-title {{
    color: #2c3e50;
    font-size: 14px;
    line-height: 1.4;
    margin-bottom: 10px;
  }}

  .item-attributes {{
    font-size: 13px;
    color: #6c757d;
    line-height: 1.6;
  }}

  .match-scores {{
    background: #e8f5e9;
    padding: 10px;
    border-radius: 6px;
    margin-top: 10px;
    font-size: 12px;
  }}

  .match-score {{
    display: inline-block;
    margin-right: 15px;
  }}

  .score-high {{ color: #4caf50; font-weight: bold; }}
  .score-medium {{ color: #ff9800; }}
  .score-low {{ color: #9e9e9e; }}

  .rating-distribution {{
    background: #f8f9fa;
    padding: 15px;
    border-radius: 8px;
    margin: 15px 0;
  }}

  .rating-distribution h4 {{
    margin-top: 0;
    color: #667eea;
  }}

  .rating-table {{
    width: 100%;
    border-collapse: collapse;
    background: white;
    border-radius: 6px;
    overflow: hidden;
    margin-top: 10px;
  }}

  .rating-table th {{
    background: #667eea;
    color: white;
    padding: 12px;
    text-align: center;
    font-weight: 600;
  }}

  .rating-table td {{
    padding: 10px;
    text-align: center;
    border-bottom: 1px solid #e0e0e0;
  }}

  .rating-table tr:last-child td {{
    border-bottom: none;
  }}

  .rating-table .label-col {{
    background: #f1f3f5;
    font-weight: 600;
  }}

  .diff-positive {{
    color: #10b981;
    font-weight: 600;
  }}

  .diff-negative {{
    color: #ef4444;
    font-weight: 600;
  }}

  .diff-neutral {{
    color: #6c757d;
  }}

  .section-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 15px;
  }}

  .badge {{
    background: #667eea;
    color: white;
    padding: 4px 12px;
    border-radius: 12px;
    font-size: 14px;
    font-weight: 600;
  }}
</style>
</head>
<body>

<div class="header">
  <h1>🎯 4s Added Analysis</h1>
  <p><strong>Variant:</strong> {variant_engine} vs <strong>Control:</strong> {control_engine}</p>
  <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
</div>

<div class="summary-box">
  <h2>📊 Overall Summary</h2>

  <div class="stats-grid">
    <div class="stat-card">
      <div class="label">Total 4s Added</div>
      <div class="value">{stats_4g['count']:,}</div>
    </div>
    <div class="stat-card">
      <div class="label">Avg Overall Match</div>
      <div class="value">{stats_4g['avg_overall_match']:.0%}</div>
    </div>
    <div class="stat-card">
      <div class="label">Avg Product Type Match</div>
      <div class="value">{stats_4g['avg_pt_match']:.0%}</div>
    </div>
    <div class="stat-card">
      <div class="label">Avg Brand Match</div>
      <div class="value">{stats_4g['avg_brand_match']:.0%}</div>
    </div>
    <div class="stat-card">
      <div class="label">Avg Title Match</div>
      <div class="value">{stats_4g['avg_title_match']:.0%}</div>
    </div>
    <div class="stat-card">
      <div class="label">Unique Queries</div>
      <div class="value">{len(query_summary)}</div>
    </div>
  </div>

  <h3 style="margin-top: 30px;">💡 Key Insights</h3>
  <ul class="insights-list">
    {insights_html}
  </ul>
</div>

<div class="query-selector">
  <h2>🔍 Explore Queries</h2>
  <p>Select a query to see detailed analysis of 4s added by variant</p>
  <select id="query-select-{html_id}" onchange="updateQueryView_{html_id}()">
    <option value="">-- Select a Query --</option>
    {query_options}
  </select>
</div>

<div id="query-details-{html_id}" class="query-details" style="display: none;">
  <div class="section-header">
    <h2 id="query-title-{html_id}"></h2>
    <span class="badge" id="query-count-{html_id}"></span>
  </div>

  <div class="intent-section">
    <h4>🎯 Query Intents (from Perceive API)</h4>
    <div id="query-intents-{html_id}"></div>
  </div>

  <div class="rating-distribution">
    <h4>📊 Rating Distribution (Control vs Variant)</h4>
    <div id="rating-dist-table-{html_id}"></div>
  </div>

  <h3>✨ 4s Added by Variant & 🔻 Non-4s Removed from Control</h3>
  <div id="items-list-{html_id}" class="items-grid"></div>
</div>

<script>
const itemsData_{html_id} = {items_json};
const querySummary_{html_id} = {query_summary_json};
const ratingDistributions_{html_id} = {rating_distributions_json};

function updateQueryView_{html_id}() {{
  const select = document.getElementById('query-select-{html_id}');
  const selectedQuery = select.value;

  if (!selectedQuery) {{
    document.getElementById('query-details-{html_id}').style.display = 'none';
    return;
  }}

  // Show details
  document.getElementById('query-details-{html_id}').style.display = 'block';

  // Get query items
  const queryItems = itemsData_{html_id}.filter(item => item.{query_col} === selectedQuery);

  // Get query summary
  const queryInfo = querySummary_{html_id}.find(q => q.{query_col} === selectedQuery);

  if (!queryInfo) {{
    console.error('Query not found in summary:', selectedQuery);
    document.getElementById('query-details-{html_id}').style.display = 'none';
    alert('Error: Query data not found. Please try another query.');
    return;
  }}

  // Calculate net gain from rating distribution
  let badgeText = `${{queryItems.length}} items added`;
  const ratingDist = ratingDistributions_{html_id}[selectedQuery];
  if (ratingDist) {{
    const control4s = ratingDist.control[4] || 0;
    const variant4s = ratingDist.variant[4] || 0;
    const netGain = variant4s - control4s;
    badgeText = `+${{netGain}} net 4s (${{queryItems.length}} new items)`;
  }}

  // Update header
  document.getElementById('query-title-{html_id}').textContent = selectedQuery;
  document.getElementById('query-count-{html_id}').textContent = badgeText;

  // Update intents - show ALL intent types from Perceive API
  let intentsHtml = '';
  const intentTypes = [
    {{ key: 'product_type_intent', label: 'Product Type' }},
    {{ key: 'brand_intent', label: 'Brand' }},
    {{ key: 'color_intent', label: 'Color' }},
    {{ key: 'gender_intent', label: 'Gender' }},
    {{ key: 'size_intent', label: 'Size' }},
    {{ key: 'material_intent', label: 'Material' }},
    {{ key: 'pattern_intent', label: 'Pattern' }},
    {{ key: 'style_intent', label: 'Style' }},
    {{ key: 'age_group_intent', label: 'Age Group' }},
    {{ key: 'occasion_intent', label: 'Occasion' }},
    {{ key: 'category_intent', label: 'Category' }}
  ];

  intentTypes.forEach(intent => {{
    if (queryInfo && queryInfo[intent.key]) {{
      intentsHtml += `<div class="intent-item"><span class="intent-label">${{intent.label}}:</span> ${{queryInfo[intent.key]}}</div>`;
    }}
  }});

  if (!intentsHtml) {{
    intentsHtml = '<div class="intent-item" style="color: #9e9e9e;"><em>No structured intents detected</em></div>';
  }}
  document.getElementById('query-intents-{html_id}').innerHTML = intentsHtml;

  // Render rating distribution table
  let ratingTableHtml = '';
  // ratingDist already defined above for badge calculation
  if (ratingDist) {{
    const controlDist = ratingDist.control || {{}};
    const variantDist = ratingDist.variant || {{}};

    ratingTableHtml = `
      <table class="rating-table">
        <thead>
          <tr>
            <th>Label</th>
            <th>Control</th>
            <th>Variant</th>
            <th>Diff</th>
          </tr>
        </thead>
        <tbody>
    `;

    for (let label = 1; label <= 4; label++) {{
      const ctrlCount = controlDist[label] || 0;
      const varCount = variantDist[label] || 0;
      const diff = varCount - ctrlCount;
      const diffClass = diff > 0 ? 'diff-positive' : diff < 0 ? 'diff-negative' : 'diff-neutral';
      const diffText = diff > 0 ? `+${{diff}}` : diff;

      ratingTableHtml += `
        <tr>
          <td class="label-col">${{label}}</td>
          <td>${{ctrlCount}}</td>
          <td>${{varCount}}</td>
          <td class="${{diffClass}}">${{diffText}}</td>
        </tr>
      `;
    }}

    ratingTableHtml += `
        </tbody>
      </table>
    `;
  }} else {{
    ratingTableHtml = '<div style="color: #9e9e9e;"><em>Rating distribution not available</em></div>';
  }}
  document.getElementById('rating-dist-table-{html_id}').innerHTML = ratingTableHtml;

  // Render items
  let itemsHtml = '';
  queryItems.forEach(item => {{
    // Use item_ columns from joined item_attributes (same for all change types)
    const title = item.item_title || item.title_var || item.title_ctrl || item.title || 'N/A';
    const pt = item.item_product_type || item.product_type_var || item.product_type_ctrl || item.product_type || 'N/A';
    const brand = item.item_brand || item.brand_var || item.brand_ctrl || item.brand || 'N/A';
    const color = item.item_color || item.color_var || item.color_ctrl || item.color || '';
    const gender = item.item_gender || item.gender_var || item.gender_ctrl || item.gender || '';

    // Parse query intents to get scores for matched attributes
    const ptIntent = queryInfo && queryInfo.product_type_intent ? JSON.parse(queryInfo.product_type_intent) : [];
    const brandIntent = queryInfo && queryInfo.brand_intent ? JSON.parse(queryInfo.brand_intent) : [];
    const colorIntent = queryInfo && queryInfo.color_intent ? JSON.parse(queryInfo.color_intent) : [];
    const genderIntent = queryInfo && queryInfo.gender_intent ? JSON.parse(queryInfo.gender_intent) : [];

    // Find intent scores for matched attributes (case-insensitive match)
    const findIntentScore = (intentArray, itemValue) => {{
      if (!itemValue || itemValue === 'N/A') return null;
      const match = intentArray.find(intent =>
        intent.value && intent.value.toLowerCase() === itemValue.toLowerCase()
      );
      return match ? match.score : null;
    }};

    const ptScore = findIntentScore(ptIntent, pt);
    const brandScore = findIntentScore(brandIntent, brand);
    const colorScore = findIntentScore(colorIntent, color);
    const genderScore = findIntentScore(genderIntent, gender);

    // Title match is % of query tokens in title (keep as %)
    const titleMatch = item.title_match || 0;

    // Overall match comes from pre-computed matching (from attribute_matching skill)
    const overallMatch = item.overall_match || 0;

    const overallClass = overallMatch > 0.5 ? 'score-high' : overallMatch > 0.2 ? 'score-medium' : 'score-low';

    // Different badge for change type
    let changeTypeBadge = '';
    if (item.change_type === '4_gained') {{
      changeTypeBadge = '<span style="background: #10b981; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600;">✨ 4 ADDED</span>';
    }} else {{
      const label = item.label_ctrl || '?';
      const labelColor = label == 3 ? '#f59e0b' : label == 2 ? '#ef4444' : '#dc2626';
      changeTypeBadge = `<span style="background: ${{labelColor}}; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600;">🔻 LABEL-${{label}} REMOVED</span>`;
    }}

    itemsHtml += `
      <div class="item-card" style="${{item.change_type === 'non4_removed' ? 'border-left: 4px solid #ef4444;' : 'border-left: 4px solid #10b981;'}}">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
          <div class="item-id">${{item.pg_prod_id}}</div>
          ${{changeTypeBadge}}
        </div>
        <div class="item-title">${{title.substring(0, 80)}}...</div>
        <div class="item-attributes">
          <div><strong>Product Type:</strong> ${{pt}}</div>
          <div><strong>Brand:</strong> ${{brand}}</div>
          ${{color ? `<div><strong>Color:</strong> ${{color}}</div>` : ''}}
          ${{gender ? `<div><strong>Gender:</strong> ${{gender}}</div>` : ''}}
        </div>
        <div class="match-scores">
          <div class="match-score"><strong>Overall:</strong> <span class="${{overallClass}}">${{overallMatch.toFixed(2)}}</span></div>
          ${{ptScore !== null ? `<div class="match-score"><strong>PT:</strong> ${{ptScore.toFixed(2)}} <span style="color: #666; font-size: 10px;">(${{pt}})</span></div>` : ''}}
          ${{brandScore !== null ? `<div class="match-score"><strong>Brand:</strong> ${{brandScore.toFixed(2)}} <span style="color: #666; font-size: 10px;">(${{brand}})</span></div>` : ''}}
          ${{colorScore !== null ? `<div class="match-score"><strong>Color:</strong> ${{colorScore.toFixed(2)}} <span style="color: #666; font-size: 10px;">(${{color}})</span></div>` : ''}}
          ${{genderScore !== null ? `<div class="match-score"><strong>Gender:</strong> ${{genderScore.toFixed(2)}} <span style="color: #666; font-size: 10px;">(${{gender}})</span></div>` : ''}}
          <div class="match-score"><strong>Title:</strong> ${{(titleMatch * 100).toFixed(0)}}%</div>
        </div>
      </div>
    `;
  }});

  document.getElementById('items-list-{html_id}').innerHTML = itemsHtml;
}}
</script>

</body>
</html>"""

    # Write HTML file
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    return output_path


def main():
    parser = argparse.ArgumentParser(description='Generate HTML report for 4s added')
    parser.add_argument(
        '--input',
        type=str,
        default='./temp/downloaded_files/qip_pairs.parquet',
        help='Input QI pairs file (same as view_qip_pairs.py uses)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='./reports/4s_added_report.html',
        help='Output HTML file'
    )
    parser.add_argument(
        '--with-matching',
        action='store_true',
        help='Use pre-computed matching scores from qip_pairs_with_matching.parquet'
    )

    args = parser.parse_args()

    # Determine input file
    input_file = args.input

    # Check input
    if not Path(input_file).exists():
        print(f"❌ Input file not found: {input_file}")
        if args.with_matching:
            print("\nRun: python skills/attribute_matching/main.py")
        else:
            print("\nRun: python create_qip_pairs.py")
        return 1

    # Load data
    print(f"\nLoading: {input_file}")
    df = pd.read_parquet(input_file)
    print(f"Loaded {len(df):,} pairs")

    # Load and join item attributes if available
    item_attrs_path = input_file.replace('_qip_pairs_with_matching.parquet', '_item_attributes.jsonl')
    item_attrs_path = item_attrs_path.replace('_qip_pairs.parquet', '_item_attributes.jsonl')

    if Path(item_attrs_path).exists():
        print(f"\nLoading item attributes: {item_attrs_path}")
        import json

        # Load JSONL file
        items_data = []
        with open(item_attrs_path, 'r') as f:
            for line in f:
                items_data.append(json.loads(line))

        items_df = pd.DataFrame(items_data)
        print(f"  Loaded {len(items_df):,} items")

        # Extract simple attributes we need
        if 'product_type' in items_df.columns:
            items_df['item_product_type'] = items_df['product_type'].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) > 0 else (x if isinstance(x, str) else None)
            )
        if 'brand' in items_df.columns:
            items_df['item_brand'] = items_df['brand'].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) > 0 else (x if isinstance(x, str) else None)
            )
        if 'color' in items_df.columns:
            items_df['item_color'] = items_df['color'].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) > 0 else (x if isinstance(x, str) else None)
            )
        if 'gender' in items_df.columns:
            items_df['item_gender'] = items_df['gender'].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) > 0 else (x if isinstance(x, str) else None)
            )
        if 'title' in items_df.columns:
            items_df['item_title'] = items_df['title']

        # Join with pairs on pg_prod_id
        item_cols_to_join = ['pg_prod_id'] + [c for c in items_df.columns if c.startswith('item_')]
        items_to_join = items_df[item_cols_to_join].copy()

        df = df.merge(items_to_join, on='pg_prod_id', how='left')
        print(f"  ✅ Joined item attributes to pairs")
    else:
        print(f"\n⚠️  Item attributes not found: {item_attrs_path}")
        print("  Items will show N/A for product_type, brand, color, gender")

    # If not using pre-computed matching, compute it now
    if not args.with_matching and 'overall_match' not in df.columns:
        print("\nComputing attribute matching scores...")
        from skills.attribute_matching.main import AttributeMatchingSkill, AttributeMatchingInput

        query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'
        input_config = AttributeMatchingInput(
            pairs_df=df,
            query_col=query_col,
            focus_change_types=['4_gained', 'non4_removed', '4_lost']
        )

        skill = AttributeMatchingSkill(verbose=True)
        output = skill.run(input_config)
        df = output.pairs_with_scores
        print(f"✅ Computed matching scores for {len(df):,} pairs")

    # Summary stats are computed from the data itself (no separate summary file needed)

    # Get control and variant engine names
    control_engine = 'control'
    variant_engine = 'variant'

    # Load filtered QIP scores to get rating distributions per query
    filtered_qip_path = input_file.replace('_qip_pairs_with_matching.parquet', '_qip_4s_gain_filtered.parquet')
    filtered_qip_path = filtered_qip_path.replace('_qip_pairs.parquet', '_qip_4s_gain_filtered.parquet')

    # Try to get engine names from summary (use same base path as filtered file)
    summary_path = str(filtered_qip_path).replace('.parquet', '_summary.json')
    if Path(summary_path).exists():
        print(f"\nLoading engine names from: {summary_path}")
        with open(summary_path, 'r') as f:
            filter_summary = json.load(f)
            control_engine = filter_summary.get('control_engine', 'control')
            variant_engine = filter_summary.get('variant_engine', 'variant')
            print(f"  Control: {control_engine}")
            print(f"  Variant: {variant_engine}")
    else:
        print(f"\n⚠️ Summary file not found: {summary_path}")
        print("  Using default engine names: control, variant")

    rating_distributions = {}
    if Path(filtered_qip_path).exists():
        print(f"\nLoading rating distributions from: {filtered_qip_path}")
        qip_filtered = pd.read_parquet(filtered_qip_path)
        query_col = 'contextualQuery' if 'contextualQuery' in qip_filtered.columns else 'query'

        # Compute rating distribution for each query
        for query in qip_filtered[query_col].unique():
            query_data = qip_filtered[qip_filtered[query_col] == query]

            # Group by engine and label
            dist = query_data.groupby(['engine', 'label']).size().unstack(fill_value=0)

            # Separate control and variant
            control_dist = dist.loc[control_engine] if control_engine in dist.index else pd.Series([0,0,0,0], index=[1,2,3,4])
            variant_dist = dist.loc[variant_engine] if variant_engine in dist.index else pd.Series([0,0,0,0], index=[1,2,3,4])

            rating_distributions[query] = {
                'control': control_dist.to_dict(),
                'variant': variant_dist.to_dict()
            }

        print(f"✅ Computed rating distributions for {len(rating_distributions)} queries")

    # Generate simple insights if not provided
    fours_gained = df[df['change_type'] == '4_gained']
    insights = []
    if len(fours_gained) > 0 and 'overall_match' in fours_gained.columns:
        avg_match = fours_gained['overall_match'].mean()
        insights.append(f"4s added have {avg_match:.0%} average overall attribute matching")

        if fours_gained['brand_exact_match'].mean() > 0.3:
            insights.append(f"Strong brand matching: {fours_gained['brand_exact_match'].mean():.0%} of items match query brand intent")

        if fours_gained['title_match'].mean() > 0.5:
            insights.append(f"High title relevance: {fours_gained['title_match'].mean():.0%} of query intents appear in item titles")

    # Generate report
    print("\nGenerating HTML report...")
    output_path = generate_html_report(
        df,
        {'4_gained': {
            'count': len(fours_gained),
            'avg_overall_match': fours_gained['overall_match'].mean() if 'overall_match' in fours_gained.columns else 0,
            'avg_pt_match': fours_gained['pt_exact_match'].mean() if 'pt_exact_match' in fours_gained.columns else 0,
            'avg_brand_match': fours_gained['brand_exact_match'].mean() if 'brand_exact_match' in fours_gained.columns else 0,
            'avg_title_match': fours_gained['title_match'].mean() if 'title_match' in fours_gained.columns else 0,
        }},
        insights,
        args.output,
        control_engine,
        variant_engine,
        rating_distributions
    )

    print(f"\n✅ Report generated: {output_path}")
    print(f"\nOpen in browser: file://{Path(output_path).absolute()}")

    return 0


if __name__ == '__main__':
    exit(main())
