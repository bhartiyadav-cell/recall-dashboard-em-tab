"""
Library for implementing common Recall Ranker Comparison patterns.

Compares label distribution between control and variation engines,
with statistical testing and query-level analysis.
"""

import pandas as pd
import numpy as np
from scipy.stats import ttest_rel
from typing import Optional, Tuple, List


def get_label_distribution(group: pd.DataFrame) -> pd.Series:
    """
    Compute label distribution for a group of items.
    
    Args:
        group: DataFrame group with 'label' column
        
    Returns:
        Series with count_1, count_2, count_3, count_4, total
    """
    dist = group['label'].value_counts()
    return pd.Series({
        'count_1': dist.get(1, 0),
        'count_2': dist.get(2, 0),
        'count_3': dist.get(3, 0),
        'count_4': dist.get(4, 0),
        'total': len(group)
    })


def compute_distribution_by_query(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute label distribution per contextualQuery.
    
    Args:
        df: DataFrame with contextualQuery, label columns
        
    Returns:
        DataFrame with one row per query and label counts
    """
    distribution = df.groupby('contextualQuery').apply(get_label_distribution).reset_index()
    
    # Extract raw query and query length
    distribution['query'] = (
        distribution['contextualQuery']
        .str.split('(')
        .str[0]
        .str.strip()
    )
    distribution['query_len'] = distribution['query'].str.split().str.len()
    
    return distribution


def merge_control_variant_distributions(
    control_dist: pd.DataFrame,
    variant_dist: pd.DataFrame,
    min_total: int = 400,
    max_total_diff: int = 5
) -> pd.DataFrame:
    """
    Merge control and variant distributions with optional filtering.
    
    Args:
        control_dist: Distribution DataFrame for control
        variant_dist: Distribution DataFrame for variant
        min_total: Minimum total items required
        max_total_diff: Maximum difference in total items allowed
        
    Returns:
        Merged DataFrame with _ctrl and _var suffixes
    """
    comparison = control_dist.merge(
        variant_dist,
        on=['contextualQuery', 'query', 'query_len'],
        how='inner',
        suffixes=['_ctrl', '_var']
    )
    
    # Apply filters
    if min_total > 0 or max_total_diff < float('inf'):
        comparison = comparison[
            (abs(comparison['total_ctrl'] - comparison['total_var']) < max_total_diff) &
            (comparison['total_var'] > min_total)
        ]
    
    # Compute gains
    for label in [1, 2, 3, 4]:
        comparison[f'{label}s_gain'] = (
            comparison[f'count_{label}_var'] - comparison[f'count_{label}_ctrl']
        )
    
    return comparison


def run_paired_ttest(
    comparison: pd.DataFrame,
    labels: List[int] = [1, 2, 3, 4]
) -> pd.DataFrame:
    """
    Run paired t-tests for label counts between control and variant.
    
    Args:
        comparison: Merged comparison DataFrame
        labels: List of labels to test
        
    Returns:
        DataFrame with t-test results
    """
    results = []
    
    for label in labels:
        ctrl_col = f'count_{label}_ctrl'
        var_col = f'count_{label}_var'
        
        stat, pval = ttest_rel(
            comparison[ctrl_col],
            comparison[var_col],
            nan_policy='omit'
        )
        
        results.append({
            'label': label,
            'metric': f'count_{label}',
            't_stat': stat,
            'p_value': pval,
            'mean_ctrl': comparison[ctrl_col].mean(),
            'mean_var': comparison[var_col].mean(),
            'mean_diff': (comparison[var_col] - comparison[ctrl_col]).mean(),
            'n_queries': len(comparison)
        })
    
    return pd.DataFrame(results)


def run_ttest_by_query_length(
    comparison: pd.DataFrame,
    labels: List[int] = [1, 2, 3, 4],
    max_individual_len: int = 4
) -> pd.DataFrame:
    """
    Run paired t-tests grouped by query length.
    
    Args:
        comparison: Merged comparison DataFrame
        labels: List of labels to test
        max_individual_len: Lengths above this are grouped as "N+"
        
    Returns:
        DataFrame with t-test results by query length
    """
    # Create query length groups
    comparison = comparison.copy()
    comparison['query_len_group'] = comparison['query_len'].apply(
        lambda x: str(x) if x <= max_individual_len else f'{max_individual_len + 1}+'
    )
    
    results = []
    
    for qlen, group in comparison.groupby('query_len_group'):
        if len(group) < 5:
            continue
        
        for label in labels:
            ctrl_col = f'count_{label}_ctrl'
            var_col = f'count_{label}_var'
            
            stat, pval = ttest_rel(
                group[ctrl_col],
                group[var_col],
                nan_policy='omit'
            )
            
            results.append({
                'query_len_group': qlen,
                'label': label,
                'metric': f'count_{label}',
                'n_queries': len(group),
                't_stat': stat,
                'p_value': pval,
                'mean_ctrl': group[ctrl_col].mean(),
                'mean_var': group[var_col].mean(),
                'mean_diff': (group[var_col] - group[ctrl_col]).mean()
            })
    
    return pd.DataFrame(results).sort_values(['query_len_group', 'label'])


def find_missing_items(
    df_source: pd.DataFrame,
    df_target: pd.DataFrame,
    label_filter: Optional[int] = None
) -> pd.DataFrame:
    """
    Find items in source that are NOT in target.
    
    Args:
        df_source: Source DataFrame (items we're looking for)
        df_target: Target DataFrame (items to check against)
        label_filter: If provided, only consider items with this label
        
    Returns:
        DataFrame of items in source but not in target
    """
    source = df_source.copy()
    target = df_target.copy()
    
    if label_filter is not None:
        source = source[source['label'] == label_filter]
        target = target[target['label'] == label_filter]
    
    # Create key from contextualQuery + item identifier
    # Try pg_prod_id first, fall back to item_id
    id_col = 'pg_prod_id' if 'pg_prod_id' in source.columns else 'item_id'
    
    source['_key'] = source['contextualQuery'].astype(str) + "::" + source[id_col].astype(str)
    target['_key'] = target['contextualQuery'].astype(str) + "::" + target[id_col].astype(str)
    
    # Find items in source but not in target
    missing = source[~source['_key'].isin(target['_key'])].copy()
    missing = missing.drop(columns=['_key'])
    
    return missing


def find_extra_items(
    df_source: pd.DataFrame,
    df_target: pd.DataFrame,
    label_filter: Optional[int] = None
) -> pd.DataFrame:
    """
    Find items in source that are NOT in target (alias for find_missing_items with swapped args).
    
    Args:
        df_source: Source DataFrame (variant)
        df_target: Target DataFrame (control)
        label_filter: If provided, only consider items with this label
        
    Returns:
        DataFrame of items in source but not in target
    """
    return find_missing_items(df_source, df_target, label_filter)


def get_queries_with_label_gain(
    comparison: pd.DataFrame,
    label: int,
    min_gain: int = 1
) -> pd.DataFrame:
    """
    Get queries where variant has more items of specified label than control.
    
    Args:
        comparison: Merged comparison DataFrame
        label: Label to check (1, 2, 3, or 4)
        min_gain: Minimum gain required
        
    Returns:
        DataFrame sorted by gain descending
    """
    gain_col = f'{label}s_gain'
    return (
        comparison[comparison[gain_col] >= min_gain]
        .sort_values(gain_col, ascending=False)
    )


def get_queries_with_label_loss(
    comparison: pd.DataFrame,
    label: int,
    max_loss: int = -1
) -> pd.DataFrame:
    """
    Get queries where variant has fewer items of specified label than control.
    
    Args:
        comparison: Merged comparison DataFrame
        label: Label to check (1, 2, 3, or 4)
        max_loss: Maximum loss threshold (negative value)
        
    Returns:
        DataFrame sorted by loss ascending (most loss first)
    """
    gain_col = f'{label}s_gain'
    return (
        comparison[comparison[gain_col] <= max_loss]
        .sort_values(gain_col, ascending=True)
    )


def summarize_recall_comparison(
    df: pd.DataFrame,
    control_engine: str = 'control',
    variant_engine: str = None,
    min_total: int = 400,
    max_total_diff: int = 5
) -> dict:
    """
    Generate a complete summary of recall comparison.
    
    Args:
        df: Full DataFrame with engine, contextualQuery, pg_prod_id, label
        control_engine: Name of control engine
        variant_engine: Name of variant engine (auto-detected if None)
        min_total: Minimum total items required per query
        max_total_diff: Maximum difference in total items allowed
        
    Returns:
        Dictionary with comparison results
    """
    engines = df['engine'].unique().tolist()
    
    if variant_engine is None:
        variant_engine = [e for e in engines if e != control_engine][0]
    
    df_ctrl = df[df['engine'] == control_engine]
    df_var = df[df['engine'] == variant_engine]
    
    ctrl_dist = compute_distribution_by_query(df_ctrl)
    var_dist = compute_distribution_by_query(df_var)
    
    comparison = merge_control_variant_distributions(
        ctrl_dist, var_dist, min_total, max_total_diff
    )
    
    ttest_overall = run_paired_ttest(comparison)
    ttest_by_qlen = run_ttest_by_query_length(comparison)
    
    # Find missing/extra 4-rated items
    missing_4s_in_var = find_missing_items(df_ctrl, df_var, label_filter=4)
    extra_4s_in_var = find_missing_items(df_var, df_ctrl, label_filter=4)
    
    return {
        'control_engine': control_engine,
        'variant_engine': variant_engine,
        'comparison': comparison,
        'ttest_overall': ttest_overall,
        'ttest_by_qlen': ttest_by_qlen,
        'missing_4s_in_variant': missing_4s_in_var,
        'extra_4s_in_variant': extra_4s_in_var,
        'n_queries': len(comparison),
        'queries_with_4s_gain': get_queries_with_label_gain(comparison, 4),
        'queries_with_4s_loss': get_queries_with_label_loss(comparison, 4)
    }