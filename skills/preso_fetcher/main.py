"""
Main logic for Preso Fetcher skill.

Fetches top-40 results from Preso API with rate limiting and parallel processing.
"""

import json
import requests
import pandas as pd
import urllib.parse
import time
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

from .config import PresoFetcherInput, PresoFetcherOutput

# Disable SSL warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')


class RateLimiter:
    """Thread-safe rate limiter."""

    def __init__(self, qps: int):
        """
        Initialize rate limiter.

        Args:
            qps: Queries per second limit
        """
        self.min_interval = 1.0 / qps
        self.lock = threading.Lock()
        self.last_call = 0.0

    def wait(self):
        """Wait if necessary to respect rate limit."""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            sleep_time = self.min_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            self.last_call = time.time()


def load_contextual_queries(jsonl_path: str) -> Dict[str, dict]:
    """
    Load contextual queries from JSONL file(s) or directory.

    Args:
        jsonl_path: Path to a JSONL file or directory containing JSONL files

    Returns:
        Dict mapping query string to full contextualQuery dict
    """
    queries_map = {}

    def load_from_file(filepath: str) -> int:
        """Load queries from a single file. Returns count loaded."""
        count = 0
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if line.strip():
                        try:
                            data = json.loads(line)
                            cq = data.get('contextualQuery', {})
                            query_str = cq.get('query', '')

                            if not query_str:
                                continue

                            # Create a unique key: query + prg + stores
                            # This handles cases where same query has different contexts
                            key = f"{query_str}|{cq.get('prg', 'desktop')}|{cq.get('stores', '')}"

                            queries_map[key] = cq
                            count += 1
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            print(f"  ⚠️  Error loading {filepath}: {e}")
        return count

    path = Path(jsonl_path)

    # Handle directory input - load all JSONL files
    if path.is_dir():
        print(f"  Loading all JSONL files from directory: {path}")
        total_count = 0
        for jsonl_file in sorted(path.glob("*.jsonl")):
            count = load_from_file(str(jsonl_file))
            if count > 0:
                print(f"    Loaded {count} queries from {jsonl_file.name}")
                total_count += count

        if total_count == 0:
            print(f"  ⚠️  No queries found in directory {path}")
        else:
            print(f"  Total: {total_count} unique queries loaded")

    # Handle single file input
    elif path.is_file():
        count = load_from_file(str(path))
        print(f"  Loaded {count} queries from {path.name}")

        # If we have very few queries, search directory for other JSONL files
        if count < 10:
            print(f"  ⚠️  Only {count} queries found, searching for additional JSONL files...")

            directory = path.parent
            for jsonl_file in directory.glob("*.jsonl"):
                if jsonl_file == path:
                    continue  # Skip primary file

                additional_count = load_from_file(str(jsonl_file))
                if additional_count > 0:
                    print(f"    Loaded {additional_count} additional queries from {jsonl_file.name}")

    else:
        raise FileNotFoundError(f"Path not found: {path}")

    return queries_map


def map_qip_to_contextual_queries(
    qip_df: pd.DataFrame,
    contextual_queries_map: Dict[str, dict]
) -> pd.DataFrame:
    """
    Map queries in qip_scores to full contextualQuery data.

    Args:
        qip_df: DataFrame from qip_scores.parquet
        contextual_queries_map: Map of query keys to contextualQuery dicts

    Returns:
        DataFrame with added contextualQuery column
    """
    def find_contextual_query(row):
        query = row.get('query', '')

        # Try different key combinations
        # 1. Try with any metadata we have
        for key in contextual_queries_map.keys():
            if key.startswith(f"{query}|"):
                return contextual_queries_map[key]

        # 2. Default: just match on query string (take first match)
        for key, cq in contextual_queries_map.items():
            if cq.get('query') == query:
                return cq

        # 3. No match - create minimal context
        return {
            'query': query,
            'prg': 'desktop',
            'stores': None,
            'stateOrProvinceCode': None,
            'zipcode': None
        }

    # Apply mapping
    qip_df['contextualQuery'] = qip_df.apply(find_contextual_query, axis=1)

    return qip_df


def parse_experiment_config(config_json: str) -> Tuple[Dict, Dict, str]:
    """
    Parse experiment configuration JSON.

    Returns:
        (control_config, variant_config, variant_engine_name)
    """
    if isinstance(config_json, dict):
        config = config_json
    else:
        config = json.loads(config_json)

    # Extract engines section
    engines = config.get('engines', {})

    # Control engine
    control_config = engines.get('control', {})

    # Variant engine (find the non-control engine)
    variant_engine_name = None
    variant_config = None

    for engine_name, engine_config in engines.items():
        if engine_name != 'control':
            variant_engine_name = engine_name
            variant_config = engine_config
            break

    if not variant_config:
        raise ValueError("No variant engine found in config")

    return control_config, variant_config, variant_engine_name


def build_preso_url(
    contextual_query: dict,
    base_host: str,
    request_params: Dict,
    top_n: int = 40,
    ptss: Optional[str] = None,
    trsp: Optional[str] = None
) -> str:
    """
    Build Preso search URL from contextual query.

    Args:
        contextual_query: Dict with query, prg, stores, zipcode, etc.
        base_host: Preso host URL
        request_params: Base request parameters from config
        top_n: Number of results to fetch
        ptss: PTSS flags (experiment switches)
        trsp: TRSP flags (variant model config)

    Returns:
        Full Preso URL
    """
    # Start with base params from config
    params = request_params.copy()

    # Override with contextual query params
    params['query'] = contextual_query.get('query')
    params['prg'] = contextual_query.get('prg', 'desktop')

    if contextual_query.get('stores'):
        params['stores'] = contextual_query.get('stores')

    if contextual_query.get('stateOrProvinceCode'):
        params['stateOrProvinceCode'] = contextual_query.get('stateOrProvinceCode')

    if contextual_query.get('zipcode'):
        params['zipcode'] = contextual_query.get('zipcode')

    # Add sort if present
    if contextual_query.get('sort'):
        params['sort'] = contextual_query.get('sort')

    # Set page size
    params['ps'] = top_n

    # Add experiment flags
    if ptss:
        params['ptss'] = ptss

    if trsp:
        params['trsp'] = trsp

    # Build URL
    # Remove http:// or https:// from base_host if present
    host = base_host.replace('http://', '').replace('https://', '').rstrip('/')
    # Remove any path components
    if '/' in host:
        host = host.split('/')[0]

    url = f"http://{host}/v1/search?{urllib.parse.urlencode(params)}"

    return url


def fetch_preso_results_for_query(
    contextual_query: dict,
    control_config: Dict,
    variant_config: Dict,
    access_key: str,
    top_n: int,
    rate_limiter: RateLimiter
) -> Tuple[str, List[dict], List[dict]]:
    """
    Fetch Preso results for a single contextual query.

    Returns:
        (query_string, control_results, variant_results)
    """
    query_str = contextual_query.get('query', '')

    headers = {
        "access_key": access_key,
        "Content-Type": "application/json"
    }

    # Build control URL
    control_host = control_config.get('host', 'preso-usgm-wcnp.prod.walmart.com')
    control_params = control_config.get('request_params', {})
    control_url = build_preso_url(
        contextual_query,
        control_host,
        control_params,
        top_n=top_n
    )

    # Build variant URL
    # Variant inherits control's base params and adds its own
    variant_host = variant_config.get('host', 'preso-usgm-wcnp.prod.walmart.com')

    # Start with control params as base, then override with variant-specific params
    variant_params = control_params.copy()
    variant_params.update(variant_config.get('request_params', {}))

    # Merge ptss: control + variant
    control_ptss = control_params.get('ptss', '')
    variant_ptss_only = variant_params.get('ptss', '')

    if control_ptss and variant_ptss_only:
        merged_ptss = f"{control_ptss};{variant_ptss_only}"
    elif variant_ptss_only:
        merged_ptss = variant_ptss_only
    elif control_ptss:
        merged_ptss = control_ptss
    else:
        merged_ptss = None

    # Merge trsp: control + variant
    control_trsp = control_params.get('trsp', '')
    variant_trsp_only = variant_params.get('trsp', '')

    if control_trsp and variant_trsp_only:
        merged_trsp = f"{control_trsp};{variant_trsp_only}"
    elif variant_trsp_only:
        merged_trsp = variant_trsp_only
    elif control_trsp:
        merged_trsp = control_trsp
    else:
        merged_trsp = None

    variant_url = build_preso_url(
        contextual_query,
        variant_host,
        variant_params,
        top_n=top_n,
        ptss=merged_ptss,
        trsp=merged_trsp
    )

    control_results = []
    variant_results = []

    # Print URLs for debugging
    print(f"\n🔗 URLs for query '{query_str}':")
    print(f"  Control: {control_url}")
    print(f"  Variant: {variant_url}")

    try:
        # Fetch control
        rate_limiter.wait()
        control_resp = requests.get(control_url, headers=headers, timeout=30, verify=False)
        control_resp.raise_for_status()
        control_json = control_resp.json()

        # Extract items from Preso modular response
        # Structure: moduleArray[0]['content']['items'] or ['content']['itemStacks']
        items_list = []

        if 'moduleArray' in control_json and control_json['moduleArray']:
            for module in control_json['moduleArray']:
                if 'content' in module:
                    content = module['content']

                    # Check for direct items
                    if 'items' in content:
                        items_list.extend(content['items'])

                    # Check for itemStacks
                    elif 'itemStacks' in content:
                        for stack in content['itemStacks']:
                            if 'items' in stack:
                                items_list.extend(stack['items'])

        # Fallback: check for items or itemStacks at root level
        if not items_list:
            if 'items' in control_json:
                items_list = control_json['items']
            elif 'itemStacks' in control_json:
                for stack in control_json['itemStacks']:
                    if 'items' in stack:
                        items_list.extend(stack['items'])

        for idx, item in enumerate(items_list[:top_n]):
            control_results.append({
                'query': query_str,
                'contextualQuery': json.dumps(contextual_query) if isinstance(contextual_query, dict) else str(contextual_query),
                'rank': idx + 1,
                'product_id': item.get('productId') or item.get('usItemId'),
                'title': item.get('title', ''),
                'engine': 'control',
                'url': control_url
            })

        # Fetch variant
        rate_limiter.wait()
        variant_resp = requests.get(variant_url, headers=headers, timeout=30, verify=False)
        variant_resp.raise_for_status()
        variant_json = variant_resp.json()

        # Extract items from Preso modular response
        # Structure: moduleArray[0]['content']['items'] or ['content']['itemStacks']
        items_list = []

        if 'moduleArray' in variant_json and variant_json['moduleArray']:
            for module in variant_json['moduleArray']:
                if 'content' in module:
                    content = module['content']

                    # Check for direct items
                    if 'items' in content:
                        items_list.extend(content['items'])

                    # Check for itemStacks
                    elif 'itemStacks' in content:
                        for stack in content['itemStacks']:
                            if 'items' in stack:
                                items_list.extend(stack['items'])

        # Fallback: check for items or itemStacks at root level
        if not items_list:
            if 'items' in variant_json:
                items_list = variant_json['items']
            elif 'itemStacks' in variant_json:
                for stack in variant_json['itemStacks']:
                    if 'items' in stack:
                        items_list.extend(stack['items'])

        for idx, item in enumerate(items_list[:top_n]):
            variant_results.append({
                'query': query_str,
                'contextualQuery': json.dumps(contextual_query) if isinstance(contextual_query, dict) else str(contextual_query),
                'rank': idx + 1,
                'product_id': item.get('productId') or item.get('usItemId'),
                'title': item.get('title', ''),
                'engine': 'variant',
                'url': variant_url
            })

    except Exception as e:
        import traceback
        print(f"\n❌ Error fetching results for query '{query_str}':")
        print(f"   Exception type: {type(e).__name__}")
        print(f"   Exception: {e}")
        print(f"   Control URL: {control_url}")
        print(f"   Variant URL: {variant_url}")
        print(f"   Traceback:")
        traceback.print_exc()

    return query_str, control_results, variant_results


def fetch_preso_results(input_config: PresoFetcherInput) -> PresoFetcherOutput:
    """
    Main execution function for Preso fetcher.

    1. Load qip_scores and contextual queries
    2. Map queries to full contextualQuery data
    3. Fetch Preso results with rate limiting
    4. Return DataFrame with results
    """

    print("="*80)
    print("Preso Fetcher - Fetching Top-40 Results")
    print("="*80)

    # Step 1: Load experiment config
    print("\n⚙️  Step 1: Loading experiment configuration...")

    config_json = None
    if input_config.experiment_config:
        config_json = input_config.experiment_config
    elif input_config.experiment_config_file:
        with open(input_config.experiment_config_file, 'r') as f:
            config_json = f.read()
    else:
        return PresoFetcherOutput(
            status="error",
            message="Must provide experiment_config or experiment_config_file"
        )

    try:
        control_config, variant_config, variant_name = parse_experiment_config(config_json)
        print(f"✅ Control engine: control")
        print(f"✅ Variant engine: {variant_name}")
    except Exception as e:
        return PresoFetcherOutput(
            status="error",
            message=f"Failed to parse experiment config: {e}"
        )

    # Step 2: Load qip_scores (contains contextualQuery info)
    print(f"\n📊 Step 2: Loading qip_scores from {input_config.qip_scores_file}...")

    try:
        qip_df = pd.read_parquet(input_config.qip_scores_file)
        print(f"✅ Loaded {len(qip_df)} QIP records")

        # Parse contextualQuery column
        # Format: "query (stores=XXXX, zipcode=XXXXX)" or similar
        if 'contextualQuery' in qip_df.columns:
            def parse_contextual_query(cq_str):
                """Parse contextualQuery string like 'milk (stores=2716, zipcode=52404)'"""
                import re

                # Extract query (before parentheses)
                match = re.match(r'^(.+?)\s*\((.+)\)$', cq_str)
                if not match:
                    # No parentheses - just the query
                    return {'query': cq_str.strip(), 'prg': 'desktop'}

                query = match.group(1).strip()
                params_str = match.group(2)

                # Parse parameters
                params = {'query': query, 'prg': 'desktop'}

                # Extract key=value pairs
                for param in params_str.split(','):
                    param = param.strip()
                    if '=' in param:
                        key, value = param.split('=', 1)
                        key = key.strip()
                        value = value.strip()

                        # Convert to appropriate type
                        if key == 'stores':
                            params['stores'] = int(value)
                        elif key == 'zipcode':
                            params['zipcode'] = value
                        elif key == 'stateOrProvinceCode':
                            params['stateOrProvinceCode'] = value
                        elif key == 'prg':
                            params['prg'] = value
                        else:
                            params[key] = value

                return params

            # Parse all contextualQuery strings
            qip_df['contextualQuery_parsed'] = qip_df['contextualQuery'].apply(parse_contextual_query)
            # Extract query strings
            qip_df['query'] = qip_df['contextualQuery_parsed'].apply(lambda x: x.get('query', ''))
        elif 'query' not in qip_df.columns:
            return PresoFetcherOutput(
                status="error",
                message="qip_scores file must have either 'query' or 'contextualQuery' column"
            )

        # Get unique queries
        unique_queries = qip_df['query'].unique()
        print(f"✅ Found {len(unique_queries)} unique queries")
    except Exception as e:
        return PresoFetcherOutput(
            status="error",
            message=f"Failed to load qip_scores: {e}"
        )

    # Step 3: Load FULL contextual queries from JSONL files and map to QIP data
    print("\n🔗 Step 3: Loading contextual queries from JSONL and mapping to QIP data...")

    try:
        # Load contextual queries from JSONL file(s)
        print(f"   Loading from: {input_config.contextual_queries_file}")
        contextual_queries_map = load_contextual_queries(input_config.contextual_queries_file)
        print(f"✅ Loaded {len(contextual_queries_map)} contextual queries from JSONL")

        # Map QIP queries to JSONL contextual queries
        queries_to_fetch_all = []
        seen_queries = set()
        matched_count = 0
        unmatched_count = 0

        # Group by contextualQuery to get unique query contexts
        for cq_str in qip_df['contextualQuery'].unique():
            # Get the parsed version from QIP
            sample_row = qip_df[qip_df['contextualQuery'] == cq_str].iloc[0]
            parsed_cq = sample_row['contextualQuery_parsed']
            query_str = parsed_cq.get('query', '')
            prg = parsed_cq.get('prg', 'desktop')
            stores = parsed_cq.get('stores', '')

            # Try to find in contextual_queries_map
            # Strategy 1: Try exact match with prg
            key_with_prg = f"{query_str}|{prg}|{stores}"

            # Strategy 2: Try matching with any prg (search for query+stores prefix)
            matched_cq = None
            if key_with_prg in contextual_queries_map:
                matched_cq = contextual_queries_map[key_with_prg]
            else:
                # Try to find with different prg values
                for prg_variant in ['desktop', 'ios', 'android', 'mWeb']:
                    key_variant = f"{query_str}|{prg_variant}|{stores}"
                    if key_variant in contextual_queries_map:
                        matched_cq = contextual_queries_map[key_variant]
                        break

            if matched_cq:
                # Use FULL contextual query from JSONL
                if query_str not in seen_queries:
                    queries_to_fetch_all.append(matched_cq)
                    seen_queries.add(query_str)
                    matched_count += 1
            else:
                # Fall back to parsed data from QIP (may be incomplete)
                if query_str not in seen_queries:
                    queries_to_fetch_all.append(parsed_cq)
                    seen_queries.add(query_str)
                    unmatched_count += 1

        coverage = matched_count / (matched_count + unmatched_count) * 100 if (matched_count + unmatched_count) > 0 else 0
        print(f"✅ Matched {matched_count} queries to JSONL data ({coverage:.1f}% coverage)")
        if unmatched_count > 0:
            print(f"⚠️  {unmatched_count} queries not found in JSONL, using parsed QIP data")

        # Apply start_idx and end_idx slice
        queries_to_fetch = queries_to_fetch_all[input_config.start_idx:input_config.end_idx]

        print(f"✅ Will fetch {len(queries_to_fetch)} queries (slice {input_config.start_idx}:{input_config.end_idx})")

        # Show sample
        if queries_to_fetch:
            sample_cq = queries_to_fetch[0]
            print(f"   Sample: query='{sample_cq.get('query')}', stores={sample_cq.get('stores')}, " +
                  f"zipcode={sample_cq.get('zipcode')}, state={sample_cq.get('stateOrProvinceCode')}, prg={sample_cq.get('prg')}")

    except Exception as e:
        return PresoFetcherOutput(
            status="error",
            message=f"Failed to build contextual queries: {e}"
        )

    # Step 4: Fetch Preso results with rate limiting
    print(f"\n🚀 Step 4: Fetching Preso results (QPS: {input_config.qps}, Workers: {input_config.max_workers})...")
    print(f"   This will take approximately {len(queries_to_fetch) * 2 / input_config.qps / 60:.1f} minutes")

    rate_limiter = RateLimiter(qps=input_config.qps)
    all_results = []
    queries_processed = 0
    queries_failed = 0

    control_urls = []
    variant_urls = []

    with ThreadPoolExecutor(max_workers=input_config.max_workers) as executor:
        futures = {
            executor.submit(
                fetch_preso_results_for_query,
                cq,
                control_config,
                variant_config,
                input_config.access_key,
                input_config.top_n,
                rate_limiter
            ): cq
            for cq in queries_to_fetch
        }

        for i, future in enumerate(as_completed(futures), start=1):
            cq = futures[future]
            try:
                query_str, control_results, variant_results = future.result()

                if control_results or variant_results:
                    all_results.extend(control_results)
                    all_results.extend(variant_results)

                    if control_results:
                        control_urls.append(control_results[0]['url'])
                    if variant_results:
                        variant_urls.append(variant_results[0]['url'])

                    queries_processed += 1
                else:
                    queries_failed += 1

                if i % 10 == 0:
                    print(f"   Progress: {i}/{len(queries_to_fetch)} queries ({queries_processed} success, {queries_failed} failed)")

            except Exception as e:
                print(f"   Error processing query {cq.get('query')}: {e}")
                queries_failed += 1

    print(f"\n✅ Completed fetching results")
    print(f"   Queries processed: {queries_processed}")
    print(f"   Queries failed: {queries_failed}")
    print(f"   Total results: {len(all_results)}")

    # Step 5: Create results DataFrame
    if all_results:
        results_df = pd.DataFrame(all_results)
        print(f"\n📊 Results DataFrame: {len(results_df)} rows, {len(results_df.columns)} columns")
    else:
        results_df = pd.DataFrame()
        print("\n⚠️  No results fetched")

    return PresoFetcherOutput(
        results_df=results_df,
        control_urls=control_urls,
        variant_urls=variant_urls,
        status="success",
        message=f"Fetched results for {queries_processed} queries",
        queries_processed=queries_processed,
        queries_failed=queries_failed
    )
