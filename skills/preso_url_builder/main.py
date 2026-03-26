"""
Main logic for Preso URL Builder skill.

Extracts experiment configuration and builds Preso search URLs.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlencode
import pandas as pd
import base64

from .config import PresoUrlBuilderInput, PresoUrlBuilderOutput


def decode_email_body(email_text: str) -> str:
    """
    Decode email body if it's base64 encoded.

    Handles multipart/mixed emails with base64 encoded HTML content.
    """
    # Check if email contains base64 encoded content
    if 'Content-Transfer-Encoding: base64' in email_text:
        # Find the base64 content (after the headers)
        lines = email_text.split('\n')

        # Find where base64 content starts (after empty line following Content-Transfer-Encoding)
        in_base64_section = False
        base64_lines = []

        for i, line in enumerate(lines):
            if 'Content-Transfer-Encoding: base64' in line:
                in_base64_section = True
                # Skip until we hit an empty line
                continue

            if in_base64_section:
                # Empty line indicates start of base64 content
                if line.strip() == '':
                    # Start collecting base64 content from next line
                    for j in range(i + 1, len(lines)):
                        content_line = lines[j].strip()
                        # Stop at boundary marker
                        if content_line.startswith('--'):
                            break
                        if content_line:
                            base64_lines.append(content_line)
                    break

        if base64_lines:
            try:
                # Join and decode
                base64_content = ''.join(base64_lines)
                decoded_bytes = base64.b64decode(base64_content)
                decoded_text = decoded_bytes.decode('utf-8', errors='ignore')
                # Return both original and decoded (for searching)
                return email_text + '\n\n' + decoded_text
            except Exception as e:
                # If decode fails, just return original
                pass

    return email_text


def extract_gcs_path_from_email(email_text: str) -> Optional[str]:
    """
    Extract GCS bucket path from email text.

    Looks for gs:// URLs.
    """
    # Pattern: gs://bucket-name/path/to/experiment/id
    gcs_pattern = r'gs://[a-zA-Z0-9_\-./]+'

    matches = re.findall(gcs_pattern, email_text)

    if matches:
        # Return the first (or longest) GCS path found
        return max(matches, key=len)

    return None


def extract_json_from_html_pre(html_text: str) -> Optional[str]:
    """
    Extract JSON from HTML <pre> tag.

    The recall analysis script embeds the config JSON in a <pre> tag with <br/> line breaks.
    """
    # Look for <pre> tag with config
    pre_pattern = r'<pre[^>]*>(.*?)</pre>'
    matches = re.findall(pre_pattern, html_text, re.DOTALL | re.IGNORECASE)

    for match in matches:
        # Remove HTML tags and entities
        content = match
        content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)  # Replace <br/> with newlines
        content = re.sub(r'<[^>]+>', '', content)  # Remove any other HTML tags
        content = content.replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"').replace('&amp;', '&')

        # Check if it looks like a config (has "engines" or "comments")
        if '"engines"' in content or '"comments"' in content:
            # Try to parse as JSON
            try:
                json.loads(content)
                return content
            except json.JSONDecodeError:
                # Try to clean up and parse again
                # Sometimes there might be extra whitespace or formatting issues
                content = content.strip()
                try:
                    json.loads(content)
                    return content
                except json.JSONDecodeError:
                    continue

    return None


def extract_json_from_email(email_text: str) -> Optional[str]:
    """
    Extract JSON configuration from email text.

    Tries multiple approaches:
    1. Look for JSON in HTML <pre> tags (from recall analysis email)
    2. Look for raw JSON blocks between curly braces
    """
    # First try: HTML <pre> tag (most likely for recall analysis emails)
    json_str = extract_json_from_html_pre(email_text)
    if json_str:
        return json_str

    # Second try: Raw JSON in text
    # Try to find the config JSON starting with "comments" or "engines"
    start_markers = ['"comments"', '"engines"']

    for marker in start_markers:
        if marker in email_text:
            # Find the opening brace before this marker
            marker_pos = email_text.find(marker)

            # Search backwards for opening brace
            brace_count = 0
            start_pos = None

            for i in range(marker_pos - 1, -1, -1):
                if email_text[i] == '{':
                    start_pos = i
                    break

            if start_pos is None:
                continue

            # Now find the matching closing brace
            brace_count = 0
            end_pos = None

            for i in range(start_pos, len(email_text)):
                if email_text[i] == '{':
                    brace_count += 1
                elif email_text[i] == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break

            if end_pos:
                json_str = email_text[start_pos:end_pos]

                # Validate it's proper JSON
                try:
                    json.loads(json_str)
                    return json_str
                except json.JSONDecodeError:
                    continue

    return None


def parse_experiment_config(config_json: str) -> tuple[Dict, Dict, str]:
    """
    Parse experiment configuration JSON.

    Returns:
        (control_config, variant_config, variant_engine_name)
    """
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
    query: str,
    base_host: str,
    request_params: Dict,
    ptss: Optional[str] = None,
    trsp: Optional[str] = None
) -> str:
    """
    Build Preso search URL.

    Args:
        query: Search query
        base_host: Preso host (e.g., preso-usgm-wcnp.prod.walmart.com)
        request_params: Request parameters dict (stores, zipcode, prg, etc.)
        ptss: PTSS parameters (experiment switches)
        trsp: TRSP parameters (variant model config)

    Returns:
        Full Preso search URL
    """
    # Build query parameters
    params = request_params.copy()
    params['query'] = query

    # Add ptss if provided
    if ptss:
        params['ptss'] = ptss

    # Add trsp if provided
    if trsp:
        params['trsp'] = trsp

    # Build URL
    url = f"https://{base_host}/v1/search?{urlencode(params)}"

    return url


def load_queries(queries_file: str) -> List[str]:
    """Load queries from file (CSV, parquet, or txt)."""
    file_path = Path(queries_file)

    if file_path.suffix == '.csv':
        df = pd.read_csv(queries_file)
        # Assume first column or column named 'query'/'contextualQuery'
        if 'query' in df.columns:
            return df['query'].dropna().unique().tolist()
        elif 'contextualQuery' in df.columns:
            return df['contextualQuery'].dropna().unique().tolist()
        else:
            return df.iloc[:, 0].dropna().unique().tolist()

    elif file_path.suffix == '.parquet':
        df = pd.read_parquet(queries_file)
        if 'query' in df.columns:
            return df['query'].dropna().unique().tolist()
        elif 'contextualQuery' in df.columns:
            return df['contextualQuery'].dropna().unique().tolist()
        else:
            return df.iloc[:, 0].dropna().unique().tolist()

    elif file_path.suffix == '.txt':
        with open(queries_file, 'r') as f:
            return [line.strip() for line in f if line.strip()]

    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")


def run(input_config: PresoUrlBuilderInput) -> PresoUrlBuilderOutput:
    """
    Main execution function for Preso URL Builder.

    1. Extract/load experiment configuration
    2. Extract GCS path (if from email)
    3. Parse control and variant configs
    4. Build Preso URLs for queries (if provided)
    """

    # Step 1: Get configuration JSON and extract GCS path
    config_json = None
    gcs_path = None
    email_content = None

    if input_config.config_json:
        config_json = input_config.config_json

    elif input_config.config_file:
        with open(input_config.config_file, 'r') as f:
            config_json = f.read()

    elif input_config.email_text:
        email_content = input_config.email_text
        # Decode if base64 encoded
        email_content = decode_email_body(email_content)
        config_json = extract_json_from_email(email_content)
        gcs_path = extract_gcs_path_from_email(email_content)

        if not config_json:
            return PresoUrlBuilderOutput(
                control_config={},
                variant_config={},
                variant_engine_name="",
                gcs_path=gcs_path,
                status="error",
                message="Could not extract JSON from email text"
            )

    elif input_config.email_file:
        with open(input_config.email_file, 'r') as f:
            email_content = f.read()

        # Decode if base64 encoded
        email_content = decode_email_body(email_content)
        config_json = extract_json_from_email(email_content)
        gcs_path = extract_gcs_path_from_email(email_content)

        if not config_json:
            return PresoUrlBuilderOutput(
                control_config={},
                variant_config={},
                variant_engine_name="",
                gcs_path=gcs_path,
                status="error",
                message="Could not extract JSON from email file"
            )

    else:
        return PresoUrlBuilderOutput(
            control_config={},
            variant_config={},
            variant_engine_name="",
            status="error",
            message="No configuration source provided (config_json, config_file, email_text, or email_file)"
        )

    # Step 2: Parse configuration
    try:
        control_config, variant_config, variant_engine_name = parse_experiment_config(config_json)
    except Exception as e:
        return PresoUrlBuilderOutput(
            control_config={},
            variant_config={},
            variant_engine_name="",
            status="error",
            message=f"Failed to parse config: {e}"
        )

    # Step 3: Build URLs if queries provided
    urls_df = None

    if input_config.queries or input_config.queries_file or input_config.add_to_dataframe is not None:

        # Get queries
        queries = []

        if input_config.queries:
            queries = input_config.queries

        elif input_config.queries_file:
            queries = load_queries(input_config.queries_file)

        elif input_config.add_to_dataframe is not None:
            df = input_config.add_to_dataframe
            if 'contextualQuery' in df.columns:
                queries = df['contextualQuery'].dropna().unique().tolist()
            elif 'query' in df.columns:
                queries = df['query'].dropna().unique().tolist()

        # Extract base URLs and params
        control_host = control_config.get('host', '').replace('http://', '').replace('https://', '')
        control_params = control_config.get('request_params', {})

        # Variant uses same params as control, plus ptss from variant config
        variant_host = control_host  # Usually same host
        variant_params = control_params.copy()
        variant_ptss = variant_config.get('request_params', {}).get('ptss', '')

        # Build URLs for each query
        url_data = []
        for query in queries:
            control_url = build_preso_url(query, control_host, control_params)
            variant_url = build_preso_url(query, variant_host, variant_params, ptss=variant_ptss)

            url_data.append({
                'query': query,
                'control_url': control_url,
                'variant_url': variant_url
            })

        urls_df = pd.DataFrame(url_data)

        # If add_to_dataframe provided, merge URLs back
        if input_config.add_to_dataframe is not None:
            df = input_config.add_to_dataframe
            query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'

            # Merge URLs
            df = df.merge(
                urls_df.rename(columns={'query': query_col}),
                on=query_col,
                how='left'
            )

            urls_df = df

        # Save to file if requested
        if input_config.output_file:
            output_path = Path(input_config.output_file)
            if output_path.suffix == '.csv':
                urls_df.to_csv(input_config.output_file, index=False)
            elif output_path.suffix == '.parquet':
                urls_df.to_parquet(input_config.output_file, index=False)
            else:
                # Default to CSV
                urls_df.to_csv(input_config.output_file, index=False)

    return PresoUrlBuilderOutput(
        control_config=control_config,
        variant_config=variant_config,
        variant_engine_name=variant_engine_name,
        gcs_path=gcs_path,
        urls_df=urls_df,
        status="success",
        message=f"Generated URLs for {len(urls_df) if urls_df is not None else 0} queries. GCS path: {gcs_path if gcs_path else 'Not found'}"
    )
