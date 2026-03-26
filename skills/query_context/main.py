"""
Main logic for Query Context Enrichment skill.

Extracts structured query intent annotations from Perceive API:
- Product types, brands, colors, gender, categories
- Synonyms and attribute values with scores
- Used for item-query attribute matching analysis
"""

import asyncio
import aiohttp
import pandas as pd
import logging
import json
from urllib.parse import quote
from aiohttp import ClientSession, ClientTimeout
from typing import List, Dict, Any
import random

from .config import QueryContextInput, QueryContextOutput

logger = logging.getLogger(__name__)


# Category to vertical mapping
CAT_VERTICAL_MAPPING = {
    "5438": "Food",
    "976759": "Home",
    "1085666": "Clothing",
    "4044": "Electronics",
    "1072864": "Baby",
    "976760": "Garden",
    "5427": "Health",
    "4125": "Toys",
    "1229749": "Office",
    "4044_4125": "Tech",
}


PERCEIVE_HEADERS = {
    "tenant-id": "elh9ie",
    "Accept-Language": "en-US"
}


def extract_query_intent_attributes(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract structured query intent attributes from Perceive API response.

    Extracts:
    - product_type: List of product types with scores
    - brand: List of brands with scores
    - color: List of colors with scores
    - gender: List of genders with scores
    - category: List of categories
    - synonyms: Query synonyms
    - And other attributes from queryIntent.intents.annotations

    Args:
        data: Perceive API response JSON

    Returns:
        Dictionary with structured attributes
    """
    attributes = {
        "product_type": [],
        "brand": [],
        "color": [],
        "gender": [],
        "category": [],
        "size": [],
        "material": [],
        "pattern": [],
        "style": [],
        "age_group": [],
        "occasion": [],
        "synonyms": [],
        "other_attributes": {}
    }

    # Extract from queryIntent.intents.annotations
    intents = data.get("queryIntent", {}).get("intents", [])

    for intent in intents:
        annotations = intent.get("annotations", [])

        for ann in annotations:
            attribute = ann.get("attribute", "").lower()

            # Helper function to add attribute value
            def add_attribute_value(attr_name, value, score=1.0):
                if not value:
                    return
                if attr_name == "product_type":
                    attributes["product_type"].append({"value": value, "score": score})
                elif attr_name == "brand":
                    attributes["brand"].append({"value": value, "score": score})
                elif attr_name == "color":
                    attributes["color"].append({"value": value, "score": score})
                elif attr_name == "gender":
                    attributes["gender"].append({"value": value, "score": score})
                elif attr_name == "category":
                    attributes["category"].append({"value": value, "score": score})
                elif attr_name == "size":
                    attributes["size"].append({"value": value, "score": score})
                elif attr_name == "material":
                    attributes["material"].append({"value": value, "score": score})
                elif attr_name == "pattern":
                    attributes["pattern"].append({"value": value, "score": score})
                elif attr_name == "style":
                    attributes["style"].append({"value": value, "score": score})
                elif attr_name == "age_group":
                    attributes["age_group"].append({"value": value, "score": score})
                elif attr_name == "occasion":
                    attributes["occasion"].append({"value": value, "score": score})
                elif attr_name == "synonym":
                    attributes["synonyms"].append(value)
                else:
                    # Store other attributes
                    if attr_name not in attributes["other_attributes"]:
                        attributes["other_attributes"][attr_name] = []
                    attributes["other_attributes"][attr_name].append({"value": value, "score": score})

            # 1. Extract from catalogMappings (has values and scores)
            catalog_mappings = ann.get("catalogMappings", [])
            for mapping in catalog_mappings:
                values = mapping.get("values", [])
                for val in values:
                    name = val.get("name")
                    score = val.get("score", 0)
                    add_attribute_value(attribute, name, score)

            # 2. Extract direct value from annotation (fallback if no catalogMappings)
            if not catalog_mappings:
                direct_value = ann.get("value")
                if direct_value:
                    add_attribute_value(attribute, direct_value, 1.0)

    # Also extract from qcResult.categories
    qc = data.get("qcResult", {})
    categories = qc.get("categories", [])
    for cat in categories:
        cat_id = cat.get("id")
        cat_name = cat.get("name")
        if cat_name:
            attributes["category"].append({"value": cat_name, "score": 1.0, "id": cat_id})

    # Deduplicate and sort by score
    for key in ["product_type", "brand", "color", "gender", "category", "size",
                "material", "pattern", "style", "age_group", "occasion"]:
        if attributes[key]:
            # Deduplicate by value, keeping highest score
            unique_dict = {}
            for item in attributes[key]:
                val = item["value"]
                score = item.get("score", 0)
                if val not in unique_dict or score > unique_dict[val]["score"]:
                    unique_dict[val] = item

            # Sort by score descending
            attributes[key] = sorted(unique_dict.values(), key=lambda x: x.get("score", 0), reverse=True)

    return attributes


def clean_query_for_perceive(query: str) -> str:
    """
    Clean query by removing store/zipcode parameters.

    Examples:
        "valentines day pajamas for women (stores=1327, zipcode=47037)"
        -> "valentines day pajamas for women"

        "milk (facet=fulfillment_method:Delivery, stores=1714, zipcode=26554)"
        -> "milk"

    Args:
        query: Raw query string

    Returns:
        Cleaned query without store/zipcode parameters
    """
    import re
    # Remove everything in parentheses that contains "stores=" or "zipcode=" or "facet="
    cleaned = re.sub(r'\s*\([^)]*(?:stores=|zipcode=|facet=)[^)]*\)\s*', '', query)
    return cleaned.strip()


async def fetch_query_context(
    query: str,
    session: ClientSession,
    sem: asyncio.Semaphore,
    retry_limit: int = 5,
    timeout_seconds: int = 5,
    include_pt_features: bool = True
) -> Dict:
    """
    Fetch query context from Perceive API.

    Args:
        query: Search query string (may include store/zipcode parameters)
        session: aiohttp session
        sem: Semaphore for concurrency control
        retry_limit: Max retries
        timeout_seconds: Request timeout
        include_pt_features: Extract product type features

    Returns:
        Dictionary with query context and structured attributes
    """
    # Clean query to remove store/zipcode parameters for consistent Perceive results
    cleaned_query = clean_query_for_perceive(query)

    url = f"http://perceive-gm-wcnp.prodb.walmart.com/perceive/v2/modular/iu?query={quote(cleaned_query, safe='')}&includeQC=true"

    for attempt in range(1, retry_limit + 1):
        try:
            async with sem:
                async with session.get(
                    url,
                    headers=PERCEIVE_HEADERS,
                    ssl=False,
                    timeout=ClientTimeout(total=timeout_seconds)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"⚠️ Non-200 ({resp.status}) for '{query}' (attempt {attempt})")
                        await asyncio.sleep(2)
                        continue

                    data = await resp.json()

                    # Extract basic query features
                    sem_data = data.get("semanticity", {})
                    scount = sem_data.get("sCount")
                    bcount = sem_data.get("bCount")
                    acount = sem_data.get("aCount")

                    trig = data.get("triggeringSignals", {}).get("specificity", {})
                    specificity = trig.get("score")

                    traffic = data.get("trafficInfo", {})
                    try:
                        q_from = float(traffic.get("quantileFrom", -1))
                        q_to = float(traffic.get("quantileTo", -1))
                        segment = (q_from + q_to) / 2 if q_from >= 0 and q_to >= 0 else None
                    except Exception:
                        segment = None

                    qc = data.get("qcResult", {})
                    try:
                        cat_path = qc.get("categories", [{}])[0].get("id", "")
                        l1_category = cat_path.split('/')[1] if '/' in cat_path else None
                        vertical = CAT_VERTICAL_MAPPING.get(l1_category)
                    except Exception:
                        l1_category = None
                        vertical = None

                    # Extract structured query intent attributes
                    intent_attrs = extract_query_intent_attributes(data)

                    # Serialize attributes to JSON strings for DataFrame storage
                    result = {
                        "query": query,
                        "scount": scount,
                        "bcount": bcount,
                        "acount": acount,
                        "specificity": specificity,
                        "segment": segment,
                        "l1_category": l1_category,
                        "vertical": vertical,
                        # Store structured attributes as JSON strings
                        "product_type_intent": json.dumps(intent_attrs["product_type"]),
                        "brand_intent": json.dumps(intent_attrs["brand"]),
                        "color_intent": json.dumps(intent_attrs["color"]),
                        "gender_intent": json.dumps(intent_attrs["gender"]),
                        "category_intent": json.dumps(intent_attrs["category"]),
                        "size_intent": json.dumps(intent_attrs["size"]),
                        "material_intent": json.dumps(intent_attrs["material"]),
                        "pattern_intent": json.dumps(intent_attrs["pattern"]),
                        "style_intent": json.dumps(intent_attrs["style"]),
                        "age_group_intent": json.dumps(intent_attrs["age_group"]),
                        "occasion_intent": json.dumps(intent_attrs["occasion"]),
                        "synonyms": json.dumps(intent_attrs["synonyms"]),
                        "other_attributes": json.dumps(intent_attrs["other_attributes"]),
                        "status": "success"
                    }

                    # Add summary counts
                    result["n_product_types"] = len(intent_attrs["product_type"])
                    result["n_brands"] = len(intent_attrs["brand"])
                    result["n_colors"] = len(intent_attrs["color"])
                    result["n_genders"] = len(intent_attrs["gender"])
                    result["n_categories"] = len(intent_attrs["category"])

                    return result

        except Exception as e:
            logger.warning(f"❌ Exception for '{query}' on attempt {attempt}: {e}")
            await asyncio.sleep(random.uniform(1, 3))

    # Fallback if all retries fail
    return {
        "query": query,
        "scount": None,
        "bcount": None,
        "acount": None,
        "specificity": None,
        "segment": None,
        "l1_category": None,
        "vertical": None,
        "product_type_intent": "[]",
        "brand_intent": "[]",
        "color_intent": "[]",
        "gender_intent": "[]",
        "category_intent": "[]",
        "size_intent": "[]",
        "material_intent": "[]",
        "pattern_intent": "[]",
        "style_intent": "[]",
        "age_group_intent": "[]",
        "occasion_intent": "[]",
        "synonyms": "[]",
        "other_attributes": "{}",
        "n_product_types": 0,
        "n_brands": 0,
        "n_colors": 0,
        "n_genders": 0,
        "n_categories": 0,
        "status": "failed"
    }


async def fetch_all_queries(
    queries: List[str],
    concurrency_limit: int,
    retry_limit: int,
    timeout_seconds: int,
    include_pt_features: bool
) -> List[Dict]:
    """
    Fetch context for all queries asynchronously.

    Args:
        queries: List of query strings
        concurrency_limit: Max concurrent requests
        retry_limit: Max retries per query
        timeout_seconds: Request timeout
        include_pt_features: Include product type features

    Returns:
        List of query context dictionaries
    """
    sem = asyncio.Semaphore(concurrency_limit)

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_query_context(
                q, session, sem, retry_limit, timeout_seconds, include_pt_features
            )
            for q in queries
        ]
        return await asyncio.gather(*tasks)


def run(input_config: QueryContextInput) -> QueryContextOutput:
    """
    Main entry point for Query Context Enrichment skill.

    Args:
        input_config: QueryContextInput configuration

    Returns:
        QueryContextOutput with enriched query data
    """
    logger.info("Starting Query Context Enrichment")

    # Extract unique queries from input
    if isinstance(input_config.queries, pd.DataFrame):
        df = input_config.queries
        if 'query' not in df.columns and 'contextualQuery' in df.columns:
            query_col = 'contextualQuery'
        else:
            query_col = 'query'

        unique_queries = df[query_col].dropna().unique().tolist()
        logger.info(f"Extracted {len(unique_queries)} unique queries from DataFrame")
    else:
        unique_queries = list(set(input_config.queries))
        logger.info(f"Processing {len(unique_queries)} unique queries")

    # Fetch query context asynchronously
    logger.info(f"Fetching query context (concurrency: {input_config.concurrency_limit})...")
    results = asyncio.run(
        fetch_all_queries(
            unique_queries,
            input_config.concurrency_limit,
            input_config.retry_limit,
            input_config.timeout_seconds,
            input_config.include_pt_features
        )
    )

    # Convert to DataFrame
    result_df = pd.DataFrame(results)

    # Count successes and failures
    queries_processed = (result_df['status'] == 'success').sum()
    queries_failed = (result_df['status'] == 'failed').sum()

    # Extract feature names (exclude status and query)
    features_extracted = [col for col in result_df.columns if col not in ['query', 'status']]

    logger.info(f"✓ Processed {queries_processed} queries successfully")
    if queries_failed > 0:
        logger.warning(f"⚠ Failed to process {queries_failed} queries")

    # If input was DataFrame, merge the results back
    if isinstance(input_config.queries, pd.DataFrame):
        # Drop status column before merging
        merge_df = result_df.drop(columns=['status'])

        if query_col == 'contextualQuery':
            # Rename 'query' to 'contextualQuery' for merging
            merge_df = merge_df.rename(columns={'query': 'contextualQuery'})

        enriched_df = input_config.queries.merge(
            merge_df,
            on=query_col,
            how='left'
        )
    else:
        enriched_df = result_df.drop(columns=['status'])

    return QueryContextOutput(
        enriched_df=enriched_df,
        queries_processed=queries_processed,
        queries_failed=queries_failed,
        features_extracted=features_extracted
    )
