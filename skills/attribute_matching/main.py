#!/usr/bin/env python3
"""
Skill 3: Attribute Matching Analysis

Analyzes alignment between query intents and item attributes to understand
why variant adds certain 4s and removes certain non-4s.

Key questions:
1. Do 4s added by variant have better attribute matching than items they replace?
2. Do non-4s removed have poor attribute matching (mismatches)?
3. What patterns exist in text matching (title/description)?

Input: QI pairs DataFrame with change_type, query intents, item attributes
Output: Matching scores and analysis for each pair
"""

import pandas as pd
import numpy as np
import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Set
import re

# Try to import NLTK stemmer
try:
    from nltk.stem import PorterStemmer
    from nltk.tokenize import word_tokenize
    import nltk
    # Download required data if not present
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        nltk.download('punkt', quiet=True)
    STEMMER_AVAILABLE = True
except ImportError:
    STEMMER_AVAILABLE = False
    print("⚠️  NLTK not available, using simple tokenization")


@dataclass
class AttributeMatchingInput:
    """Input configuration for attribute matching analysis."""
    pairs_df: pd.DataFrame
    query_col: str = 'contextualQuery'
    focus_change_types: List[str] = None  # e.g., ['4_gained', 'non4_removed']

    def __post_init__(self):
        if self.focus_change_types is None:
            self.focus_change_types = ['4_gained', 'non4_removed', '4_lost']


@dataclass
class AttributeMatchingOutput:
    """Output from attribute matching analysis."""
    pairs_with_scores: pd.DataFrame
    summary_stats: Dict[str, Any]
    insights: List[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'pairs_count': len(self.pairs_with_scores),
            'summary_stats': self.summary_stats,
            'insights': self.insights
        }


class AttributeMatchingSkill:
    """Analyze attribute matching between query intents and item attributes."""

    def __init__(self, verbose: bool = False):
        self.name = "attribute_matching"
        self.version = "1.0.0"
        self.verbose = verbose

        # Initialize stemmer if available
        if STEMMER_AVAILABLE:
            self.stemmer = PorterStemmer()
            if verbose:
                print("✅ Using NLTK Porter Stemmer for text matching")
        else:
            self.stemmer = None
            if verbose:
                print("⚠️  NLTK not available, using simple suffix-based stemming")

    def tokenize_and_stem(self, text: str) -> Set[str]:
        """
        Tokenize text and apply stemming.

        Returns set of stemmed tokens (lowercase).
        """
        if not text or pd.isna(text):
            return set()

        text = str(text).lower()

        # Remove special characters and extra spaces
        text = re.sub(r'[^\w\s]', ' ', text)

        if self.stemmer and STEMMER_AVAILABLE:
            # Use NLTK tokenization and stemming
            try:
                tokens = word_tokenize(text)
                stemmed = {self.stemmer.stem(token) for token in tokens if len(token) > 2}
                return stemmed
            except:
                # Fallback to simple tokenization
                pass

        # Simple tokenization fallback
        tokens = text.split()
        # Simple stemming: remove common suffixes
        stemmed = set()
        for token in tokens:
            if len(token) <= 2:
                continue
            # Remove common suffixes
            for suffix in ['ing', 'ed', 'es', 's', 'er', 'ly']:
                if token.endswith(suffix) and len(token) > len(suffix) + 2:
                    token = token[:-len(suffix)]
                    break
            stemmed.add(token)

        return stemmed

    def extract_query_tokens(self, query: str, query_intents: List[str]) -> Set[str]:
        """
        Extract tokens from query text and query intents.

        Combines:
        - Tokens from the raw query string
        - Tokens from all query intent values

        Returns set of stemmed tokens.
        """
        all_tokens = set()

        # Extract from query string (before parentheses with stores/zipcode)
        if query and not pd.isna(query):
            # Remove store/zipcode info: "eggs (stores=1660, zipcode=93560)" -> "eggs"
            clean_query = re.sub(r'\s*\([^)]*\)\s*$', '', str(query))
            all_tokens.update(self.tokenize_and_stem(clean_query))

        # Extract from intent values
        for intent in query_intents:
            all_tokens.update(self.tokenize_and_stem(intent))

        return all_tokens

    def parse_intent_list(self, intent_str: str) -> List[Dict[str, Any]]:
        """Parse JSON intent string into list of dicts."""
        if pd.isna(intent_str) or not intent_str or intent_str == '[]':
            return []

        try:
            intents = json.loads(intent_str)
            return intents if isinstance(intents, list) else []
        except:
            return []

    def extract_intent_values(self, intent_str: str) -> List[str]:
        """Extract just the values from intent JSON."""
        intents = self.parse_intent_list(intent_str)
        return [intent.get('value', '').lower() for intent in intents if intent.get('value')]

    def compute_intent_match_score(
        self,
        query_intents: List[Dict],  # Changed from List[str] to List[Dict] to include scores
        item_value: Any
    ) -> float:
        """
        Compute intent-based match score between query intents and item attribute.

        Returns the Perceive intent score if there's a match, 0.0 otherwise.
        This allows us to differentiate between high-confidence matches (score 0.95)
        and low-confidence matches (score 0.20).

        Args:
            query_intents: List of intent dicts with 'value' and 'score'
            item_value: Item attribute value to match against

        Returns:
            Intent score (0.0-1.0) if match found, 0.0 otherwise
        """
        if not query_intents or pd.isna(item_value):
            return 0.0

        # Handle item_value being a list (e.g., brand: ['Nike'])
        item_values = []
        if isinstance(item_value, list):
            item_values = [str(v).lower() for v in item_value]
        else:
            item_values = [str(item_value).lower()]

        # Check for exact match and return the intent score
        for intent in query_intents:
            intent_value = intent.get('value', '').lower()
            intent_score = intent.get('score', 0.0)

            for item_val in item_values:
                if intent_value == item_val:
                    return intent_score  # Return Perceive intent score instead of 1.0

        return 0.0

    def compute_partial_match_score(
        self,
        query_intents: List[Dict],  # Changed from List[str] to List[Dict]
        item_value: Any
    ) -> float:
        """
        Compute partial match score (substring matching).

        Returns score between 0.0 and 1.0 based on best partial match.
        """
        if not query_intents or pd.isna(item_value):
            return 0.0

        # Handle item_value being a list
        item_values = []
        if isinstance(item_value, list):
            item_values = [str(v).lower() for v in item_value]
        else:
            item_values = [str(item_value).lower()]

        best_score = 0.0
        for intent in query_intents:
            intent_value = intent.get('value', '').lower()
            intent_score = intent.get('score', 0.0)

            for item_val in item_values:
                # Check substring match
                if intent_value in item_val or item_val in intent_value:
                    # Score based on length similarity weighted by intent score
                    min_len = min(len(intent_value), len(item_val))
                    max_len = max(len(intent_value), len(item_val))
                    similarity = min_len / max_len if max_len > 0 else 0.0
                    # Weight by intent score from Perceive
                    score = similarity * intent_score
                    best_score = max(best_score, score)

        return best_score

    def compute_text_match_score(
        self,
        query_tokens: Set[str],
        text: str
    ) -> float:
        """
        Compute text match score using stemmed tokens.

        Tokenizes and stems the text, then computes ratio of query tokens found.

        Args:
            query_tokens: Set of stemmed query tokens
            text: Item title or description

        Returns:
            Ratio of query tokens found in text (0.0 to 1.0)
        """
        if not query_tokens or pd.isna(text) or not text:
            return 0.0

        # Tokenize and stem the text
        text_tokens = self.tokenize_and_stem(text)

        if not text_tokens:
            return 0.0

        # Count matches
        matches = len(query_tokens & text_tokens)  # Intersection

        return matches / len(query_tokens) if len(query_tokens) > 0 else 0.0

    def compute_matching_scores(self, row: pd.Series) -> Dict[str, float]:
        """
        Compute all matching scores for a single QI pair.

        Returns dict with matching scores for different attributes.
        """
        # Determine which column suffix to use
        pair_type = row.get('pair_type', 'unknown')
        if pair_type == 'variant_only':
            suffix = '_var'
        elif pair_type == 'control_only':
            suffix = '_ctrl'
        else:
            suffix = '_var'  # Prefer variant for both_engines

        # Extract query intents (keep full objects with scores, not just values)
        pt_intents = self.parse_intent_list(row.get('product_type_intent', '[]'))
        brand_intents = self.parse_intent_list(row.get('brand_intent', '[]'))
        color_intents = self.parse_intent_list(row.get('color_intent', '[]'))
        gender_intents = self.parse_intent_list(row.get('gender_intent', '[]'))

        # All query intents combined (extract just values for this)
        all_intent_values = [i.get('value', '').lower() for i in pt_intents + brand_intents + color_intents + gender_intents if i.get('value')]

        # Get query text and extract stemmed tokens
        # For title/description matching, use ONLY the raw query text (not intents)
        # since intents are already matched in structured attributes
        query_col = 'contextualQuery' if 'contextualQuery' in row.index else 'query'
        query_text = row.get(query_col, '')

        # Extract tokens from raw query only (remove store/zipcode metadata)
        if query_text and not pd.isna(query_text):
            clean_query = re.sub(r'\s*\([^)]*\)\s*$', '', str(query_text))
            query_tokens = self.tokenize_and_stem(clean_query)
        else:
            query_tokens = set()

        # Get item attributes
        item_pt = row.get(f'product_type{suffix}', row.get('product_type'))
        item_brand = row.get(f'brand{suffix}', row.get('brand'))
        item_color = row.get(f'color{suffix}', row.get('color'))
        item_gender = row.get(f'gender{suffix}', row.get('gender'))
        item_title = row.get(f'title{suffix}', row.get('title', ''))
        item_desc = row.get(f'description{suffix}', row.get('description', ''))

        # Compute scores (now using intent scores instead of binary 0/1)
        scores = {
            # Intent-based matches (returns Perceive intent score if matched)
            'pt_exact_match': self.compute_intent_match_score(pt_intents, item_pt),
            'brand_exact_match': self.compute_intent_match_score(brand_intents, item_brand),
            'color_exact_match': self.compute_intent_match_score(color_intents, item_color),
            'gender_exact_match': self.compute_intent_match_score(gender_intents, item_gender),

            # Partial matches
            'pt_partial_match': self.compute_partial_match_score(pt_intents, item_pt),
            'brand_partial_match': self.compute_partial_match_score(brand_intents, item_brand),
            'color_partial_match': self.compute_partial_match_score(color_intents, item_color),
            'gender_partial_match': self.compute_partial_match_score(gender_intents, item_gender),

            # Text matches (using stemmed tokens)
            'title_match': self.compute_text_match_score(query_tokens, item_title),
            'desc_match': self.compute_text_match_score(query_tokens, item_desc),
        }

        # Overall score (average of exact matches for structured attributes)
        structured_matches = [
            scores['pt_exact_match'],
            scores['brand_exact_match'],
            scores['color_exact_match'],
            scores['gender_exact_match']
        ]

        # Only average non-zero scores (attributes that have intents)
        has_intents = [
            len(pt_intents) > 0,
            len(brand_intents) > 0,
            len(color_intents) > 0,
            len(gender_intents) > 0
        ]

        relevant_scores = [s for s, has in zip(structured_matches, has_intents) if has]
        scores['overall_match'] = np.mean(relevant_scores) if relevant_scores else 0.0

        return scores

    def analyze_pairs(self, input_config: AttributeMatchingInput) -> AttributeMatchingOutput:
        """
        Main analysis function.

        Computes matching scores for all pairs and generates insights.
        """
        df = input_config.pairs_df.copy()

        print("\n" + "="*80)
        print("Attribute Matching Analysis")
        print("="*80)

        # Filter to focus change types if specified
        if input_config.focus_change_types:
            df = df[df['change_type'].isin(input_config.focus_change_types)]
            print(f"\nFocusing on change types: {input_config.focus_change_types}")
            print(f"Pairs to analyze: {len(df):,}")

        # Compute matching scores for each pair
        print("\nComputing matching scores...")
        matching_scores = df.apply(self.compute_matching_scores, axis=1)

        # Convert to DataFrame and join with original
        scores_df = pd.DataFrame(matching_scores.tolist())
        df_with_scores = pd.concat([df.reset_index(drop=True), scores_df], axis=1)

        # Compute summary statistics
        print("\nComputing summary statistics...")
        summary_stats = {}

        for change_type in df_with_scores['change_type'].unique():
            subset = df_with_scores[df_with_scores['change_type'] == change_type]

            summary_stats[change_type] = {
                'count': len(subset),
                'avg_overall_match': subset['overall_match'].mean(),
                'avg_pt_match': subset['pt_exact_match'].mean(),
                'avg_brand_match': subset['brand_exact_match'].mean(),
                'avg_color_match': subset['color_exact_match'].mean(),
                'avg_gender_match': subset['gender_exact_match'].mean(),
                'avg_title_match': subset['title_match'].mean(),
                'avg_desc_match': subset['desc_match'].mean(),
            }

        # Generate insights
        insights = self.generate_insights(df_with_scores, summary_stats)

        return AttributeMatchingOutput(
            pairs_with_scores=df_with_scores,
            summary_stats=summary_stats,
            insights=insights
        )

    def generate_insights(
        self,
        df: pd.DataFrame,
        summary_stats: Dict[str, Any]
    ) -> List[str]:
        """Generate insights from matching analysis."""
        insights = []

        # Compare 4s_gained vs non4_removed
        if '4_gained' in summary_stats and 'non4_removed' in summary_stats:
            gained = summary_stats['4_gained']
            removed = summary_stats['non4_removed']

            match_diff = gained['avg_overall_match'] - removed['avg_overall_match']

            if match_diff > 0.1:
                insights.append(
                    f"✅ 4s added have {match_diff:.1%} BETTER overall attribute matching "
                    f"than non-4s removed ({gained['avg_overall_match']:.1%} vs {removed['avg_overall_match']:.1%})"
                )
            elif match_diff < -0.1:
                insights.append(
                    f"⚠️  4s added have {abs(match_diff):.1%} WORSE overall attribute matching "
                    f"than non-4s removed ({gained['avg_overall_match']:.1%} vs {removed['avg_overall_match']:.1%})"
                )
            else:
                insights.append(
                    f"≈ 4s added have similar attribute matching to non-4s removed "
                    f"({gained['avg_overall_match']:.1%} vs {removed['avg_overall_match']:.1%})"
                )

            # Product type matching
            if gained['avg_pt_match'] > removed['avg_pt_match'] + 0.1:
                insights.append(
                    f"  📦 Product type matching is stronger in 4s added "
                    f"({gained['avg_pt_match']:.1%} vs {removed['avg_pt_match']:.1%})"
                )

            # Brand matching
            if gained['avg_brand_match'] > removed['avg_brand_match'] + 0.1:
                insights.append(
                    f"  🏷️  Brand matching is stronger in 4s added "
                    f"({gained['avg_brand_match']:.1%} vs {removed['avg_brand_match']:.1%})"
                )

            # Title matching
            if gained['avg_title_match'] > removed['avg_title_match'] + 0.1:
                insights.append(
                    f"  📝 Title text matching is stronger in 4s added "
                    f"({gained['avg_title_match']:.1%} vs {removed['avg_title_match']:.1%})"
                )

        # Check if 4s_lost had good matching (shouldn't have been lost)
        if '4_lost' in summary_stats:
            lost = summary_stats['4_lost']
            if lost['avg_overall_match'] > 0.5:
                insights.append(
                    f"⚠️  4s lost had GOOD attribute matching ({lost['avg_overall_match']:.1%}) "
                    f"- variant may be removing relevant items"
                )

        return insights

    def run(self, input_config: AttributeMatchingInput) -> AttributeMatchingOutput:
        """Main entry point for the skill."""
        return self.analyze_pairs(input_config)


def main():
    """Standalone execution for testing."""
    import argparse

    parser = argparse.ArgumentParser(description='Attribute Matching Analysis')
    parser.add_argument(
        '--input',
        type=str,
        default='./temp/downloaded_files/qip_pairs.parquet',
        help='Input QI pairs file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='./temp/downloaded_files/qip_pairs_with_matching.parquet',
        help='Output file with matching scores'
    )

    args = parser.parse_args()

    # Load pairs
    print(f"\nLoading: {args.input}")
    df = pd.read_parquet(args.input)
    print(f"Loaded {len(df):,} pairs")

    # Create input config
    query_col = 'contextualQuery' if 'contextualQuery' in df.columns else 'query'
    input_config = AttributeMatchingInput(
        pairs_df=df,
        query_col=query_col,
        focus_change_types=['4_gained', 'non4_removed', '4_lost']
    )

    # Run skill with verbose output
    skill = AttributeMatchingSkill(verbose=True)
    output = skill.run(input_config)

    # Save results
    output.pairs_with_scores.to_parquet(args.output, index=False)
    print(f"\n💾 Saved {len(output.pairs_with_scores):,} pairs with scores to: {args.output}")

    # Print summary
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    for change_type, stats in output.summary_stats.items():
        print(f"\n{change_type.upper()}:")
        print(f"  Count: {stats['count']:,}")
        print(f"  Overall Match: {stats['avg_overall_match']:.1%}")
        print(f"  Product Type Match: {stats['avg_pt_match']:.1%}")
        print(f"  Brand Match: {stats['avg_brand_match']:.1%}")
        print(f"  Color Match: {stats['avg_color_match']:.1%}")
        print(f"  Gender Match: {stats['avg_gender_match']:.1%}")
        print(f"  Title Match: {stats['avg_title_match']:.1%}")
        print(f"  Description Match: {stats['avg_desc_match']:.1%}")

    # Print insights
    print("\n" + "="*80)
    print("INSIGHTS")
    print("="*80)
    for insight in output.insights:
        print(f"\n{insight}")

    print("\n" + "="*80)
    print("✅ DONE")
    print("="*80)


if __name__ == '__main__':
    main()
