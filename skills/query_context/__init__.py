"""
Skill 2: Query Context Enrichment

Enriches queries with context from Perceive API to understand:
- Query semanticity (scount, bcount, acount)
- Specificity scores
- Traffic segment (quantile)
- L1 category and vertical
- Product type features (max_pt, n_pt)

This helps explain WHY certain queries are impacted by L1 ranker changes.
"""

from .config import QueryContextInput, QueryContextOutput
from .main import run

__all__ = ['run', 'QueryContextInput', 'QueryContextOutput']
