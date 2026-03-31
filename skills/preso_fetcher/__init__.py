"""
Preso Fetcher Skill

Fetches top-40 results from Preso API for control and variant configurations.
Respects rate limiting and handles contextual queries.
"""

from .main import fetch_preso_results, PresoFetcherInput, PresoFetcherOutput

__all__ = ['fetch_preso_results', 'PresoFetcherInput', 'PresoFetcherOutput']
