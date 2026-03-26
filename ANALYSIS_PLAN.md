# L1 Ranker Recall Comparison - Clean Analysis Plan

## Starting Point
✅ **Enriched Data Available:** `qip_scores_enriched.parquet`
- 83,555 rows, 100 queries, 3 engines
- Query intents from Perceive API (brand, color, gender, product_type)
- Item attributes joined (product_type, brand, color, gender, title, description)

## Engines to Compare
1. **Control (baseline):** `control`
2. **Variant 1:** `nlfv3_alp1_utbeta05_w0_4`
3. **Variant 2:** `nltrain_alp1_w0_4`

## Analysis Workflow

### Step 1: Filter Relevant Queries (recall_analyser logic)
**Goal:** Find queries where comparison is valid

**Criteria:**
- Query exists in BOTH control and variant
- `min_total >= 400` items per query
- `|total_ctrl - total_var| <= 5` (similar total counts)

**Output:** Filtered dataset with comparable queries

**Script:** `filter_for_comparison.py`

### Step 2: Identify Queries with Performance Differences
**Goal:** Focus on queries where variants differ from control

**Metrics to compute per query:**
- Count of 4s (highly-rated items)
- Count of 1s/2s (low-rated items)
- Gain/loss: variant_4s - control_4s

**Segments:**
- **4s_gain:** Queries where variant has MORE 4s than control
- **4s_loss:** Queries where variant has FEWER 4s than control
- **no_change:** Similar performance

**Output:** Query-level summary with performance segments

**Script:** `segment_queries_by_performance.py`

### Step 3: Create QI Pairs for Analysis
**Goal:** Side-by-side comparison of (query, item) pairs

**For each comparison (control vs variant):**
- **4s added:** Items rated 4 in variant but not in control results
- **4s removed:** Items rated 4 in control but not in variant results
- **Rating changes:** Items in both with different ratings

**Categorize queries by intent richness:**
- **no_attributes:** 0 brand/color/gender intents
- **few_attributes:** 1 brand/color/gender intent
- **many_attributes:** 2+ brand/color/gender intents

**Output:** QI pairs parquet with comparison metadata

**Script:** `create_comparison_pairs.py`

### Step 4: Analyze Attribute Matching Patterns
**Goal:** Understand if variants leverage query intents better

**Questions:**
1. Do variants with MORE 4s have better query-item attribute alignment?
2. Are 4s added in variant more relevant to query intents?
3. Are 4s removed from variant actually mismatched?

**Analysis:**
- Compare query brand_intent with item brand
- Compare query color_intent with item color
- Compare query gender_intent with item gender
- Compare query product_type_intent with item product_type

**Output:** Insights on attribute matching effectiveness

**Script:** `analyze_attribute_matching.py`

### Step 5: Generate Report
**Goal:** Summarize findings

**Include:**
- Overall performance comparison (control vs variant 1 vs variant 2)
- Top queries with largest 4s gains/losses
- Attribute matching effectiveness
- Examples of good/bad changes
- Recommendations

**Script:** `generate_comparison_report.py`

## Questions to Answer

1. **Which variant performs better overall?**
   - Total 4s added vs removed
   - Across how many queries?

2. **What types of queries benefit most from variants?**
   - Generic queries (no attributes)?
   - Specific queries (many attributes)?

3. **Is the performance improvement due to better attribute matching?**
   - Do 4s added have better alignment with query intents?
   - Or is it due to other ranking signals?

4. **Are there patterns in what gets removed?**
   - Are removed items actually irrelevant?
   - Or are we losing valid results?

## Next Steps

Would you like me to:
1. ✅ **Proceed with Step 1:** Filter relevant queries using recall_analyser logic
2. ⏭️ **Skip to a specific step:** If you want to focus on something specific
3. 🔄 **Modify the plan:** If you want different analysis

Let me know and I'll implement the clean workflow!
