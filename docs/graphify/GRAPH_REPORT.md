# Graph Report - airflow_reports  (2026-10-05)

## Corpus Check
- Corpus is ~18,262 words - fits in a single context window. You may not need a graph.

## Summary
- 41 nodes · 64 edges · 6 communities (0 shown, 6 thin omitted)
- Extraction: 92% EXTRACTED · 8% INFERRED · 0% AMBIGUOUS · INFERRED: 5 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- fixVersionConfluence.py
- fixVersionConfluence2wk.py
- fixVersionConfluence30days.py
- fixVersionConfluence1wk.py
- fixVersionConfluence90days.py

## God Nodes (most connected - your core abstractions)
1. `base_function()` - 3 edges
2. `base_function()` - 3 edges
3. `base_function()` - 3 edges
4. `base_function()` - 3 edges
5. `base_function()` - 3 edges
6. `fix_version()` - 2 edges
7. `fix_version_1wk()` - 2 edges
8. `fix_version_2wk()` - 2 edges
9. `fix_version_30day()` - 2 edges
10. `fix_version_90day()` - 2 edges

## Surprising Connections (you probably didn't know these)
- None detected - all connections are within the same source files.

## Import Cycles
- None detected.

## Communities (6 total, 6 thin omitted)

## Knowledge Gaps
- **6 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Should `graphify_pipeline.py` be split into smaller, more focused modules?**
  _Cohesion score 0.13333333333333333 - nodes in this community are weakly interconnected._