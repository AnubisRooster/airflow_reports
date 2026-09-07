# Graph Report - airflow_reports  (2026-09-07)

## Corpus Check
- Corpus is ~15,576 words - fits in a single context window. You may not need a graph.

## Summary
- 23 nodes · 22 edges · 6 communities (5 shown, 1 thin omitted)
- Extraction: 77% EXTRACTED · 23% INFERRED · 0% AMBIGUOUS · INFERRED: 5 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- base_function()
- base_function()
- base_function()
- base_function()
- base_function()
- graphify_pipeline.py

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

## Communities (6 total, 1 thin omitted)

### Community 0 - "base_function()"
Cohesion: 0.67
Nodes (3): base_function(), fix_version(), dag

### Community 1 - "base_function()"
Cohesion: 0.67
Nodes (3): base_function(), fix_version_1wk(), dag

### Community 2 - "base_function()"
Cohesion: 0.67
Nodes (3): base_function(), fix_version_2wk(), dag

### Community 3 - "base_function()"
Cohesion: 0.67
Nodes (3): base_function(), fix_version_30day(), dag

### Community 4 - "base_function()"
Cohesion: 0.67
Nodes (3): base_function(), fix_version_90day(), dag

## Knowledge Gaps
- **1 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Not enough signal to generate questions. This usually means the corpus has no AMBIGUOUS edges, no bridge nodes, no INFERRED relationships, and all communities are tightly cohesive. Add more files or run with --mode deep to extract richer edges._