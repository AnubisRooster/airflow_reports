# Graph Report - airflow_reports  (2026-09-28)

## Corpus Check
- Corpus is ~18,262 words - fits in a single context window. You may not need a graph.

## Summary
- 41 nodes · 64 edges · 6 communities
- Extraction: 92% EXTRACTED · 8% INFERRED · 0% AMBIGUOUS · INFERRED: 5 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- graphify_pipeline.py
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

## Communities (6 total, 0 thin omitted)

### Community 0 - "graphify_pipeline.py"
Cohesion: 0.13
Nodes (13): graphify_analyze, graphify_build, graphify_cluster, graphify_detect, graphify_export, graphify_extract, graphify_llm, graphify_report (+5 more)

### Community 1 - "fixVersionConfluence.py"
Cohesion: 0.40
Nodes (5): airflow_decorators, airflow_models, base_function(), fix_version(), dag

### Community 2 - "fixVersionConfluence2wk.py"
Cohesion: 0.50
Nodes (4): airflow_operators_python, base_function(), fix_version_2wk(), dag

### Community 3 - "fixVersionConfluence30days.py"
Cohesion: 0.50
Nodes (4): airflow_providers_atlassian_jira_hooks_jira, base_function(), fix_version_30day(), dag

### Community 4 - "fixVersionConfluence1wk.py"
Cohesion: 0.50
Nodes (4): atlassian, base_function(), fix_version_1wk(), dag

### Community 5 - "fixVersionConfluence90days.py"
Cohesion: 0.50
Nodes (4): datetime, base_function(), fix_version_90day(), dag

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Should `graphify_pipeline.py` be split into smaller, more focused modules?**
  _Cohesion score 0.13333333333333333 - nodes in this community are weakly interconnected._