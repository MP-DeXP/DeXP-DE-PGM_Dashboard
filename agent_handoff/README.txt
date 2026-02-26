APF Insight CSV Agent Handoff Package

Included files:
- APF_INSIGHT_CSV_AGENT_PROMPT.md
- apf_insight_generation_spec.json
- generate_insight_csvs.py
- validate_insight_csvs.py

Recommended flow:
1) Read APF_INSIGHT_CSV_AGENT_PROMPT.md
2) Generate 5 output csv files using spec json
3) Validate output:
   python3 validate_insight_csvs.py --dir <output_dir>
