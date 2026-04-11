CHEMLENS benchmark v2 expansion patch

적용 파일:
- backend/run_named_reaction_benchmark_small.py
- backend/BENCHMARK_SMALL_README.md
- backend/benchmark/named_reaction_benchmark_small.json
- backend/benchmark/named_reaction_benchmark_small_results.csv
- backend/benchmark/named_reaction_benchmark_small_results.json
- backend/benchmark/named_reaction_benchmark_small_report.md

핵심 변경:
- benchmark case 수 9 -> 18
- family coverage 14개 family로 확대
- markdown report 자동 생성 지원
- unique_families 요약 추가
- acceptable_top1 / acceptable_top3 확장 필드 지원

실행 권장:
conda activate chemlens
cd C:\chemlens\backend
python run_named_reaction_benchmark_small.py
