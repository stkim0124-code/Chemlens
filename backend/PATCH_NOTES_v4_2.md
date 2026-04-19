# PATCH NOTES v4.2

## 이번 패치의 핵심
1. **Gate benchmark 분리**
   - `named_reaction_benchmark_gate.json` 추가
   - 자동화의 baseline/rollback 판정은 이제 gate benchmark만 사용
   - 기존 `named_reaction_benchmark_v4.json`은 diagnostic 용도

2. **Baseline floor 추가**
   - gate benchmark baseline이 `top1/top3 >= 0.99`를 만족하지 않으면 자동화 시작 전에 즉시 중단

3. **Candidate selector 효율화**
   - 이미 family coverage에 기여한 family는 기본적으로 제외
   - uncovered family + extract_rows 0 family를 우선순위 상단으로 올림

4. **Merge 대상 확장**
   - `gemini_auto_seed` 뿐 아니라 `deterministic_gemini_seed`도 병합
   - reaction_extracts는 새 seed molecule이 실제로 참조하는 extract만 병합

## 기대 효과
- v4 expanded benchmark가 너무 무거워 baseline이 낮아지는 문제를 피함
- plan-only 후보 목록이 coverage 확장 중심으로 바뀜
- merge 시 deterministic lane 산출물이 누락되지 않음
