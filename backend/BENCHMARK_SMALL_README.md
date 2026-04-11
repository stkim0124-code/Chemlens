# CHEMLENS small benchmark v3

이 benchmark는 named reaction 검색 회귀 확인용 소형 세트입니다.

## v3에서 달라진 점

- 케이스 수: **18 -> 27**
- family coverage: **14 -> 16**
- 2차 확장(v2)에 이어, **동일 family의 2nd/3rd exact extract**를 더 넣어 가족 내부 안정성을 확인합니다.
- `Barton-Mccombie Radical Deoxygenation Reaction`에 대해 **boundary case 1개**를 추가했습니다.
  - 현재 엔진은 이 케이스에서 neighbor family인 `Barton Radical Decarboxylation Reaction`을 더 먼저 반환할 수 있습니다.
  - 그래서 이 케이스는 `acceptable_top1` / `acceptable_top3`를 사용합니다.
  - 목적은 회귀 검출과 경계 추적이지, 현재 엔진을 억지로 불합격 처리하는 것이 아닙니다.

## 실행 방법

반드시 CHEMLENS conda 환경에서 실행하세요.

```bash
conda activate chemlens
cd C:\chemlensackend
python run_named_reaction_benchmark_small.py
```

## 출력 파일

- `benchmark/named_reaction_benchmark_small_results.csv`
- `benchmark/named_reaction_benchmark_small_results.json`
- `benchmark/named_reaction_benchmark_small_report.md`

## 해석 포인트

- `top1_accuracy`: 기본 회귀 안정성
- `top3_accuracy`: 상위 후보군 포착 성능
- `disallow_top3_violations`: 명시적으로 금지한 family가 상위권으로 침범했는지
- `acceptance_override_used`: boundary case 여부

## v3 목표

이제 benchmark는 단순히 “맞는 family가 뜨는가”만 보는 것이 아니라,

1. family aggregation / dedup 이후에도 회귀가 안정적인지
2. coarse reaction-type gating 이후에도 경계 family에서 과도한 붕괴가 없는지
3. 일부 boundary case를 추적하면서도 전체 시스템을 불필요하게 불합격 처리하지 않는지

를 함께 확인하는 용도입니다.
