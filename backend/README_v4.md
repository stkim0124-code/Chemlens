# ChemLens v4.4 자동화 실행 가이드

## 이 패치가 바로 고치는 문제
- v4.3에서 `seed=1` 후보가 곧바로 `deterministic failed`로 떨어지던 원인을 수정했습니다.
- 핵심 원인 두 가지를 고쳤습니다.
  1. `reaction_family_patterns.family_name_norm`의 legacy 값 때문에 **covered family를 covered=0으로 오판**하던 문제
  2. deterministic seed 재사용이 `reaction_extracts.reactant_smiles/product_smiles` 텍스트 필드에 의존해 **`scheme_candidate_id` NOT NULL 및 legacy 구분자 문제**로 실패하던 문제

## 준수한 운영 방침
- 목표 family 수: **최대치 (`family_target=250`)**
- Gemini 사용: **deterministic 우선 + 필요 시 Gemini fallback**
- 벤치마크: **gate benchmark + 확장 diagnostic benchmark 동시 사용**
- Gemini 모델: **`gemini-2.5-pro` 고정**
- 디스크 예산: **10GB**, `candidate_backup` 생성 안 함

## 핵심 변경점
1. **coverage lookup 정규화 수정**
   - family coverage 계산은 이제 항상 `_norm(family_name)` 기준으로 비교합니다.
   - 이미 queryable coverage에 기여한 family가 `covered=0`으로 plan 상단에 다시 올라오던 문제가 줄어듭니다.

2. **진짜 deterministic seed 재사용**
   - deterministic lane은 이제 기존 `extract_molecules`의 역할별 분자(`reactant/product/reagent/intermediate`)를 직접 읽어 재사용합니다.
   - legacy `reactant_smiles/product_smiles` 텍스트 필드의 NULL/구분자 문제를 피합니다.
   - `scheme_candidate_id` 등 필수 컬럼을 기존 extract 메타에서 이어받아 INSERT합니다.

3. **merge 대상 확장**
   - `gemini_auto_seed`
   - `deterministic_gemini_seed`
   - `deterministic_seed_from_existing`
   세 source를 모두 canonical 병합 대상으로 포함합니다.

## 실행
```cmd
conda activate chemlens
cd /d C:\chemlensackend
run_v4_automation.bat
```

## 기대되는 정상 plan-only 해석
- 이미 coverage에 들어간 family는 기본적으로 plan 상단에서 사라집니다.
- `seed=1` 후보가 보이면 deterministic 재사용 가능 후보입니다.
- `seed=0` 후보는 Gemini lane으로 가는 것이 정상입니다.

## 기대되는 정상 baseline
```text
top1=1.0 top3=1.0 violations=0
```
그리고 diagnostic benchmark는 별도로 noisy/meaningful 통계를 찍습니다.

## 중단해야 하는 경우
- gate baseline `top1` 또는 `top3`가 0.99 미만
- 동일 run에서 `seed=1` 후보가 연속으로 deterministic 실패
- batch regression이 반복되며 `inserted=0`만 계속 나옴
