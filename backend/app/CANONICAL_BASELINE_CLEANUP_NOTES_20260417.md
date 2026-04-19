# Canonical Baseline Cleanup Notes (2026-04-17)

이번 패치는 **새 데이터를 더 넣는 패치가 아니라, 기준본 봉인 전용 정리 패치**입니다.

## 이번 정리에서 한 일
- `manual_page_knowledge` / `manual_page_entities`에서 아래 source_label을 제거했습니다.
  - `named_reactions_frontmatter_batch1`
  - `named_reactions_frontmatter_batch2`
  - `named_reactions_frontmatter_batch3`
  - `named_reactions_frontmatter_batch4`
  - `named_reactions_frontmatter_batch59`

## 제거 이유
### 1) batch1~4
- p14~p53 구간에서 실제 named reaction 본문 페이지와 **page_no가 겹치는 잘못된 frontmatter 레이어**였습니다.
- 제거 후 `manual_page_knowledge`는 **page_no 중복이 0개**가 됩니다.

### 2) batch59
- 현재 batch59는 원래 메워야 했던 p502~p553 누락 구간을 **잘못된 appendix/reference 데이터로 채운 상태**였습니다.
- 실제 이미지 샘플 기준 p502, p520, p540, p553은 각각 Swern oxidation, Vilsmeier-Haack formylation, Wittig-Schlosser modification, Yamaguchi macrolactonization 계열로 확인되므로, 현재 batch59 내용은 기준본에 두면 안 됩니다.
- 따라서 **잘못 들어간 batch59를 제거하고, p502~p553은 다시 비워 둔 clean baseline**으로 되돌렸습니다.

## 정리 후 상태
### app/labint.db
- manual_page_knowledge: 757
- manual_page_entities: 1849
- distinct pages: 757
- duplicate pages: 0
- missing p502~p553: 52
- queryable: 368
- tier1: 282
- tier2: 86

### app/labint_round9_bridge_work.db
- manual_page_knowledge: 757
- manual_page_entities: 1849
- distinct pages: 757
- duplicate pages: 0
- missing p502~p553: 52

## 다음 순서
1. 이 clean baseline을 기준본으로 봉인
2. p502~p553은 **실제 이미지 기반으로 새 batch59 재작성**
3. 그 다음에 STEP2 PubChem backfill(71개 추가분)까지 포함한 구조검색 확장분을 별도 패치로 다시 얹기

즉, 지금 이 패치는 **기준본 정리용**이고, 다음 패치가 **정확한 batch59 복원 + 구조검색 확장분 재적용**입니다.
