# CHEMLENS Family Completion — 자동화 파이프라인

이 문서는 phase16 이후 shallow-family 완성 작업을 **사용자가 anaconda에서 daemon 1회 실행하는 것 외에는 개입 없이** 자동으로 진행하기 위한 배치 설명입니다.

## 전체 그림

```
(Claude, 이 대화창)                          (사용자 anaconda prompt)
  │                                            │
  ├─ phase_queue/inbox/ 에                   python rdkit_daemon.py
  │   complete_family_evidence_phaseN.py 를     │  (한 번만 실행, 창 최소화)
  │   드롭                                       │
  │                                             ├─ inbox 폴링 (5초 주기)
  │                                             ├─ 스크립트 실행 (RDKit 포함)
  │                                             ├─ verifier + dashboard 자동 실행
  │                                             └─ results/*.result.json 생성
  │
  └─ results/*.result.json 읽고 → phaseN+1 드롭
       (shallow=0 or fail-streak=3 도달 시 중단)
```

## 파일 목록 (Claude가 이미 생성)

| 파일 | 역할 |
|---|---|
| `smiles_guard.py` | RDKit-free SMILES 사전 검증기 (apply 스크립트에서 호출) |
| `rdkit_daemon.py` | 백그라운드 실행기 (사용자가 anaconda에서 1회 launch) |
| `phase_queue/inbox/complete_family_evidence_phase16_shallow_top10.py` | phase16 apply 스크립트 (daemon이 집어서 실행) |
| `phase_queue/results/*.result.json` | daemon이 실행 결과 JSON (Claude가 읽음) |
| `phase_queue/logs/*.log` | 실행 로그 (상세 진단용) |
| `phase_queue/processed/pass|fail/` | 처리 완료된 apply 스크립트 보관 |

## 사용자 개입 (단 1회)

anaconda prompt에서:

```
(rdkit) > cd C:\...\chemlens\backend
(rdkit) > python rdkit_daemon.py
```

그리고 그 창을 **최소화만** 하면 끝. 이후로는 이 대화창만 주기적으로 확인하시면 됩니다.

## 중단 조건 (Claude 측)

- `shallow_count_canonicalized == 0` (shallow 바닥남)
- 같은 family에서 hotfix 3회 연속 실패 (대화창에 보고 후 중지)
- 새롭게 발생한 RDKit 파싱 오류 패턴이 `smiles_guard.py`의 블랙리스트에 반영될 수 있도록 하는 성장 구조

## 복구 시나리오

**컴퓨터 재부팅 시**: 다음 작업 세션 때 anaconda 창 하나 열고 `python rdkit_daemon.py` 한 줄 재실행.

**daemon 창 실수로 닫음**: 위와 동일.

**apply 스크립트가 RDKit 파싱 실패로 FAIL**: Claude가 `results/*.result.json`을 읽고, 문제의 SMILES 패턴을 `smiles_guard.py` 블랙리스트에 추가 + 해당 family의 보수화된 seed로 hotfix 스크립트 재생성 → inbox 드롭.

**daemon 로그 궁금할 때**: `phase_queue/logs/phaseN_타임스탬프_*.log` 확인.

## 옵션: 완전 상시 실행 (computer 재부팅 대응)

Windows 작업 스케줄러에 `rdkit_daemon.py`를 "로그온 시 시작"으로 등록하면 재부팅 후에도 자동 복구됩니다. 등록 PS1 스크립트가 필요하시면 Claude에게 요청하세요.
