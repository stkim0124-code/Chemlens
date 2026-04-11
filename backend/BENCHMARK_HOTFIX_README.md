# Benchmark runner hotfix

이 패치는 `run_named_reaction_benchmark_small.py`가 현재 셸 환경에 `fastapi`가 없어도 실행되도록 바꿉니다.

## 왜 에러가 났는가
기존 스크립트는 `app/evidence_search.py`를 직접 import하는데, 그 파일 맨 위에서 `fastapi`를 import합니다.
그래서 백엔드 서버는 잘 돌고 있어도, **지금 실행한 Python 환경(base)에 fastapi가 없으면 benchmark가 먼저 죽습니다.**

## 이 패치가 하는 일
- benchmark 실행 시에만 아주 작은 `fastapi` shim을 주입
- API 서버를 띄우는 것이 아니라, benchmark에 필요한 import만 통과시킴
- RDKit / pydantic / DB는 그대로 실제 환경을 사용

## 적용 후 실행
```bash
cd C:\chemlensackend
python run_named_reaction_benchmark_small.py
```

그래도 `pydantic` 또는 `rdkit` 관련 에러가 나면, 그때는 benchmark 문제가 아니라 현재 셸 환경 자체가 백엔드 실행 환경과 다른 것입니다. 그 경우에는 CHEMLENS를 평소 실행하던 conda 환경으로 들어가서 다시 실행하면 됩니다.
