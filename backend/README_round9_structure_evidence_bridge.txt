# round9_structure_evidence_bridge_patch

이 패치는 `named reactions.zip` 한 묶음에 대해:

1. 봉인본 `labint_round9_v5_final_staging.db`는 그대로 두고
2. 메인 `labint.db`를 복사한 작업용 DB `labint_round9_bridge_work.db`를 만들고
3. `page_images / scheme_candidates / reaction_extracts`를 붙인 뒤
4. `extract_molecules` 브릿지 테이블을 로컬-only 휴리스틱으로 구축하고
5. 구조검색 화면 아래에 Named Reaction Evidence 패널을 붙입니다.

## 포함 파일
- backend/build_round9_structure_evidence_bridge.py
- backend/run_build_round9_structure_evidence_bridge.bat
- backend/run_backend_round9_bridge.bat
- backend/app/evidence_search.py
- backend/app/main.py
- backend/app/labint_round9_bridge_work.db
- frontend/src/App.jsx
- frontend/src/components/EvidencePanel.jsx

## 권장 적용 위치
`C:\chemlens` 프로젝트 루트에 덮어쓰기

## 실행 순서
1. 압축 해제 후 `C:\chemlens`에 덮어쓰기
2. 필요하면 `backend\run_build_round9_structure_evidence_bridge.bat` 실행
3. `backend\run_backend_round9_bridge.bat` 실행
4. 프론트는 기존처럼 `C:\chemlens\frontend`에서 `npm run dev`
5. 구조검색 탭에서 구조를 그린 뒤 `Apply` 또는 `검색`

## 확인 URL
- stats: `http://127.0.0.1:8000/api/search/structure-evidence/stats`
- family test: `http://127.0.0.1:8000/api/search/structure-evidence?family=aldol`
