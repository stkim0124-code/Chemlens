Swern Oxidation round1 dry-run patch.

적용:
1) 압축을 C:\chemlensackend 에 풉니다.
2) Anaconda Prompt에서 아래 실행:
   conda activate chemlens
   cd /d C:\chemlensackend
   run_gemini_rebuild_swern_round1.bat

이 패치는 DRY-RUN만 수행합니다.
- Gemini 2.5 Pro를 사용해 Swern Oxidation 후보를 최대 3회 생성
- temp DB에 삽입
- small benchmark guard 실행
- PASS 후보가 있으면 summary JSON에 기록

핵심 결과 파일:
reports\gemini_single_family_rebuild\<timestamp>\gemini_single_family_rebuild_summary.json
