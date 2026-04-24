# FAMILY COMPLETION PHASE8 SHALLOW TOP5

이번 phase8는 shallow-family 4차 sprint입니다.

대상 canonical family:
1. Cope Elimination / Cope Reaction
2. Corey-Bakshi-Shibata Reduction (CBS Reduction)
3. Corey-Chaykovsky Epoxidation and Cyclopropanation
4. Corey-Fuchs Alkyne Synthesis
5. Corey-Kim Oxidation

이번 패치가 하는 일:
- data-table alias cleanup
- extract_molecules 기준 reaction_extracts reactant/product backfill
- 위 5개 family에 대해 curated application seed 2개씩 추가
- final_state_verifier / family_completion_dashboard도 같이 업데이트
  - phase8 alias collapse 반영
  - recent completed family 반영
  - short/long drift가 본판 truth를 다시 오염시키지 않도록 보정

실행 순서:

conda activate chemlens
cd /d C:\chemlens\backend

python complete_family_evidence_phase8_shallow_top5.py --db app\labint.db --dry-run
python complete_family_evidence_phase8_shallow_top5.py --db app\labint.db
python verify_family_completion_phase8_shallow_top5.py --db app\labint.db

python final_state_verifier.py --db app\labint.db
python family_completion_dashboard.py --backend-root .
