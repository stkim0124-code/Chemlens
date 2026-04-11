@echo off
call conda activate chemlens
cd /d %~dp0
python merge_round9_staging_into_labint.py --main-db app\labint.db --staging-db labint_round9_v5_final_staging.db --tag round9_named_reactions_test
pause
