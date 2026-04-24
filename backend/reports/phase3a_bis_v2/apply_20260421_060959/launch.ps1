$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUTF8 = '1'
& 'C:\Users\tmdxo\miniconda3\envs\chemlens\python.exe' 'C:\Users\tmdxo\OneDrive\문서\Claude\Projects\유기합성 앱 개발 프로젝트\phase3a_bis_v2\phase3a_bis_v2.py' apply `
    --backend-root 'C:\chemlens\backend' `
    --db 'C:\chemlens\backend\app\labint.db' `
    --plan 'C:\Users\tmdxo\OneDrive\문서\Claude\Projects\유기합성 앱 개발 프로젝트\phase3a_bis_v2\overrides_plan.yaml' `
    --report-dir 'C:\chemlens\backend\reports\phase3a_bis_v2\apply_20260421_060959' *>&1 | Tee-Object -FilePath 'C:\chemlens\backend\reports\phase3a_bis_v2\apply_20260421_060959\child_stdout.log' | Out-Null
'RC=' + $LASTEXITCODE | Out-File -FilePath 'C:\chemlens\backend\reports\phase3a_bis_v2\apply_20260421_060959\rc.txt' -Encoding ascii
