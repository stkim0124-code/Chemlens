@echo off
echo hello from bat > "C:\chemlens\backend\reports\phase3d\apply_3d_tier1_20260421_104107\bat_test.log"
echo pwd is %CD% >> "C:\chemlens\backend\reports\phase3d\apply_3d_tier1_20260421_104107\bat_test.log"
"C:\Users\tmdxo\miniconda3\envs\chemlens\python.exe" -c "print('py ran from bat')" >> "C:\chemlens\backend\reports\phase3d\apply_3d_tier1_20260421_104107\bat_test.log" 2>&1