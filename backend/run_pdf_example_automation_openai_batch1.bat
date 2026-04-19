@echo off
setlocal
if "%OPENAI_API_KEY%"=="" echo [ERROR] OPENAI_API_KEY is not set & exit /b 1
python pdf_example_automation.py --backend-root . --call-openai --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement" --sleep 2 --max-retries 6 --retry-initial-sleep 10
