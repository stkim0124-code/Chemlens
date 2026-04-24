@echo off
conda activate chemlens
cd /d C:\chemlens\backend
python redact_pdf_example_api_keys.py --backend-root .
python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement" --limit-pages 10 --sleep 60 --max-regions-per-page 1 --cooldown-on-429 0
python verify_pdf_example_gemini_batch.py --backend-root .
python inspect_pdf_example_automation_errors.py --backend-root .
pause
