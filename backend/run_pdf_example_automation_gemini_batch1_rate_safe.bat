@echo off
setlocal
echo ========================================================================
echo PDF EXAMPLE AUTOMATION - GEMINI BATCH1 RATE SAFE
echo ========================================================================
python pdf_example_automation.py --backend-root . --call-gemini --families "Aldol Reaction;Alkene (Olefin) Metathesis;Diels-Alder Cycloaddition;Swern Oxidation;Ritter Reaction;Pinner Reaction;Schwartz Hydrozirconation;Tsuji-Wilkinson Decarbonylation Reaction;Barton-McCombie Radical Deoxygenation Reaction;Baeyer-Villiger Oxidation/Rearrangement" --sleep 5 --max-retries 8 --retry-initial-sleep 15
