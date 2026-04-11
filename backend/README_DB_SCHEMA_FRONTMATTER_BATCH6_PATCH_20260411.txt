CHEMLENS backend DB schema / frontmatter batch6 patch

This cumulative patch extends the prior batch5 build with real named-reaction body pages:
- Alkyne Metathesis (pp. 12-13)
- Amadori Reaction / Rearrangement (pp. 14-15)
- Arbuzov Reaction / Michaelis-Arbuzov Reaction (pp. 16-17)
- Arndt-Eistert Homologation / Synthesis (pp. 18-19)
- Aza-Claisen Rearrangement / 3-Aza-Cope Rearrangement (pp. 20-21)

Main effects:
- Adds 10 manual_page_knowledge rows for named-reaction overview/application pages
- Adds curated aliases such as RCAM, Lindlar catalyst, p-TsOH, P(OMe)3, SOCl2, CH2N2, Ag2O, LiHMDS, NaHMDS
- Adds 5 family-pattern rows with reaction transformation / mechanism / reagent clues
- Adds page entities for key targets and related reactions

Apply:
1) overwrite backend folder with this ZIP contents
2) restart backend
3) optionally run run_db_frontmatter_batch6_upgrade.bat
