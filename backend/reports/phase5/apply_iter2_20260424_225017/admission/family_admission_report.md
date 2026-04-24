# CHEMLENS family admission benchmark phase2_mvp

## Summary

- total_cases: 253
- top1_correct: 203
- top1_accuracy: 0.8024
- top3_correct: 226
- top3_accuracy: 0.8933
- unique_expected_families: 253
- confused_pairs (top1 miss): 50

## Confusion pairs (top1 ≠ expected)

| expected_canonical | predicted_top1 | count |
|---|---|---:|
| Aldol Reaction | Stetter Reaction | 1 |
| Amadori Reaction / Rearrangement | Pinner Reaction | 1 |
| Bamford-Stevens-Shapiro Olefination | Schwartz Hydrozirconation | 1 |
| Barton Radical Decarboxylation Reaction | Diels-Alder Cycloaddition | 1 |
| Barton-McCombie Radical Deoxygenation Reaction | Baker-Venkataraman Rearrangement | 1 |
| Beckmann Rearrangement | Eschenmoser Methenylation | 1 |
| Biginelli Reaction | Retro-Claisen Reaction | 1 |
| Charette Asymmetric Cyclopropanation | Buchner Method of Ring Expansion (Buchner Reaction) | 1 |
| Chugaev Elimination Reaction (Xanthate Ester Pyrolysis) | Vinylcyclopropane-Cyclopentene Rearrangement | 1 |
| Corey-Kim Oxidation | Ley Oxidation | 1 |
| Criegee Oxidation | Baeyer-Villiger Oxidation/Rearrangement | 1 |
| Dakin-West Reaction | Aldol Reaction | 1 |
| Davis' Oxaziridine Oxidations | Clemmensen Reduction | 1 |
| De Mayo Cycloaddition | Robinson Annulation | 1 |
| Dess-Martin Oxidation | Ley Oxidation | 1 |
| Enyne Metathesis | Schwartz Hydrozirconation | 1 |
| Feist-Bénary Furan Synthesis | Retro-Claisen Reaction | 1 |
| Furukawa Modification | Simmons-Smith Cyclopropanation | 1 |
| Jacobsen-Katsuki Epoxidation | Shi Asymmetric Epoxidation | 1 |
| Knoevenagel Condensation | Pinnick Oxidation | 1 |
| McMurry Coupling | Lieben Haloform Reaction | 1 |
| Meerwein-Ponndorf-Verley Reduction | Noyori Asymmetric Hydrogenation | 1 |
| Michael Addition Reaction | Malonic Ester Synthesis | 1 |
| Minisci Reaction | Barton Radical Decarboxylation Reaction | 1 |
| Mitsunobu Reaction | Regitz Diazo Transfer | 1 |
| Negishi Cross-Coupling | Kumada Cross-Coupling | 1 |
| Nicholas Reaction | Barton-McCombie Radical Deoxygenation Reaction | 1 |
| Nozaki-Hiyama-Kishi Reaction | Pinnick Oxidation | 1 |
| Oppenauer Oxidation | Corey-Kim Oxidation | 1 |
| Passerini Multicomponent Reaction | Baeyer-Villiger Oxidation/Rearrangement | 1 |
| Pfitzner-Moffatt Oxidation | Ley Oxidation | 1 |
| Prilezhaev Reaction | Shi Asymmetric Epoxidation | 1 |
| Prins Reaction | Hetero Diels-Alder Cycloaddition (HDA) | 1 |
| Quasi-Favorskii Rearrangement | Cannizzaro Reaction | 1 |
| Reformatsky Reaction | Benzoin and Retro-Benzoin Condensation | 1 |
| Retro-Claisen Reaction | Malonic Ester Synthesis | 1 |
| Schotten-Baumann Reaction | Schmidt Reaction | 1 |
| Seyferth-Gilbert Homologation | Corey-Fuchs Alkyne Synthesis | 1 |
| Sonogashira Cross-Coupling | Friedel-Crafts Alkylation | 1 |
| Staudinger Reaction | Friedel-Crafts Alkylation | 1 |
| Stephen Aldehyde Synthesis (Stephen Reduction) | Kornblum Oxidation | 1 |
| Stille Cross-Coupling (Migita-Kosugi-Stille Coupling) | Friedel-Crafts Acylation | 1 |
| Stobbe Condensation | Knoevenagel Condensation | 1 |
| Swern Oxidation | Ley Oxidation | 1 |
| Tishchenko Reaction | Stetter Reaction | 1 |
| Ugi Multicomponent Reaction | Aldol Reaction | 1 |
| Weinreb Ketone Synthesis | Snieckus Directed Ortho Metalation | 1 |
| Wittig Reaction | Aza-Wittig Reaction | 1 |
| Wurtz Coupling | Gabriel Synthesis | 1 |
| von Pechmann Reaction | Acetoacetic Ester Synthesis | 1 |

## Per-family

| family | cases | top1 | top3 |
|---|---:|---:|---:|
| Acetoacetic Ester Synthesis | 1 | 1/1 | 1/1 |
| Acyloin Condensation | 1 | 1/1 | 1/1 |
| Alder (Ene) Reaction (Hydro-Allyl Addition) | 1 | 1/1 | 1/1 |
| Aldol Reaction | 1 | 0/1 | 1/1 |
| Alkene (Olefin) Metathesis | 1 | 1/1 | 1/1 |
| Alkyne Metathesis | 1 | 1/1 | 1/1 |
| Amadori Reaction / Rearrangement | 1 | 0/1 | 0/1 |
| Arbuzov Reaction (Michaelis-Arbuzov Reaction) | 1 | 1/1 | 1/1 |
| Arndt-Eistert Homologation / Synthesis | 1 | 1/1 | 1/1 |
| Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement) | 1 | 1/1 | 1/1 |
| Aza-Cope Rearrangement | 1 | 1/1 | 1/1 |
| Aza-Wittig Reaction | 1 | 1/1 | 1/1 |
| Aza-[2,3]-Wittig Rearrangement | 1 | 1/1 | 1/1 |
| Baeyer-Villiger Oxidation/Rearrangement | 1 | 1/1 | 1/1 |
| Baker-Venkataraman Rearrangement | 1 | 1/1 | 1/1 |
| Baldwin's Rules / Guidelines for Ring-Closing Reactions | 1 | 1/1 | 1/1 |
| Balz-Schiemann Reaction (Schiemann Reaction) | 1 | 1/1 | 1/1 |
| Bamford-Stevens-Shapiro Olefination | 1 | 0/1 | 1/1 |
| Barbier Coupling Reaction | 1 | 1/1 | 1/1 |
| Bartoli Indole Synthesis | 1 | 1/1 | 1/1 |
| Barton Nitrite Ester Reaction | 1 | 1/1 | 1/1 |
| Barton Radical Decarboxylation Reaction | 1 | 0/1 | 0/1 |
| Barton-McCombie Radical Deoxygenation Reaction | 1 | 0/1 | 1/1 |
| Baylis-Hillman Reaction | 1 | 1/1 | 1/1 |
| Beckmann Rearrangement | 1 | 0/1 | 0/1 |
| Benzilic Acid Rearrangement | 1 | 1/1 | 1/1 |
| Benzoin and Retro-Benzoin Condensation | 1 | 1/1 | 1/1 |
| Bergman Cycloaromatization Reaction | 1 | 1/1 | 1/1 |
| Biginelli Reaction | 1 | 0/1 | 0/1 |
| Birch Reduction | 1 | 1/1 | 1/1 |
| Bischler-Napieralski Isoquinoline Synthesis | 1 | 1/1 | 1/1 |
| Brook Rearrangement | 1 | 1/1 | 1/1 |
| Brown Hydroboration Reaction | 1 | 1/1 | 1/1 |
| Buchner Method of Ring Expansion (Buchner Reaction) | 1 | 1/1 | 1/1 |
| Buchwald-Hartwig Cross-Coupling | 1 | 1/1 | 1/1 |
| Burgess Dehydration Reaction | 1 | 1/1 | 1/1 |
| Cannizzaro Reaction | 1 | 1/1 | 1/1 |
| Carroll Rearrangement (Kimel-Cope Rearrangement) | 1 | 1/1 | 1/1 |
| Castro-Stephens Coupling | 1 | 1/1 | 1/1 |
| Charette Asymmetric Cyclopropanation | 1 | 0/1 | 1/1 |
| Chichibabin Amination Reaction (Chichibabin Reaction) | 1 | 1/1 | 1/1 |
| Chugaev Elimination Reaction (Xanthate Ester Pyrolysis) | 1 | 0/1 | 0/1 |
| Ciamician-Dennstedt Rearrangement | 1 | 1/1 | 1/1 |
| Claisen Condensation / Claisen Reaction | 1 | 1/1 | 1/1 |
| Claisen Rearrangement | 1 | 1/1 | 1/1 |
| Claisen-Ireland Rearrangement | 1 | 1/1 | 1/1 |
| Clemmensen Reduction | 1 | 1/1 | 1/1 |
| Combes Quinoline Synthesis | 1 | 1/1 | 1/1 |
| Cope Elimination / Cope Reaction | 1 | 1/1 | 1/1 |
| Cope Rearrangement | 1 | 1/1 | 1/1 |
| Corey-Bakshi-Shibata Reduction (CBS Reduction) | 1 | 1/1 | 1/1 |
| Corey-Chaykovsky Epoxidation and Cyclopropanation | 1 | 1/1 | 1/1 |
| Corey-Fuchs Alkyne Synthesis | 1 | 1/1 | 1/1 |
| Corey-Kim Oxidation | 1 | 0/1 | 1/1 |
| Corey-Nicolaou Macrolactonization | 1 | 1/1 | 1/1 |
| Corey-Winter Olefination | 1 | 1/1 | 1/1 |
| Cornforth Rearrangement | 1 | 1/1 | 1/1 |
| Criegee Oxidation | 1 | 0/1 | 0/1 |
| Curtius Rearrangement | 1 | 1/1 | 1/1 |
| Dakin Oxidation | 1 | 1/1 | 1/1 |
| Dakin-West Reaction | 1 | 0/1 | 1/1 |
| Danheiser Benzannulation | 1 | 1/1 | 1/1 |
| Danheiser Cyclopentene Annulation | 1 | 1/1 | 1/1 |
| Danishefsky's Diene Cycloaddition | 1 | 1/1 | 1/1 |
| Darzens Glycidic Ester Condensation | 1 | 1/1 | 1/1 |
| Davis' Oxaziridine Oxidations | 1 | 0/1 | 0/1 |
| De Mayo Cycloaddition | 1 | 0/1 | 1/1 |
| Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement | 1 | 1/1 | 1/1 |
| Dess-Martin Oxidation | 1 | 0/1 | 0/1 |
| Dieckmann Condensation | 1 | 1/1 | 1/1 |
| Diels-Alder Cycloaddition | 1 | 1/1 | 1/1 |
| Dienone-Phenol Rearrangement | 1 | 1/1 | 1/1 |
| Dimroth Rearrangement | 1 | 1/1 | 1/1 |
| Doering-Laflamme Allene Synthesis | 1 | 1/1 | 1/1 |
| Dötz Benzannulation Reaction | 1 | 1/1 | 1/1 |
| Enders SAMP/RAMP Hydrazone Alkylation | 1 | 1/1 | 1/1 |
| Enyne Metathesis | 1 | 0/1 | 0/1 |
| Eschenmoser Methenylation | 1 | 1/1 | 1/1 |
| Eschenmoser-Claisen Rearrangement | 1 | 1/1 | 1/1 |
| Eschenmoser-Tanabe Fragmentation | 1 | 1/1 | 1/1 |
| Eschweiler-Clarke Methylation | 1 | 1/1 | 1/1 |
| Evans Aldol Reaction | 1 | 1/1 | 1/1 |
| Favorskii Rearrangement | 1 | 1/1 | 1/1 |
| Feist-Bénary Furan Synthesis | 1 | 0/1 | 1/1 |
| Ferrier Reaction | 1 | 1/1 | 1/1 |
| Finkelstein Reaction | 1 | 1/1 | 1/1 |
| Fischer Indole Synthesis | 1 | 1/1 | 1/1 |
| Fleming-Tamao Oxidation | 1 | 1/1 | 1/1 |
| Friedel-Crafts Acylation | 1 | 1/1 | 1/1 |
| Friedel-Crafts Alkylation | 1 | 1/1 | 1/1 |
| Fries Rearrangement | 1 | 1/1 | 1/1 |
| Furukawa Modification | 1 | 0/1 | 1/1 |
| Gabriel Synthesis | 1 | 1/1 | 1/1 |
| Gattermann and Gattermann-Koch Formylation | 1 | 1/1 | 1/1 |
| Glaser Coupling | 1 | 1/1 | 1/1 |
| Grignard Reaction | 1 | 1/1 | 1/1 |
| Grob Fragmentation | 1 | 1/1 | 1/1 |
| Hajos-Parrish Reaction | 1 | 1/1 | 1/1 |
| Hantzsch Dihydropyridine Synthesis | 1 | 1/1 | 1/1 |
| Heck Reaction | 1 | 1/1 | 1/1 |
| Heine Reaction | 1 | 1/1 | 1/1 |
| Hell-Volhard-Zelinsky Reaction | 1 | 1/1 | 1/1 |
| Henry Reaction | 1 | 1/1 | 1/1 |
| Hetero Diels-Alder Cycloaddition (HDA) | 1 | 1/1 | 1/1 |
| Hofmann Elimination | 1 | 1/1 | 1/1 |
| Hofmann Rearrangement | 1 | 1/1 | 1/1 |
| Hofmann-Loffler-Freytag Reaction | 1 | 1/1 | 1/1 |
| Horner-Wadsworth-Emmons Olefination | 1 | 1/1 | 1/1 |
| Houben-Hoesch Reaction | 1 | 1/1 | 1/1 |
| Hunsdiecker Reaction | 1 | 1/1 | 1/1 |
| Intramolecular Nitrile Oxide Cycloaddition | 1 | 1/1 | 1/1 |
| Jacobsen Hydrolytic Kinetic Resolution | 1 | 1/1 | 1/1 |
| Jacobsen-Katsuki Epoxidation | 1 | 0/1 | 1/1 |
| Japp-Klingemann Reaction | 1 | 1/1 | 1/1 |
| Johnson-Claisen Rearrangement | 1 | 1/1 | 1/1 |
| Jones Oxidation | 1 | 1/1 | 1/1 |
| Julia-Lythgoe Olefination | 1 | 1/1 | 1/1 |
| Kagan-Molander Samarium Diiodide-Mediated Coupling | 1 | 1/1 | 1/1 |
| Kahne Glycosidation | 1 | 1/1 | 1/1 |
| Keck Asymmetric Allylation | 1 | 1/1 | 1/1 |
| Keck Radical Allylation | 1 | 1/1 | 1/1 |
| Knoevenagel Condensation | 1 | 0/1 | 1/1 |
| Knorr Pyrrole Synthesis | 1 | 1/1 | 1/1 |
| Koenigs-Knorr Glycosidation | 1 | 1/1 | 1/1 |
| Kolbe-Schmitt Reaction | 1 | 1/1 | 1/1 |
| Kornblum Oxidation | 1 | 1/1 | 1/1 |
| Krapcho Dealkoxycarbonylation (Krapcho Reaction) | 1 | 1/1 | 1/1 |
| Kröhnke Pyridine Synthesis | 1 | 1/1 | 1/1 |
| Kulinkovich Reaction | 1 | 1/1 | 1/1 |
| Kulinovich Reaction | 1 | 1/1 | 1/1 |
| Kumada Cross-Coupling | 1 | 1/1 | 1/1 |
| Larock Indole Synthesis | 1 | 1/1 | 1/1 |
| Ley Oxidation | 1 | 1/1 | 1/1 |
| Lieben Haloform Reaction | 1 | 1/1 | 1/1 |
| Lossen Rearrangement | 1 | 1/1 | 1/1 |
| Luche Reduction | 1 | 1/1 | 1/1 |
| Madelung Indole Synthesis | 1 | 1/1 | 1/1 |
| Malonic Ester Synthesis | 1 | 1/1 | 1/1 |
| Mannich Reaction | 1 | 1/1 | 1/1 |
| McMurry Coupling | 1 | 0/1 | 0/1 |
| Meerwein Arylation | 1 | 1/1 | 1/1 |
| Meerwein-Ponndorf-Verley Reduction | 1 | 0/1 | 0/1 |
| Meisenheimer Rearrangement | 1 | 1/1 | 1/1 |
| Meyer-Schuster and Rupe Rearrangement | 1 | 1/1 | 1/1 |
| Michael Addition Reaction | 1 | 0/1 | 1/1 |
| Midland Alpine Borane Reduction | 1 | 1/1 | 1/1 |
| Minisci Reaction | 1 | 0/1 | 1/1 |
| Mislow-Evans Rearrangement | 1 | 1/1 | 1/1 |
| Mitsunobu Reaction | 1 | 0/1 | 0/1 |
| Miyaura Boration | 1 | 1/1 | 1/1 |
| Mukaiyama Aldol Reaction | 1 | 1/1 | 1/1 |
| Myers Asymmetric Alkylation | 1 | 1/1 | 1/1 |
| Nagata Hydrocyanation | 1 | 1/1 | 1/1 |
| Nazarov Cyclization | 1 | 1/1 | 1/1 |
| Neber Rearrangement | 1 | 1/1 | 1/1 |
| Nef Reaction | 1 | 1/1 | 1/1 |
| Negishi Cross-Coupling | 1 | 0/1 | 0/1 |
| Nenitzescu Indole Synthesis | 1 | 1/1 | 1/1 |
| Nicholas Reaction | 1 | 0/1 | 1/1 |
| Noyori Asymmetric Hydrogenation | 1 | 1/1 | 1/1 |
| Nozaki-Hiyama-Kishi Reaction | 1 | 0/1 | 0/1 |
| Oppenauer Oxidation | 1 | 0/1 | 0/1 |
| Overman Rearrangement | 1 | 1/1 | 1/1 |
| Oxy-Cope Rearrangement and Anionic Oxy-Cope Rearrangement | 1 | 1/1 | 1/1 |
| Paal-Knorr Furan Synthesis | 1 | 1/1 | 1/1 |
| Paal-Knorr Pyrrole Synthesis | 1 | 1/1 | 1/1 |
| Passerini Multicomponent Reaction | 1 | 0/1 | 0/1 |
| Paternò-Büchi Reaction | 1 | 1/1 | 1/1 |
| Pauson-Khand Reaction | 1 | 1/1 | 1/1 |
| Payne Rearrangement | 1 | 1/1 | 1/1 |
| Perkin Reaction | 1 | 1/1 | 1/1 |
| Petasis Boronic Acid-Mannich Reaction | 1 | 1/1 | 1/1 |
| Petasis-Ferrier Rearrangement | 1 | 1/1 | 1/1 |
| Peterson Olefination | 1 | 1/1 | 1/1 |
| Pfitzner-Moffatt Oxidation | 1 | 0/1 | 0/1 |
| Pictet-Spengler Tetrahydroisoquinoline Synthesis | 1 | 1/1 | 1/1 |
| Pinacol and Semipinacol Rearrangement | 1 | 1/1 | 1/1 |
| Pinner Reaction | 1 | 1/1 | 1/1 |
| Pinnick Oxidation | 1 | 1/1 | 1/1 |
| Polonovski Reaction | 1 | 1/1 | 1/1 |
| Pomeranz-Fritsch Reaction | 1 | 1/1 | 1/1 |
| Prilezhaev Reaction | 1 | 0/1 | 1/1 |
| Prins Reaction | 1 | 0/1 | 0/1 |
| Prins-Pinacol Rearrangement | 1 | 1/1 | 1/1 |
| Prévost Reaction | 1 | 1/1 | 1/1 |
| Pummerer Rearrangement | 1 | 1/1 | 1/1 |
| Quasi-Favorskii Rearrangement | 1 | 0/1 | 1/1 |
| Ramberg-Bäcklund Rearrangement | 1 | 1/1 | 1/1 |
| Reformatsky Reaction | 1 | 0/1 | 0/1 |
| Regitz Diazo Transfer | 1 | 1/1 | 1/1 |
| Reimer-Tiemann Reaction | 1 | 1/1 | 1/1 |
| Retro-Claisen Reaction | 1 | 0/1 | 0/1 |
| Riley Selenium Dioxide Oxidation | 1 | 1/1 | 1/1 |
| Ring-Closing Alkyne Metathesis | 1 | 1/1 | 1/1 |
| Ring-Closing Metathesis | 1 | 1/1 | 1/1 |
| Ring-Opening Metathesis | 1 | 1/1 | 1/1 |
| Ring-Opening Metathesis Polymerization | 1 | 1/1 | 1/1 |
| Ritter Reaction | 1 | 1/1 | 1/1 |
| Robinson Annulation | 1 | 1/1 | 1/1 |
| Roush Asymmetric Allylation | 1 | 1/1 | 1/1 |
| Rubottom Oxidation | 1 | 1/1 | 1/1 |
| Saegusa Oxidation | 1 | 1/1 | 1/1 |
| Sakurai Allylation | 1 | 1/1 | 1/1 |
| Sandmeyer Reaction | 1 | 1/1 | 1/1 |
| Schmidt Reaction | 1 | 1/1 | 1/1 |
| Schotten-Baumann Reaction | 1 | 0/1 | 1/1 |
| Schwartz Hydrozirconation | 1 | 1/1 | 1/1 |
| Seyferth-Gilbert Homologation | 1 | 0/1 | 1/1 |
| Sharpless Asymmetric Aminohydroxylation | 1 | 1/1 | 1/1 |
| Sharpless Asymmetric Dihydroxylation | 1 | 1/1 | 1/1 |
| Sharpless Asymmetric Epoxidation | 1 | 1/1 | 1/1 |
| Shi Asymmetric Epoxidation | 1 | 1/1 | 1/1 |
| Simmons-Smith Cyclopropanation | 1 | 1/1 | 1/1 |
| Skraup and Doebner-Miller Quinoline Synthesis | 1 | 1/1 | 1/1 |
| Smiles Rearrangement | 1 | 1/1 | 1/1 |
| Smith-Tietze Multicomponent Dithiane Linchpin Coupling | 1 | 1/1 | 1/1 |
| Snieckus Directed Ortho Metalation | 1 | 1/1 | 1/1 |
| Sommelet-Hauser Rearrangement | 1 | 1/1 | 1/1 |
| Sonogashira Cross-Coupling | 1 | 0/1 | 0/1 |
| Staudinger Ketene Cycloaddition | 1 | 1/1 | 1/1 |
| Staudinger Reaction | 1 | 0/1 | 1/1 |
| Stephen Aldehyde Synthesis (Stephen Reduction) | 1 | 0/1 | 1/1 |
| Stetter Reaction | 1 | 1/1 | 1/1 |
| Stevens Rearrangement | 1 | 1/1 | 1/1 |
| Stille Cross-Coupling (Migita-Kosugi-Stille Coupling) | 1 | 0/1 | 0/1 |
| Stille-Kelly Coupling | 1 | 1/1 | 1/1 |
| Stobbe Condensation | 1 | 0/1 | 0/1 |
| Stork Enamine Synthesis | 1 | 1/1 | 1/1 |
| Strecker Reaction | 1 | 1/1 | 1/1 |
| Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling) | 1 | 1/1 | 1/1 |
| Swern Oxidation | 1 | 0/1 | 0/1 |
| Takai-Utimoto Olefination (Takai Reaction) | 1 | 1/1 | 1/1 |
| Tebbe Olefination/Petasis-Tebbe Olefination | 1 | 1/1 | 1/1 |
| Tishchenko Reaction | 1 | 0/1 | 1/1 |
| Tsuji-Trost Reaction / Allylation | 1 | 1/1 | 1/1 |
| Tsuji-Wilkinson Decarbonylation Reaction | 1 | 1/1 | 1/1 |
| Ugi Multicomponent Reaction | 1 | 0/1 | 0/1 |
| Ullmann Reaction / Coupling / Biaryl Synthesis | 1 | 1/1 | 1/1 |
| Vilsmeier-Haack Formylation | 1 | 1/1 | 1/1 |
| Vinylcyclopropane-Cyclopentene Rearrangement | 1 | 1/1 | 1/1 |
| Wacker Oxidation | 1 | 1/1 | 1/1 |
| Wagner-Meerwein Rearrangement | 1 | 1/1 | 1/1 |
| Weinreb Ketone Synthesis | 1 | 0/1 | 0/1 |
| Wharton Fragmentation | 1 | 1/1 | 1/1 |
| Wharton Olefin Synthesis (Wharton Transposition) | 1 | 1/1 | 1/1 |
| Williamson Ether Synthesis | 1 | 1/1 | 1/1 |
| Wittig Reaction | 1 | 0/1 | 1/1 |
| Wittig-[1,2]- and [2,3]-Rearrangement | 1 | 1/1 | 1/1 |
| Wohl-Ziegler Bromination | 1 | 1/1 | 1/1 |
| Wolff Rearrangement | 1 | 1/1 | 1/1 |
| Wolff-Kishner Reduction | 1 | 1/1 | 1/1 |
| Wurtz Coupling | 1 | 0/1 | 1/1 |
| von Pechmann Reaction | 1 | 0/1 | 0/1 |

## Miss table (top1 miss only)

| case_id | expected | top1 | top3 |
|---|---|---|---|
| adm_aldol_reaction_1519 | Aldol Reaction | Stetter Reaction | Stetter Reaction, Grignard Reaction, Aldol Reaction |
| adm_amadori_reaction_/_rearrangement_806 | Amadori Reaction / Rearrangement | Pinner Reaction | Pinner Reaction, Friedel-Crafts Acylation, Sharpless Asymmetric Aminohydroxylation |
| adm_bamford-stevens-shapiro_olefination_1531 | Bamford-Stevens-Shapiro Olefination | Schwartz Hydrozirconation | Schwartz Hydrozirconation, Bamford-Stevens-Shapiro Olefination, Wacker Oxidation |
| adm_barton_radical_decarboxylation_reaction_357 | Barton Radical Decarboxylation Reaction | Diels-Alder Cycloaddition | Diels-Alder Cycloaddition, Danheiser Benzannulation, Dieckmann Condensation |
| adm_barton-mccombie_radical_deoxygenation_reaction_796 | Barton-McCombie Radical Deoxygenation Reaction | Baker-Venkataraman Rearrangement | Baker-Venkataraman Rearrangement, Paternò-Büchi Reaction, Barton-McCombie Radical Deoxygenation Reaction |
| adm_beckmann_rearrangement_1435 | Beckmann Rearrangement | Eschenmoser Methenylation | Eschenmoser Methenylation, Enders SAMP/RAMP Hydrazone Alkylation, Ciamician-Dennstedt Rearrangement |
| adm_biginelli_reaction_1441 | Biginelli Reaction | Retro-Claisen Reaction | Retro-Claisen Reaction, Charette Asymmetric Cyclopropanation, Tishchenko Reaction |
| adm_charette_asymmetric_cyclopropanation_824 | Charette Asymmetric Cyclopropanation | Buchner Method of Ring Expansion (Buchner Reaction) | Buchner Method of Ring Expansion (Buchner Reaction), Charette Asymmetric Cyclopropanation, Furukawa Modification |
| adm_chugaev_elimination_reaction_(xanthate_ester_pyrolysis)_826 | Chugaev Elimination Reaction (Xanthate Ester Pyrolysis) | Vinylcyclopropane-Cyclopentene Rearrangement | Vinylcyclopropane-Cyclopentene Rearrangement, Corey-Winter Olefination, Tebbe Olefination/Petasis-Tebbe Olefination |
| adm_corey-kim_oxidation_843 | Corey-Kim Oxidation | Ley Oxidation | Ley Oxidation, Corey-Kim Oxidation, Kornblum Oxidation |
| adm_criegee_oxidation_855 | Criegee Oxidation | Baeyer-Villiger Oxidation/Rearrangement | Baeyer-Villiger Oxidation/Rearrangement, Baeyer-Villiger Oxidation, Eschenmoser Methenylation |
| adm_dakin-west_reaction_853 | Dakin-West Reaction | Aldol Reaction | Aldol Reaction, Dakin-West Reaction, Clemmensen Reduction |
| adm_davis'_oxaziridine_oxidations_863 | Davis' Oxaziridine Oxidations | Clemmensen Reduction | Clemmensen Reduction, Dess-Martin Oxidation, Balz-Schiemann Reaction (Schiemann Reaction) |
| adm_de_mayo_cycloaddition_867 | De Mayo Cycloaddition | Robinson Annulation | Robinson Annulation, De Mayo Cycloaddition, Regitz Diazo Transfer |
| adm_dess-martin_oxidation_871 | Dess-Martin Oxidation | Ley Oxidation | Ley Oxidation, Corey-Kim Oxidation, Kornblum Oxidation |
| adm_enyne_metathesis_611 | Enyne Metathesis | Schwartz Hydrozirconation | Schwartz Hydrozirconation, Brown Hydroboration Reaction, Dess-Martin Oxidation |
| adm_feist-bénary_furan_synthesis_895 | Feist-Bénary Furan Synthesis | Retro-Claisen Reaction | Retro-Claisen Reaction, Claisen Condensation / Claisen Reaction, Feist-Bénary Furan Synthesis |
| adm_furukawa_modification_928 | Furukawa Modification | Simmons-Smith Cyclopropanation | Simmons-Smith Cyclopropanation, Furukawa Modification, Shi Asymmetric Epoxidation |
| adm_jacobsen-katsuki_epoxidation_934 | Jacobsen-Katsuki Epoxidation | Shi Asymmetric Epoxidation | Shi Asymmetric Epoxidation, Jacobsen-Katsuki Epoxidation, Prilezhaev Reaction |
| adm_knoevenagel_condensation_948 | Knoevenagel Condensation | Pinnick Oxidation | Pinnick Oxidation, Knoevenagel Condensation, Danishefsky's Diene Cycloaddition |
| adm_mcmurry_coupling_1045 | McMurry Coupling | Lieben Haloform Reaction | Lieben Haloform Reaction, Mannich Reaction, Noyori Asymmetric Hydrogenation |
| adm_meerwein-ponndorf-verley_reduction_1051 | Meerwein-Ponndorf-Verley Reduction | Noyori Asymmetric Hydrogenation | Noyori Asymmetric Hydrogenation, Corey-Bakshi-Shibata Reduction (CBS Reduction), Midland Alpine Borane Reduction |
| adm_michael_addition_reaction_610 | Michael Addition Reaction | Malonic Ester Synthesis | Malonic Ester Synthesis, Acetoacetic Ester Synthesis, Michael Addition Reaction |
| adm_minisci_reaction_561 | Minisci Reaction | Barton Radical Decarboxylation Reaction | Barton Radical Decarboxylation Reaction, Minisci Reaction, Barton Radical Decarboxylation |
| adm_mitsunobu_reaction_612 | Mitsunobu Reaction | Regitz Diazo Transfer | Regitz Diazo Transfer, Lieben Haloform Reaction, Mannich Reaction |
| adm_negishi_cross-coupling_1096 | Negishi Cross-Coupling | Kumada Cross-Coupling | Kumada Cross-Coupling, Kornblum Oxidation, Suzuki Cross-Coupling |
| adm_nicholas_reaction_1102 | Nicholas Reaction | Barton-McCombie Radical Deoxygenation Reaction | Barton-McCombie Radical Deoxygenation Reaction, Alkyne Metathesis, Nicholas Reaction |
| adm_nozaki-hiyama-kishi_reaction_1108 | Nozaki-Hiyama-Kishi Reaction | Pinnick Oxidation | Pinnick Oxidation, Takai-Utimoto Olefination (Takai Reaction), Danishefsky's Diene Cycloaddition |
| adm_oppenauer_oxidation_1111 | Oppenauer Oxidation | Corey-Kim Oxidation | Corey-Kim Oxidation, Dess-Martin Oxidation, Swern Oxidation |
| adm_passerini_multicomponent_reaction_1126 | Passerini Multicomponent Reaction | Baeyer-Villiger Oxidation/Rearrangement | Baeyer-Villiger Oxidation/Rearrangement, Corey-Bakshi-Shibata Reduction (CBS Reduction), Midland Alpine Borane Reduction |
| adm_pfitzner-moffatt_oxidation_1150 | Pfitzner-Moffatt Oxidation | Ley Oxidation | Ley Oxidation, Corey-Kim Oxidation, Kornblum Oxidation |
| adm_prilezhaev_reaction_1171 | Prilezhaev Reaction | Shi Asymmetric Epoxidation | Shi Asymmetric Epoxidation, Jacobsen-Katsuki Epoxidation, Prilezhaev Reaction |
| adm_prins_reaction_1174 | Prins Reaction | Hetero Diels-Alder Cycloaddition (HDA) | Hetero Diels-Alder Cycloaddition (HDA), Alder (Ene) Reaction (Hydro-Allyl Addition), Sharpless Asymmetric Epoxidation |
| adm_quasi-favorskii_rearrangement_1186 | Quasi-Favorskii Rearrangement | Cannizzaro Reaction | Cannizzaro Reaction, Quasi-Favorskii Rearrangement, Wagner-Meerwein Rearrangement |
| adm_reformatsky_reaction_1192 | Reformatsky Reaction | Benzoin and Retro-Benzoin Condensation | Benzoin and Retro-Benzoin Condensation, Darzens Glycidic Ester Condensation, Arbuzov Reaction (Michaelis-Arbuzov Reaction) |
| adm_retro-claisen_reaction_1201 | Retro-Claisen Reaction | Malonic Ester Synthesis | Malonic Ester Synthesis, Krapcho Dealkoxycarbonylation (Krapcho Reaction), Regitz Diazo Transfer |
| adm_schotten-baumann_reaction_1243 | Schotten-Baumann Reaction | Schmidt Reaction | Schmidt Reaction, Lieben Haloform Reaction, Schotten-Baumann Reaction |
| adm_seyferth-gilbert_homologation_1249 | Seyferth-Gilbert Homologation | Corey-Fuchs Alkyne Synthesis | Corey-Fuchs Alkyne Synthesis, Seyferth-Gilbert Homologation, Wolff-Kishner Reduction |
| adm_sonogashira_cross-coupling_1285 | Sonogashira Cross-Coupling | Friedel-Crafts Alkylation | Friedel-Crafts Alkylation, Balz-Schiemann Reaction (Schiemann Reaction), Alkyne Metathesis |
| adm_staudinger_reaction_1291 | Staudinger Reaction | Friedel-Crafts Alkylation | Friedel-Crafts Alkylation, Friedel-Crafts Acylation, Staudinger Reaction |
| adm_stephen_aldehyde_synthesis_(stephen_reduction)_1294 | Stephen Aldehyde Synthesis (Stephen Reduction) | Kornblum Oxidation | Kornblum Oxidation, Stephen Aldehyde Synthesis (Stephen Reduction), Gattermann and Gattermann-Koch Formylation |
| adm_stille_cross-coupling_(migita-kosugi-stille_coupling)_1303 | Stille Cross-Coupling (Migita-Kosugi-Stille Coupling) | Friedel-Crafts Acylation | Friedel-Crafts Acylation, Dess-Martin Oxidation, Lieben Haloform Reaction |
| adm_stobbe_condensation_1312 | Stobbe Condensation | Knoevenagel Condensation | Knoevenagel Condensation, Darzens Glycidic Ester Condensation, Regitz Diazo Transfer |
| adm_swern_oxidation_1327 | Swern Oxidation | Ley Oxidation | Ley Oxidation, Corey-Kim Oxidation, Kornblum Oxidation |
| adm_tishchenko_reaction_1336 | Tishchenko Reaction | Stetter Reaction | Stetter Reaction, Tishchenko Reaction, Benzoin and Retro-Benzoin Condensation |
| adm_ugi_multicomponent_reaction_1345 | Ugi Multicomponent Reaction | Aldol Reaction | Aldol Reaction, Aza-Wittig Reaction, Combes Quinoline Synthesis |
| adm_weinreb_ketone_synthesis_1366 | Weinreb Ketone Synthesis | Snieckus Directed Ortho Metalation | Snieckus Directed Ortho Metalation, Friedel-Crafts Acylation, Lieben Haloform Reaction |
| adm_wittig_reaction_466 | Wittig Reaction | Aza-Wittig Reaction | Aza-Wittig Reaction, Wittig Reaction, Wolff-Kishner Reduction |
| adm_wurtz_coupling_1396 | Wurtz Coupling | Gabriel Synthesis | Gabriel Synthesis, Brown Hydroboration Reaction, Wurtz Coupling |
| adm_von_pechmann_reaction_1402 | von Pechmann Reaction | Acetoacetic Ester Synthesis | Acetoacetic Ester Synthesis, Michael Addition Reaction, Regitz Diazo Transfer |
