from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH12_VERSION = 'labint_frontmatter_batch12_v2_20260411'
SOURCE_LABEL = 'named_reactions_frontmatter_batch12'
LATEST_SOURCE_ZIP = 'chemlens_backend_db_schema_frontmatter_batch12_patch_20260411.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(12, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(12, FRONTMATTER_BATCH12_VERSION)
CURATED_BATCH12_ABBREVIATIONS = load_abbreviation_seeds(
    12,
    SOURCE_LABEL,
    source_pages={
        'BR': 72,
        'NaNH2': 80,
        '9-BBN': 81,
        'LDA': 77,
        'KHMDS': 74,
        'DBU': 79,
        'p-cymene': 81,
        'CuI': 78,
        'Ph3P': 79,
        'DMS': 81,
    },
    default_note='batch12 frontmatter abbreviation seed (restored from CSV and page-verified from source images)',
)

PAGE_ENTITY_SEEDS: list[dict[str, Any]] = [
    {'page_label':'p72','entity_text':'Burgess reagent','canonical_name':'Burgess reagent','entity_type':'reagent_class','family_name':'Burgess Dehydration Reaction','notes':'Inner salt reagent responsible for mild syn-dehydration and related cyclodehydration chemistry.','confidence':0.95},
    {'page_label':'p72','entity_text':'Sulfamate ester','canonical_name':'Sulfamate ester','entity_type':'intermediate','family_name':'Burgess Dehydration Reaction','notes':'Mechanistic intermediate explicitly shown before syn elimination.','confidence':0.95},
    {'page_label':'p72','entity_text':'Syn elimination','canonical_name':'Syn elimination','entity_type':'mechanistic_term','family_name':'Burgess Dehydration Reaction','notes':'Characteristic stereospecific elimination mode highlighted on the overview page.','confidence':0.94},
    {'page_label':'p73','entity_text':'Exocyclic alkene','canonical_name':'Exocyclic alkene','entity_type':'product_class','family_name':'Burgess Dehydration Reaction','notes':'Taxol-fragment application showing exomethylene installation.','confidence':0.90},
    {'page_label':'p73','entity_text':'(-)-Madumycin II','canonical_name':'(-)-Madumycin II','entity_type':'target_molecule','family_name':'Burgess Dehydration Reaction','notes':'Application page example involving Burgess cyclodehydration en route to an oxazole-containing antibiotic.','confidence':0.89},
    {'page_label':'p73','entity_text':'Herbicidin B','canonical_name':'Herbicidin B','entity_type':'target_molecule','family_name':'Burgess Dehydration Reaction','notes':'Nucleoside antibiotic application featuring Burgess-mediated dehydration to an enone.','confidence':0.89},

    {'page_label':'p74','entity_text':'Non-enolizable aldehyde','canonical_name':'Non-enolizable aldehyde','entity_type':'reactant_class','family_name':'Cannizzaro Reaction','notes':'Canonical substrate class for the Cannizzaro disproportionation.','confidence':0.95},
    {'page_label':'p74','entity_text':'Hydride transfer','canonical_name':'Hydride transfer','entity_type':'mechanistic_term','family_name':'Cannizzaro Reaction','notes':'Core redox event connecting oxidation and reduction of two aldehyde molecules.','confidence':0.95},
    {'page_label':'p74','entity_text':'Transannular Cannizzaro reaction','canonical_name':'Transannular Cannizzaro reaction','entity_type':'reaction_term','family_name':'Cannizzaro Reaction','notes':'Special intramolecular variant explicitly highlighted in the synthetic application panel.','confidence':0.91},
    {'page_label':'p75','entity_text':'Dibenzoheptalene bislactone','canonical_name':'Dibenzoheptalene bislactone','entity_type':'product_class','family_name':'Cannizzaro Reaction','notes':'Application page product arising from double intramolecular Cannizzaro logic.','confidence':0.89},
    {'page_label':'p75','entity_text':'4-Chloro-3-(hydroxymethyl)pyridine','canonical_name':'4-Chloro-3-(hydroxymethyl)pyridine','entity_type':'target_molecule','family_name':'Cannizzaro Reaction','notes':'Crossed-Cannizzaro based application highlighted on the page.','confidence':0.90},
    {'page_label':'p75','entity_text':'(+)-Isokotanin','canonical_name':'(+)-Isokotanin','entity_type':'target_molecule','family_name':'Cannizzaro Reaction','notes':'Atropoenantioselective diaryl-lactone application featuring a Cannizzaro step.','confidence':0.89},

    {'page_label':'p76','entity_text':'β-Keto allylic ester','canonical_name':'Beta-keto allylic ester','entity_type':'reactant_class','family_name':'Carroll Rearrangement','notes':'Canonical substrate class for the Carroll/Kimel-Cope rearrangement.','confidence':0.95},
    {'page_label':'p76','entity_text':'[3,3]-Sigmatropic rearrangement','canonical_name':'[3,3]-Sigmatropic rearrangement','entity_type':'mechanistic_term','family_name':'Carroll Rearrangement','notes':'Concerted rearrangement step explicitly shown on the overview page.','confidence':0.95},
    {'page_label':'p76','entity_text':'γ,δ-Unsaturated ketone','canonical_name':'Gamma,delta-unsaturated ketone','entity_type':'product_class','family_name':'Carroll Rearrangement','notes':'Characteristic product class formed after rearrangement and decarboxylation.','confidence':0.94},
    {'page_label':'p77','entity_text':'(-)-Malyngolide','canonical_name':'(-)-Malyngolide','entity_type':'target_molecule','family_name':'Carroll Rearrangement','notes':'Asymmetric Carroll rearrangement application shown on the page.','confidence':0.90},
    {'page_label':'p77','entity_text':'4-epi-Acetomycin','canonical_name':'4-epi-Acetomycin','entity_type':'target_molecule','family_name':'Carroll Rearrangement','notes':'Stereoselective ester enolate Carroll rearrangement product.','confidence':0.89},
    {'page_label':'p77','entity_text':'Prelog-Djerassi lactone','canonical_name':'Prelog-Djerassi lactone','entity_type':'target_molecule','family_name':'Carroll Rearrangement','notes':'Representative lactone application derived from Carroll rearrangement chemistry.','confidence':0.89},

    {'page_label':'p78','entity_text':'Copper acetylide','canonical_name':'Copper acetylide','entity_type':'reactant_class','family_name':'Castro-Stephens Coupling','notes':'Characteristic alkynyl partner in the Castro-Stephens coupling.','confidence':0.95},
    {'page_label':'p78','entity_text':'Four-centered transition state','canonical_name':'Four-centered transition state','entity_type':'mechanistic_term','family_name':'Castro-Stephens Coupling','notes':'Mechanistic model explicitly drawn on the overview page.','confidence':0.94},
    {'page_label':'p78','entity_text':'Disubstituted acetylene','canonical_name':'Disubstituted acetylene','entity_type':'product_class','family_name':'Castro-Stephens Coupling','notes':'Generic product class from coupling of aryl/vinyl halides with alkynes.','confidence':0.93},
    {'page_label':'p79','entity_text':'Isocumestan','canonical_name':'Isocumestan','entity_type':'target_molecule','family_name':'Castro-Stephens Coupling','notes':'Application page example formed through an extended Castro-Stephens manifold.','confidence':0.89},
    {'page_label':'p79','entity_text':'12-Membered (E,Z)-diene lactone','canonical_name':'12-Membered (E,Z)-diene lactone','entity_type':'product_class','family_name':'Castro-Stephens Coupling','notes':'Macrocyclization application obtained by intramolecular Castro-Stephens coupling followed by reduction.','confidence':0.89},
    {'page_label':'p79','entity_text':'Epothilone B fragment coupling','canonical_name':'Epothilone B fragment coupling','entity_type':'application_class','family_name':'Castro-Stephens Coupling','notes':'Modified Castro-Stephens coupling of two advanced subunits in epothilone synthesis.','confidence':0.88},

    {'page_label':'p80','entity_text':'2-Aminopyridine derivative','canonical_name':'2-Aminopyridine derivative','entity_type':'product_class','family_name':'Chichibabin Amination Reaction','notes':'Canonical product class of intermolecular Chichibabin amination.','confidence':0.95},
    {'page_label':'p80','entity_text':'Anionic σ-complex','canonical_name':'Anionic sigma complex','entity_type':'intermediate','family_name':'Chichibabin Amination Reaction','notes':'Mechanistic intermediate explicitly depicted on the overview page.','confidence':0.94},
    {'page_label':'p80','entity_text':'Hydride loss','canonical_name':'Hydride loss','entity_type':'mechanistic_term','family_name':'Chichibabin Amination Reaction','notes':'Distinguishing mechanistic feature of Chichibabin-type nucleophilic aromatic substitution.','confidence':0.93},
    {'page_label':'p81','entity_text':'PHIP','canonical_name':'2-Amino-1-methyl-6-phenyl-1H-imidazo[4,5-b]pyridine','entity_type':'target_molecule','family_name':'Chichibabin Amination Reaction','notes':'Mutagenic heteroarene synthesized through a Chichibabin amination step.','confidence':0.90},
    {'page_label':'p81','entity_text':'Tetrahydronaphthyridine derivative','canonical_name':'Tetrahydronaphthyridine derivative','entity_type':'product_class','family_name':'Chichibabin Amination Reaction','notes':'Intramolecular double Chichibabin cyclization application shown on the page.','confidence':0.89},
    {'page_label':'p81','entity_text':'12-t-Bu-[2.1.1]-(2,6)-pyridinophane','canonical_name':'12-t-Bu-[2.1.1]-(2,6)-pyridinophane','entity_type':'target_molecule','family_name':'Chichibabin Amination Reaction','notes':'Macrocyclic ligand family assembled through double Chichibabin-type condensation.','confidence':0.88},
]


def apply_frontmatter_batch12(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH12_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=12,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH12_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
        replace_existing_page_entities=True,
    )


if __name__ == '__main__':
    from pprint import pprint
    pprint(apply_frontmatter_batch12(Path(__file__).resolve().parent / 'labint.db'))
