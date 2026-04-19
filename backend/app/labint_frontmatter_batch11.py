from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from app.labint_frontmatter_batch_restore_utils import (
    apply_frontmatter_batch_generic,
    load_abbreviation_seeds,
    load_family_pattern_seeds,
    load_page_knowledge_seeds,
)

FRONTMATTER_BATCH11_VERSION = 'labint_frontmatter_batch11_v2_20260411'
SOURCE_LABEL = 'named_reactions_frontmatter_batch11'
LATEST_SOURCE_ZIP = 'chemlens_backend_db_schema_frontmatter_batch11_patch_20260411.zip'

PAGE_KNOWLEDGE_SEEDS = load_page_knowledge_seeds(11, SOURCE_LABEL)
FAMILY_PATTERN_SEEDS = load_family_pattern_seeds(11, FRONTMATTER_BATCH11_VERSION)
CURATED_BATCH11_ABBREVIATIONS = load_abbreviation_seeds(
    11,
    SOURCE_LABEL,
    source_pages={
        'POCl3': 62,
        'P2O5': 62,
        'PPA': 62,
        'Tf2O': 63,
        'BINAP': 70,
        'dppf': 70,
        'dba': 70,
        'XANTPHOS': 70,
        '9-BBN': 67,
        'BH3·DMS': 67,
        'Rh2(OAc)4': 68,
        'Pd2(dba)3': 70,
    },
    default_note='batch11 frontmatter abbreviation seed (restored from CSV and page-verified from source images)',
)

PAGE_ENTITY_SEEDS: list[dict[str, Any]] = [
    {'page_label':'p62','entity_text':'Acylated phenethylamine','canonical_name':'Acylated phenethylamine','entity_type':'reactant_class','family_name':'Bischler-Napieralski Isoquinoline Synthesis','notes':'Canonical substrate class for Bischler-Napieralski cyclodehydration.','confidence':0.95},
    {'page_label':'p62','entity_text':'3,4-Dihydroisoquinoline','canonical_name':'3,4-Dihydroisoquinoline','entity_type':'product_class','family_name':'Bischler-Napieralski Isoquinoline Synthesis','notes':'Generic cyclodehydration product shown on the overview page.','confidence':0.94},
    {'page_label':'p62','entity_text':'Cyclodehydration','canonical_name':'Cyclodehydration','entity_type':'mechanistic_term','family_name':'Bischler-Napieralski Isoquinoline Synthesis','notes':'Named mechanistic descriptor explicitly used on the page.','confidence':0.93},
    {'page_label':'p63','entity_text':'Annoretine','canonical_name':'Annoretine','entity_type':'target_molecule','family_name':'Bischler-Napieralski Isoquinoline Synthesis','notes':'Natural product application formed through Bischler-Napieralski annulation.','confidence':0.90},
    {'page_label':'p63','entity_text':'(-)-Yohimbane','canonical_name':'(-)-Yohimbane','entity_type':'target_molecule','family_name':'Bischler-Napieralski Isoquinoline Synthesis','notes':'Application example on the synthetic applications page.','confidence':0.89},
    {'page_label':'p63','entity_text':'(-)-Ancistrocladine','canonical_name':'(-)-Ancistrocladine','entity_type':'target_molecule','family_name':'Bischler-Napieralski Isoquinoline Synthesis','notes':'Atropisomeric alkaloid assembled using Bischler-Napieralski cyclization.','confidence':0.89},

    {'page_label':'p64','entity_text':'Acyl silane','canonical_name':'Acyl silane','entity_type':'reactant_class','family_name':'Brook Rearrangement','notes':'Characteristic substrate class for Brook rearrangement.','confidence':0.95},
    {'page_label':'p64','entity_text':'Pentacoordinate silicon intermediate','canonical_name':'Pentacoordinate silicon intermediate','entity_type':'intermediate','family_name':'Brook Rearrangement','notes':'Mechanistic intermediate explicitly shown on the page.','confidence':0.95},
    {'page_label':'p64','entity_text':'Retro-Brook rearrangement','canonical_name':'Retro-Brook rearrangement','entity_type':'reaction_term','family_name':'Brook Rearrangement','notes':'Reverse silyl migration process discussed on the overview page.','confidence':0.92},
    {'page_label':'p65','entity_text':'Fredericamycin A spirocyclic core','canonical_name':'Fredericamycin A spirocyclic core','entity_type':'target_molecule','family_name':'Brook Rearrangement','notes':'One-pot aldol/Brook/cyclization application.','confidence':0.90},
    {'page_label':'p65','entity_text':'Tricyclic core of cyathins','canonical_name':'Tricyclic core of cyathins','entity_type':'target_molecule','family_name':'Brook Rearrangement','notes':'Brook-mediated [4+3] annulation application.','confidence':0.89},
    {'page_label':'p65','entity_text':'(+)-Onocerin','canonical_name':'(+)-Onocerin','entity_type':'target_molecule','family_name':'Brook Rearrangement','notes':'Spontaneous Brook rearrangement / dimerization application.','confidence':0.89},

    {'page_label':'p66','entity_text':'Organoborane','canonical_name':'Organoborane','entity_type':'product_class','family_name':'Brown Hydroboration Reaction','notes':'Core intermediate class formed after hydroboration.','confidence':0.95},
    {'page_label':'p66','entity_text':'Four-centered transition state','canonical_name':'Four-centered transition state','entity_type':'mechanistic_term','family_name':'Brown Hydroboration Reaction','notes':'Canonical concerted hydroboration transition state.','confidence':0.94},
    {'page_label':'p66','entity_text':'Anti-Markovnikov addition','canonical_name':'Anti-Markovnikov addition','entity_type':'selectivity_term','family_name':'Brown Hydroboration Reaction','notes':'Regioselectivity explicitly emphasized on the overview page.','confidence':0.93},
    {'page_label':'p67','entity_text':'Trinerine','canonical_name':'Trinerine','entity_type':'target_molecule','family_name':'Brown Hydroboration Reaction','notes':'Hydroboration/oxidation application in indole alkaloid synthesis.','confidence':0.90},
    {'page_label':'p67','entity_text':'cis-Fused decalin','canonical_name':'cis-Fused decalin','entity_type':'product_class','family_name':'Brown Hydroboration Reaction','notes':'Stereochemical outcome highlighted on the application page.','confidence':0.88},
    {'page_label':'p67','entity_text':'(-)-Cassine','canonical_name':'(-)-Cassine','entity_type':'target_molecule','family_name':'Brown Hydroboration Reaction','notes':'Late-stage hydroboration/oxidation example.','confidence':0.89},

    {'page_label':'p68','entity_text':'Ethyl diazoacetate','canonical_name':'Ethyl diazoacetate','entity_type':'reactant_class','family_name':'Buchner Method of Ring Expansion','notes':'Canonical diazo precursor shown on the overview page.','confidence':0.95},
    {'page_label':'p68','entity_text':'Norcaradiene','canonical_name':'Norcaradiene','entity_type':'intermediate','family_name':'Buchner Method of Ring Expansion','notes':'Key valence-isomer intermediate preceding cycloheptatriene formation.','confidence':0.94},
    {'page_label':'p68','entity_text':'Cycloheptatriene derivative','canonical_name':'Cycloheptatriene derivative','entity_type':'product_class','family_name':'Buchner Method of Ring Expansion','notes':'Generic ring-expanded product class.','confidence':0.93},
    {'page_label':'p69','entity_text':'Egualen sodium (KT1-32)','canonical_name':'Egualen sodium (KT1-32)','entity_type':'target_molecule','family_name':'Buchner Method of Ring Expansion','notes':'Azulene-synthesis application on the page.','confidence':0.90},
    {'page_label':'p69','entity_text':'C60-fullerene functionalization','canonical_name':'C60-fullerene functionalization','entity_type':'application_class','family_name':'Buchner Method of Ring Expansion','notes':'Transition-metal-carbenoid Buchner-type functionalization of fullerene.','confidence':0.88},
    {'page_label':'p69','entity_text':'Harringtonolide','canonical_name':'Harringtonolide','entity_type':'target_molecule','family_name':'Buchner Method of Ring Expansion','notes':'Complex polycyclic diazo ketone application.','confidence':0.89},

    {'page_label':'p70','entity_text':'Aryl halide','canonical_name':'Aryl halide','entity_type':'reactant_class','family_name':'Buchwald-Hartwig Cross-Coupling','notes':'Canonical electrophile class for Buchwald-Hartwig coupling.','confidence':0.95},
    {'page_label':'p70','entity_text':'Oxidative addition','canonical_name':'Oxidative addition','entity_type':'mechanistic_term','family_name':'Buchwald-Hartwig Cross-Coupling','notes':'First key Pd-cycle step shown on the mechanism page.','confidence':0.94},
    {'page_label':'p70','entity_text':'Reductive elimination','canonical_name':'Reductive elimination','entity_type':'mechanistic_term','family_name':'Buchwald-Hartwig Cross-Coupling','notes':'Bond-forming step that releases the aryl amine or aryl ether product.','confidence':0.94},
    {'page_label':'p71','entity_text':'(±)-Cyclazocine amine derivative','canonical_name':'(±)-Cyclazocine amine derivative','entity_type':'target_molecule','family_name':'Buchwald-Hartwig Cross-Coupling','notes':'Aryl amination application replacing a phenolic oxygen motif.','confidence':0.90},
    {'page_label':'p71','entity_text':'1,2-Aziridinomitosene','canonical_name':'1,2-Aziridinomitosene','entity_type':'target_molecule','family_name':'Buchwald-Hartwig Cross-Coupling','notes':'Application page example involving Buchwald-Hartwig amination.','confidence':0.89},
    {'page_label':'p71','entity_text':'Polysubstituted phenazine','canonical_name':'Polysubstituted phenazine','entity_type':'product_class','family_name':'Buchwald-Hartwig Cross-Coupling','notes':'Sequential Pd-catalyzed aryl amination application.','confidence':0.89},
]


def apply_frontmatter_batch11(db_path: str | Path) -> Dict[str, Any]:
    return apply_frontmatter_batch_generic(
        db_path=db_path,
        source_label=SOURCE_LABEL,
        version=FRONTMATTER_BATCH11_VERSION,
        latest_source_zip=LATEST_SOURCE_ZIP,
        batch_no=11,
        page_knowledge_seeds=PAGE_KNOWLEDGE_SEEDS,
        family_pattern_seeds=FAMILY_PATTERN_SEEDS,
        abbreviation_seeds=CURATED_BATCH11_ABBREVIATIONS,
        page_entity_seeds=PAGE_ENTITY_SEEDS,
        replace_existing_page_entities=True,
    )


if __name__ == '__main__':
    from pprint import pprint
    pprint(apply_frontmatter_batch11(Path(__file__).resolve().parent / 'labint.db'))
