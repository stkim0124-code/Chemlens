"""smiles_guard.py — RDKit-free strict SMILES validator for CHEMLENS phase-apply pipeline.

Purpose
-------
Pre-validate SMILES strings *before* they reach the canonical DB, so that RDKit
parse failures that previously required user-side rdkit_audit can be caught in
the sandbox where RDKit is unavailable.

This is NOT a full replacement for RDKit. It catches structural / tokenization
errors and a blacklist of patterns that historically broke RDKit kekulization
(Fischer Indole tetrahydrocarbazole fusion, Glaser terminal alkyne on aromatic
etc.). Conservative by design: when in doubt, reject.

Usage (from an apply script):

    from smiles_guard import is_smiles_safe, validate_entry
    ok, reasons = is_smiles_safe("CCO")
    if not ok:
        raise SystemExit(f"REJECT: {reasons}")

Invocation as CLI:

    python smiles_guard.py "CCO"  "c1ccc2[nH]c3CCCCCc3cc2c1"

Exit code 0 = all safe, 1 = at least one rejected.

Author: CHEMLENS autonomous loop (Claude-backed). Phase16+.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from typing import Iterable

# -----------------------------------------------------------------------------
# Token constants
# -----------------------------------------------------------------------------

# Organic subset outside brackets (two-letter first so regex is greedy correct)
ORGANIC_SUBSET = ("Cl", "Br", "B", "C", "N", "O", "P", "S", "F", "I")
AROMATIC_LOWER = set("bcnops")

# Allowed atoms inside brackets. This is a whitelist; anything else => reject.
BRACKET_ALLOWED_ATOMS = {
    # Period 1-2 + halogens + common heteroatoms + a few metals used in seeds
    "H", "He", "Li", "Be", "B", "C", "N", "O", "F", "Ne",
    "Na", "Mg", "Al", "Si", "P", "S", "Cl", "Ar",
    "K", "Ca", "Se", "Br", "I",
    # Phase 3f 20260421: Sn added for Stille Cross-Coupling organotin reagents
    # (e.g. [Sn]([CH3])([CH3])[CH3], [Sn](CCCC)(CCCC)CCCC tributylstannyl).
    # Legacy Stille DB id=699 was ingested before smiles_guard and also uses [Sn].
    "Sn",
    # aromatic lowercase versions allowed inside brackets
    "b", "c", "n", "o", "p", "s", "se",
}

BOND_CHARS = set("-=#:$/\\.")  # SMILES bond/separator tokens

# -----------------------------------------------------------------------------
# Known-failure pattern blacklist
# -----------------------------------------------------------------------------
#
# Each entry: (regex, human_reason). These are literal-SMILES patterns that we
# have empirically seen RDKit choke on during prior phase applies.

BLACKLIST_PATTERNS: list[tuple[re.Pattern, str]] = [
    # Fischer Indole — tetrahydrocarbazole written as fully aromatic fused
    # ring with aliphatic saturation in same fused system. RDKit kekulization
    # fails on c1ccc2[nH]c3CCCCCc3cc2c1 style strings.
    (re.compile(r"\[nH\]c\d+CCCC"), "aromatic_nH_ring_fused_to_sp3_chain (Fischer Indole class)"),

    # Glaser terminal alkyne with explicit [H]-less `C#CH` on aromatic.
    # The canonical form should be C#C (implicit H) — explicit CH on
    # terminal alkyne often fails RDKit implicit-H inference.
    (re.compile(r"C#CH(?![a-zA-Z])"), "terminal_alkyne_with_explicit_CH_suffix (Glaser class)"),

    # Bare aromatic ring closing to sp3 without kekulization bond hint
    # e.g. 'c1cc...CC1' where last 1 bonds aromatic c to sp3 C directly
    # — context-sensitive, we flag only the narrower common failure.
    (re.compile(r"c\d+CCCC\d"), "aromatic_to_saturated_chain_ring_closure_ambiguous"),

    # Mixed stereo + aromatic with no chiral assignment in bracket atom —
    # almost always a seed-authoring slip. Example: /c1ccccc1 with no @/@@
    #
    # Phase 3f-2 20260423: narrowed to ONLY the authoring-slip shape — slash at
    # SMILES start or immediately after a context-break char `(`, `,`, `.`, `[`.
    # PubChem canonical output uses `C=C/c1...` legitimately for E/Z stereo on
    # double bonds conjugated to aromatic rings (e.g. Epothilone A/B, Rhizoxin D,
    # Phorboxazole A, Fredericamycin A). RDKit parses all such cases cleanly —
    # the old unanchored regex produced 11 false positives on real natural-product
    # PubChem output. Sanity checks: `/c1...` (slip) HIT, `C=C/c1...` (conjugation) OK.
    (re.compile(r"(?:^|[(,.\[])[/\\]c\d"), "stereo_slash_followed_by_aromatic_lowercase"),
]

# -----------------------------------------------------------------------------
# Core result object
# -----------------------------------------------------------------------------

@dataclass
class ValidationResult:
    smiles: str
    ok: bool
    reasons: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {"smiles": self.smiles, "ok": self.ok, "reasons": list(self.reasons)}


# -----------------------------------------------------------------------------
# Token-level scanner
# -----------------------------------------------------------------------------

def _tokenize(smiles: str) -> list[str] | None:
    """Simple left-to-right SMILES tokenizer. Returns None on tokenization
    failure. Tokens:
      - `[...]` bracket atom (kept as-is including brackets)
      - one- or two-letter organic subset symbol
      - aromatic lowercase
      - bond chars, parens, ring closure digits (1-9 or %nn)
    """
    i = 0
    n = len(smiles)
    tokens: list[str] = []
    while i < n:
        ch = smiles[i]
        # bracket atom
        if ch == "[":
            j = smiles.find("]", i)
            if j == -1:
                return None
            tokens.append(smiles[i:j + 1])
            i = j + 1
            continue
        # two-letter organic subset
        if i + 1 < n:
            two = smiles[i:i + 2]
            if two in ORGANIC_SUBSET:
                tokens.append(two)
                i += 2
                continue
        # single char organic subset
        if ch in ORGANIC_SUBSET:
            tokens.append(ch)
            i += 1
            continue
        # aromatic lowercase
        if ch in AROMATIC_LOWER:
            tokens.append(ch)
            i += 1
            continue
        # parens
        if ch in "()":
            tokens.append(ch)
            i += 1
            continue
        # bond chars
        if ch in BOND_CHARS:
            tokens.append(ch)
            i += 1
            continue
        # ring closure digits
        if ch.isdigit():
            tokens.append(ch)
            i += 1
            continue
        # %nn ring closure
        if ch == "%":
            if i + 2 < n and smiles[i + 1].isdigit() and smiles[i + 2].isdigit():
                tokens.append(smiles[i:i + 3])
                i += 3
                continue
            return None
        # whitespace — reject; canonical SMILES should not contain spaces
        if ch.isspace():
            return None
        # unknown char
        return None
    return tokens


def _check_bracket_atom(tok: str) -> tuple[bool, str]:
    """Validate contents of a bracket atom like `[C@H]`, `[NH4+]`, `[13CH3]`."""
    inner = tok[1:-1]
    if not inner:
        return False, "empty_bracket_atom"
    # strip leading isotope digits
    j = 0
    while j < len(inner) and inner[j].isdigit():
        j += 1
    rest = inner[j:]
    if not rest:
        return False, "bracket_atom_no_symbol"
    # atom symbol: first 1-2 chars trying two-letter first
    sym = None
    if len(rest) >= 2 and rest[:2] in BRACKET_ALLOWED_ATOMS:
        sym = rest[:2]
        rest = rest[2:]
    else:
        sym = rest[:1]
        rest = rest[1:]
    if sym not in BRACKET_ALLOWED_ATOMS:
        return False, f"bracket_atom_not_whitelisted:{sym}"
    # rest may contain @, @@, H, H\d+, +, -, + digit, - digit
    allowed_re = re.compile(r"^(@{1,2})?(H\d*)?([+\-]\d*)*$")
    if not allowed_re.match(rest):
        return False, f"bracket_atom_unusual_suffix:{rest}"
    return True, ""


def _check_balance(smiles: str) -> tuple[bool, list[str]]:
    """Check paren balance and ring-closure digit pairing.

    Ring closure digits must appear in pairs (each digit that appears odd
    times is a dangling ring bond).
    """
    errs: list[str] = []
    # paren balance
    depth = 0
    for ch in smiles:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                errs.append("paren_close_before_open")
                break
    if depth != 0:
        errs.append(f"paren_imbalance:{depth}")

    # ring closures: count single-digit occurrences outside brackets
    # crude: strip bracket contents first
    stripped = re.sub(r"\[[^\]]*\]", "", smiles)
    # extract %nn tokens first
    pct_tokens = re.findall(r"%\d\d", stripped)
    from collections import Counter
    pct_count = Counter(pct_tokens)
    stripped = re.sub(r"%\d\d", "", stripped)
    # single digit ring closures
    digit_count = Counter(ch for ch in stripped if ch.isdigit())
    for d, c in digit_count.items():
        if c % 2 != 0:
            errs.append(f"ring_closure_digit_odd:{d}(count={c})")
    for d, c in pct_count.items():
        if c % 2 != 0:
            errs.append(f"ring_closure_pct_odd:{d}(count={c})")

    return (len(errs) == 0, errs)


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

def is_smiles_safe(smiles: str) -> tuple[bool, list[str]]:
    """Return (ok, reasons). When ok=False, reasons explains why."""
    reasons: list[str] = []
    if not isinstance(smiles, str):
        return False, ["not_a_string"]
    s = smiles.strip()
    if not s:
        return False, ["empty_smiles"]
    # length sanity
    if len(s) > 400:
        reasons.append(f"excessive_length:{len(s)}")

    # blacklist first (short-circuit for known failures)
    for pat, reason in BLACKLIST_PATTERNS:
        if pat.search(s):
            reasons.append(f"blacklist:{reason}")

    # balance / ring closure
    bal_ok, bal_errs = _check_balance(s)
    if not bal_ok:
        reasons.extend(bal_errs)

    # tokenize
    toks = _tokenize(s)
    if toks is None:
        reasons.append("tokenization_failed")
        return False, reasons

    # inspect bracket atoms
    for t in toks:
        if t.startswith("["):
            ok, err = _check_bracket_atom(t)
            if not ok:
                reasons.append(err)

    # two-character organic-subset double letters that are common slips:
    # reject 'BN' / 'SN' etc treated as atoms — already filtered by tokenizer
    # (they'd be tokenized as single-letter B + N). So no extra check.

    ok = len(reasons) == 0
    return ok, reasons


def validate_entry(
    reactant_smiles: str | None,
    product_smiles: str | None,
    family: str | None = None,
) -> ValidationResult:
    """Wrapper that validates both sides of a reaction entry. Returns a single
    ValidationResult with .ok=True iff both are individually safe (or empty)."""
    reasons: list[str] = []
    for label, smi in [("reactant", reactant_smiles), ("product", product_smiles)]:
        if smi is None or smi == "":
            continue
        ok, rs = is_smiles_safe(smi)
        if not ok:
            for r in rs:
                reasons.append(f"{label}:{r}")
    ok = len(reasons) == 0
    return ValidationResult(
        smiles=f"R={reactant_smiles or ''} | P={product_smiles or ''}",
        ok=ok,
        reasons=reasons,
    )


def validate_batch(entries: Iterable[dict]) -> list[ValidationResult]:
    """Validate a batch of {reactant_smiles, product_smiles, family} dicts."""
    out: list[ValidationResult] = []
    for e in entries:
        r = validate_entry(
            reactant_smiles=e.get("reactant_smiles"),
            product_smiles=e.get("product_smiles"),
            family=e.get("family"),
        )
        out.append(r)
    return out


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def _cli() -> int:
    if len(sys.argv) < 2:
        print("usage: python smiles_guard.py '<smiles>' ['<smiles2>' ...]")
        return 2
    any_bad = False
    for smi in sys.argv[1:]:
        ok, reasons = is_smiles_safe(smi)
        tag = "OK  " if ok else "FAIL"
        print(f"{tag} {smi!r}  reasons={reasons}")
        if not ok:
            any_bad = True
    return 1 if any_bad else 0


if __name__ == "__main__":
    sys.exit(_cli())
