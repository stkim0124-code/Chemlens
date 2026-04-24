"""Reproduce Sandmeyer 1237 + Schmidt 1240 query with current code and DB."""
import sys, importlib.util, types, typing
from pathlib import Path

here = Path(r'C:\chemlens\backend')
sys.path.insert(0, str(here))

# fastapi shim
try:
    import fastapi
except ImportError:
    stub = types.ModuleType('fastapi')
    class HTTPException(Exception):
        def __init__(self, status_code=500, detail=None):
            self.status_code = status_code; self.detail = detail
    class APIRouter:
        def get(self, *a, **k): return lambda f: f
        def post(self, *a, **k): return lambda f: f
    def Query(default=None, *a, **k): return default
    stub.APIRouter = APIRouter; stub.HTTPException = HTTPException; stub.Query = Query
    sys.modules['fastapi'] = stub

# Load evidence_search.py
spec = importlib.util.spec_from_file_location('chemlens_evidence_search', str(here / 'app' / 'evidence_search.py'))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
if hasattr(mod.StructureEvidenceRequest, 'model_rebuild'):
    mod.StructureEvidenceRequest.model_rebuild(_types_namespace={'Optional': typing.Optional})
mod.DB_PATH = here / 'app' / 'labint.db'

# Probe Sandmeyer
print('\n=== Sandmeyer 1237: Nc1ccccc1>>Clc1ccccc1 ===')
req = mod.StructureEvidenceRequest(reaction_smiles='Nc1ccccc1>>Clc1ccccc1', top_k=5, min_tanimoto=0.25)
res = mod._search_by_reaction(req)
for i, it in enumerate(res.get('results', [])[:5]):
    print(f'  #{i+1} {it.get("reaction_family_name")!r}  score={it.get("match_score"):.3f}')

# Probe Schmidt
print('\n=== Schmidt 1240: O=C(c1ccccc1)c1ccccc1>>O=C(Nc1ccccc1)c1ccccc1 ===')
req = mod.StructureEvidenceRequest(reaction_smiles='O=C(c1ccccc1)c1ccccc1>>O=C(Nc1ccccc1)c1ccccc1', top_k=5, min_tanimoto=0.25)
res = mod._search_by_reaction(req)
for i, it in enumerate(res.get('results', [])[:5]):
    print(f'  #{i+1} {it.get("reaction_family_name")!r}  score={it.get("match_score"):.3f}')
