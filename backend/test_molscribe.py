from pathlib import Path
from molscribe import MolScribe
import huggingface_hub

model_path = huggingface_hub.hf_hub_download(
    "yujieq/MolScribe", "swin_base_char_aux_1m.pth"
)

model = MolScribe(model_path, device="cpu")

images_dir = Path("app/data/images/named reactions")
files = list(images_dir.glob("*.jpg"))[:5]

print(f"테스트 이미지 수: {len(files)}")
success = 0

for f in files:
    try:
        result = model.predict_image_file(str(f))
        smiles = result.get("smiles", "")
        if smiles:
            print(f"OK: {f.name} -> {smiles[:60]}")
            success += 1
        else:
            print(f"EMPTY: {f.name}")
    except Exception as e:
        print(f"FAIL: {f.name} -> {e}")

print(f"성공률: {success}/{len(files)}")