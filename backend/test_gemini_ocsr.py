import os
import base64
import json
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
MODEL = "gemini-2.5-pro"

def image_to_base64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

def ask_gemini(image_path):
    url = "https://generativelanguage.googleapis.com/v1beta/models/" + MODEL + ":generateContent?key=" + API_KEY
    b64 = image_to_base64(image_path)
    payload = {
        "contents": [{
            "parts": [
                {
                    "inline_data": {
                        "mime_type": "image/jpeg",
                        "data": b64
                    }
                },
                {
                    "text": "Extract all chemical structures from this image and return as SMILES strings. Output JSON only with no extra text or markdown: {\"molecules\": [\"SMILES1\", \"SMILES2\"]}"
                }
            ]
        }]
    }
    r = requests.post(url, json=payload, timeout=60)
    data = r.json()
    if "error" in data:
        raise Exception(str(data["error"]))
    candidates = data.get("candidates", [])
    if not candidates:
        raise Exception("Empty response: " + json.dumps(data)[:300])
    text = candidates[0]["content"]["parts"][0]["text"].strip()
    if "```" in text:
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else parts[0]
        if text.startswith("json"):
            text = text[4:].strip()
    return json.loads(text.strip())

base = Path("app/data/images")
folder = base / "named reactions"

files = [
    folder / "named reactions_300.jpg",
    folder / "named reactions_20.jpg",
]

print("이미지 2장 테스트")
print("folder:", folder)
print("exists:", folder.exists())
print()

for f in files:
    print("trying:", f)
    print("exists:", f.exists())
    if not f.exists():
        print("SKIP: file not found")
        print()
        continue
    try:
        res = ask_gemini(str(f))
        mols = res.get("molecules", [])
        print("OK:", f.name)
        print("추출된 구조", len(mols), "개:")
        for s in mols:
            print("  ", s)
    except Exception as e:
        print("FAIL:", f.name, "->", str(e))
    print()