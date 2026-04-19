import os
import json
import requests


def main():
    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        raise SystemExit("GEMINI_API_KEY environment variable is not set")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key={key}"
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": 'Return exactly this JSON: {"ok": true, "source": "gemini-min-test"}'}
                ]
            }
        ],
        "generationConfig": {
            "responseMimeType": "application/json"
        }
    }

    print("=" * 72)
    print("GEMINI MINIMAL TEXT CALL TEST")
    print("=" * 72)
    print("url:", url.split("?key=")[0] + "?key=***")

    try:
        r = requests.post(url, json=payload, timeout=60)
        print("status_code:", r.status_code)
        print("response_text_preview:")
        print(r.text[:2000])
        print("=" * 72)
        if r.status_code == 200:
            print("RESULT: HTTP 200 -> key/project/quota is at least capable of one minimal text call")
        elif r.status_code == 429:
            print("RESULT: HTTP 429 -> this is NOT caused by your PDF crop pipeline itself")
            print("        It means even a one-shot tiny text call is being rate-limited or quota-limited.")
        else:
            print("RESULT: Non-200/429 response. Inspect the response body above.")
    except Exception as e:
        print("EXCEPTION:", repr(e))
        raise


if __name__ == "__main__":
    main()
