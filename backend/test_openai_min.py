import os
import requests


def main():
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        raise SystemExit("OPENAI_API_KEY environment variable is not set")

    url = "https://api.openai.com/v1/responses"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "gpt-5-mini",
        "input": "Return exactly this JSON: {\"ok\": true, \"source\": \"openai-min-test\"}",
        "text": {"format": {"type": "json_object"}},
    }

    print("=" * 72)
    print("OPENAI MINIMAL TEXT CALL TEST")
    print("=" * 72)
    print("url:", url)

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
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
