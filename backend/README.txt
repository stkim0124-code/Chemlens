1) Copy these files into C:\chemlens\backend

2) In Anaconda Prompt:
   conda activate chemlens
   cd /d C:\chemlens\backend

3) Gemini minimal test:
   set GEMINI_API_KEY=YOUR_NEW_KEY
   python test_gemini_min.py

4) OpenAI minimal test:
   set OPENAI_API_KEY=YOUR_KEY
   python test_openai_min.py

Interpretation:
- HTTP 200: the provider can handle at least a tiny one-shot text call
- HTTP 429: the provider is being rate-limited or quota-limited even without the PDF crop pipeline
