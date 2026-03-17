# CHEMLENS Frontend (React + Vite)

## One Source of Truth (Frontend)
- Entry: `index.html` → `src/main.jsx`
- Ketcher editor is isolated in an iframe:
  - Parent UI: `src/components/KetcherEditor.tsx`
  - Iframe app: `ketcher_iframe.html` → `src/ketcher_iframe/main.tsx`
- Backend base URL:
  - `.env` (local only): `VITE_API_BASE=http://127.0.0.1:8000`
  - Dev proxy is enabled in `vite.config.js` for `/api`, `/search`, `/upload`, `/cards`.

## Run
```bash
npm install
npm run dev
```

## Notes
- React StrictMode is intentionally disabled (React 18 dev double-mount can break WASM editors).
- `src/polyfills.ts` provides minimal `process` global to avoid `process is not defined` errors from some deps.
