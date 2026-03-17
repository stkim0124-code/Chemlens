# Fix: Invalid hook call / useRef dispatcher null (ketcher-react)

If the browser console shows:
- `Warning: Invalid hook call...`
- `Cannot read properties of null (reading 'useRef')`
inside `ketcher-react`'s `EditorContainer`

Apply this patch and then **clear Vite optimize cache**:

```bat
cd frontend
rmdir /s /q node_modules\.vite
npm install
npm run dev
```

Why: Vite's optimizeDeps prebundle can accidentally create **two React instances** across dependency boundaries,
especially with large ESM deps (ketcher + MUI + emotion). This patch forces a single React via alias+dedupe and
excludes ketcher/emotion/mui from optimizeDeps.
