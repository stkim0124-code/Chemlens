# What this fixes
Your browser console shows: `Uncaught ReferenceError: process is not defined`.
This happens when a dependency (often `ketcher-*` or `assert`) expects Node.js globals in the browser.

## Apply (Vite + React)
1) Copy `vite.config.fixed.js` to your frontend project root and rename it to `vite.config.js`
   - Path example: `C:\chemlens\frontend\vite.config.js`

2) Install the browser shim package:
   ```
   npm i process
   ```

3) Copy `polyfills.js` into `src/polyfills.js`

4) In your `src/main.jsx` (or `src/main.tsx`), add this as the FIRST import line:
   ```js
   import './polyfills'
   ```

5) Restart Vite dev server:
   - Ctrl+C
   - `npm run dev`

## Verify
Reload the page. The `process is not defined` error should be gone.
