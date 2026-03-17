Ketcher vendor sync hotfix [d6448519]
==================================

What changed
- sync-ketcher-public.mjs no longer assumes node_modules/ketcher-standalone/dist contains index.html.
- It searches recursively for index.html (or any .html) inside:
    1) frontend/node_modules/ketcher-standalone/
    2) frontend/ketcher_standalone_src/   (fallback)

If none found
- It prints guidance and exits 0 so `npm run dev` still starts.

To actually make Ketcher iframe READY
- Provide a real "Ketcher Standalone web build" (contains index.html, css, js, assets).
- Put it under:
    C:\chemlens\frontend\ketcher_standalone_src\
  (Extract the official standalone release zip there)

Then:
- npm run dev
and the script will copy it to:
- frontend/public/ketcher/vendor/
