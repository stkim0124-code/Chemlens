// ESM shim for `lodash/lodash` so that ESM imports can safely use a default export.
// We intentionally import from the package root (CommonJS) which Vite/esbuild can
// wrap with a default export.
import lodash from "lodash";
export default lodash;
