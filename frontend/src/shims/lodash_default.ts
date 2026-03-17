// Shim to provide a default export for lodash when a dependency imports "lodash/lodash" or "lodash/lodash.js".
// Some Ketcher/Indigo builds import lodash via a deep path that Vite serves as ESM without a default export.
// This shim forces a stable default export while preserving named exports.

import lodash from "lodash";

export default lodash;
export * from "lodash";
