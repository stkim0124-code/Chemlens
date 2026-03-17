// lodash default-export shim for ESM environments
// Some dependencies (notably ketcher-react / ketcher bundles) may expect:
//   import _ from "lodash";
// but Vite can serve lodash as an ESM module without a default export.
// This shim provides BOTH:
//   - default export: namespace object
//   - named exports: re-exported from lodash

import * as lodashNS from "lodash";

export default lodashNS;
export * from "lodash";
