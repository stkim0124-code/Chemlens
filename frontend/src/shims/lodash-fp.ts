// ESM shim for `lodash/fp`.
// ketcher-react imports named exports from `lodash/fp`.
// Under some Vite/ESM resolutions, `lodash/fp` is exposed without proper named exports.
// We expose the commonly used named functions off the default fp object.
//
// If you hit a missing export at runtime, add it here as: `export const <name> = fp.<name>;`
import fp from "lodash/fp";

// Commonly used by ketcher-react (observed in stack traces)
export const escapeRegExp = fp.escapeRegExp;
export const flow = fp.flow;
export const filter = fp.filter;
export const reduce = fp.reduce;
export const throttle = fp.throttle;
export const xor = fp.xor;
export const debounce = fp.debounce;
export const upperFirst = fp.upperFirst;

// Additional safe exports (helpful for future)
export const map = fp.map;
export const omit = fp.omit;
export const round = fp.round;
export const compact = fp.compact;
export const uniq = fp.uniq;
export const uniqBy = fp.uniqBy;
export const sortBy = fp.sortBy;

export default fp;
