// Minimal browser polyfills for some Node-targeted libs that leak into the bundle.
// Fixes: "Uncaught ReferenceError: process is not defined"
//
// Safe for browser: provides process.env and a no-op nextTick.
// (If a dependency needs more, we can extend this file.)

(() => {
  const g: any = (typeof globalThis !== "undefined") ? globalThis : (window as any);

  if (!g.process) {
    g.process = {
      env: {},
      nextTick: (fn: any, ...args: any[]) => Promise.resolve().then(() => fn(...args)),
    };
  } else {
    if (!g.process.env) g.process.env = {};
    if (!g.process.nextTick) {
      g.process.nextTick = (fn: any, ...args: any[]) => Promise.resolve().then(() => fn(...args));
    }
  }

  // Some libs also expect `global` to exist.
  if (!g.global) g.global = g;
})();
