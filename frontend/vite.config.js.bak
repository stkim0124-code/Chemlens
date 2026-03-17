import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "path";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const apiBase = env.VITE_API_BASE || "http://127.0.0.1:8000";

  return {
    plugins: [react()],
    resolve: {
      // Ensure a single copy of React to prevent "Invalid hook call" crashes.
      dedupe: ["react", "react-dom", "@emotion/react", "@emotion/styled"],
    },
    define: {
      // Some deps may still look for Node-ish globals.
      "process.env": {},
      global: "globalThis",
    },
    server: {
      headers: {
        'Cross-Origin-Embedder-Policy': 'require-corp',
        'Cross-Origin-Opener-Policy': 'same-origin',
      },
      // Dev DX: call backend without CORS pain.
      proxy: {
        "/api": { target: apiBase, changeOrigin: true },
        "/search": { target: apiBase, changeOrigin: true },
        "/upload": { target: apiBase, changeOrigin: true },
        "/cards": { target: apiBase, changeOrigin: true },
      },
    },
    build: {
      rollupOptions: {
        input: {
          main: resolve(__dirname, "index.html"),
        },
      },
    },
  };
});
