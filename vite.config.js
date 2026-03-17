import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// Fix for deps that expect Node globals (process/global)
// e.g. ketcher-standalone / assert -> "process is not defined" in the browser
export default defineConfig({
  plugins: [react()],
  define: {
    // Many libs only read process.env.* (string keys), so this is sufficient.
    'process.env': {},
    global: 'globalThis',
  },
  resolve: {
    // Force a SINGLE React instance.
    // Invalid hook call / "Cannot read properties of null (reading 'useRef')" usually means React was loaded twice
    // (e.g., Vite prebundle cache or dependency graph pulling a second copy).
    dedupe: ['react', 'react-dom'],

    // Some libs import "process" explicitly; map it to the browser shim.
    // Also hard-pin react/react-dom entrypoints to this project's node_modules to prevent duplicate resolution.
    alias: {
      process: 'process/browser',

      // Hard-pin React to avoid duplicate instances via nested deps / prebundle.
      react: path.resolve(__dirname, 'node_modules/react'),
      'react-dom': path.resolve(__dirname, 'node_modules/react-dom'),
      'react/jsx-runtime': path.resolve(__dirname, 'node_modules/react/jsx-runtime.js'),
      'react/jsx-dev-runtime': path.resolve(__dirname, 'node_modules/react/jsx-dev-runtime.js'),
    },
  },
  optimizeDeps: {
    // Keep the node global shim.
    include: ['process'],

    // ketcher-* (and its deep deps) can trip Vite's prebundle cache and cause React to be resolved twice.
    // Excluding them makes Vite treat them like regular source deps while still deduping React.
    exclude: ['ketcher-react', 'ketcher-core', 'ketcher-standalone'],
  },
})
