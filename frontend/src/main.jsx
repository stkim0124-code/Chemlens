import "./polyfills.ts";
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.jsx";

// StrictMode removed to avoid React 18 dev double-mount patterns that can break WASM editors.
ReactDOM.createRoot(document.getElementById("root")).render(
  <App />
);
