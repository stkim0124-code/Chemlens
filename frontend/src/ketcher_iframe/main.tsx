import "../polyfills.ts";
import React from "react";
import ReactDOM from "react-dom/client";
import { StandaloneStructServiceProvider as StandaloneStructServiceProviderType } from "ketcher-standalone";
import { Editor } from "ketcher-react";
import "ketcher-react/dist/index.css";

// ----
// Ketcher iframe app
// - Runs the real Ketcher React editor INSIDE the iframe to isolate WASM + React lifecycle.
// - Parent communicates ONLY via postMessage.
// - Parent-facing API (message types):
//   - KETCHER_READY
//   - KETCHER_DATA { requestId, smiles, mol }
//   - KETCHER_ERROR
//   - Parent -> iframe: SET_MOLECULE { smiles }
//   - Parent -> iframe: GET_MOLECULE { requestId }
// ----

type KetcherType = {
  getSmiles: () => Promise<string>;
  getMolfile: () => Promise<string>;
  setMolecule: (smiles: string) => Promise<void> | void;
};

const ORIGIN = window.location.origin;

function post(type: string, payload?: any, requestId?: string) {
  try {
    window.parent?.postMessage({ type, payload, requestId }, "*");
  } catch {
    // ignore
  }
}

// ketcher-standalone exports a type, but runtime is constructible.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const StandaloneStructServiceProvider = StandaloneStructServiceProviderType as unknown as new () => any;

const structServiceProvider = new StandaloneStructServiceProvider();

function IframeApp() {
  const ketcherRef = React.useRef<KetcherType | null>(null);

  React.useEffect(() => {
    const onMessage = async (event: MessageEvent) => {
      const data: any = event.data || {};

      if (data.type === "PING") {
        post("PONG");
        return;
      }

      const ketcher = ketcherRef.current;
      if (!ketcher) return;

      try {
        if (data.type === "SET_MOLECULE") {
          const smiles = data.payload?.smiles ? String(data.payload.smiles) : "";
          await ketcher.setMolecule(smiles);
        }

        if (data.type === "GET_MOLECULE") {
          const requestId = String(data.requestId || "");
          const smiles = await ketcher.getSmiles();
          const mol = await ketcher.getMolfile();
          post("KETCHER_DATA", { smiles, mol }, requestId);
        }

        if (data.type === "GET_SMILES") {
          const requestId = String(data.requestId || "");
          const wantReaction = !!data.payload?.reaction;
          let smiles = "";
          try {
            smiles = wantReaction ? await (ketcher as any).getSmiles(true) : await ketcher.getSmiles();
          } catch {
            smiles = await ketcher.getSmiles();
          }
          post("KETCHER_DATA", { smiles, mol: "" }, requestId);
        }

        if (data.type === "GET_MOLFILE") {
          const requestId = String(data.requestId || "");
          const mol = await ketcher.getMolfile();
          post("KETCHER_DATA", { smiles: "", mol }, requestId);
        }
      } catch (e: any) {
        post("KETCHER_ERROR", { message: String(e?.message || e) });
      }
    };

    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  }, []);

  return (
    <div style={{ width: "100%", height: "100%" }}>
      <Editor
        // Use the pinned static resources bundle under /public/ketcher for stable WASM/worker loading (dev/prod).
        staticResourcesUrl={"/ketcher"}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        structServiceProvider={structServiceProvider as any}
        errorHandler={(message: string | Error) => {
          // Surface inside iframe + to parent
          // eslint-disable-next-line no-console
          console.error("Ketcher error:", message);
          post("KETCHER_ERROR", { message: String(message) });
        }}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        onInit={(ketcher: any) => {
          ketcherRef.current = ketcher as KetcherType;
          // Expose for debugging (optional)
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (window as any).ketcher = ketcher;
          post("KETCHER_READY");
        }}
      />
    </div>
  );
}

// StrictMode intentionally not used (avoids dev double-mount patterns)
ReactDOM.createRoot(document.getElementById("ketcher-root")!).render(<IframeApp />);
