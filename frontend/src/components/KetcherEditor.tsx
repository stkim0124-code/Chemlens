import React, {
  useEffect,
  useRef,
  useState,
  useImperativeHandle,
  forwardRef,
  useCallback,
} from "react";

/**
 * KetcherEditor - Iframe Isolation React Wrapper
 * - Runs Ketcher Standalone inside /public/ketcher/index.html
 * - Communicates via postMessage
 * - Exposes imperative API: getMolecule(), setMolecule(smiles)
 *
 * NOTE:
 * - In dev you may still see iframe reloads on HMR, but WASM heap is contained in iframe.
 */

type MoleculeData = { smiles: string; mol: string };

export type KetcherEditorHandle = {
  getMolecule: () => Promise<MoleculeData>;
  setMolecule: (smiles: string) => void;
};

type Props = {
  initialSmiles?: string;
  onChange?: (payload: any) => void;
  /** App.jsx expects this to receive an object with getSmiles/setMolecule */
  onInit?: (api: { getSmiles: () => Promise<string>; setMolecule: (smiles: string) => void }) => void;
  height?: number | string;
  className?: string;
  style?: React.CSSProperties;
};

const KetcherEditor = forwardRef<KetcherEditorHandle, Props>(
  ({ initialSmiles = "", onChange, onInit, height = 600, className, style }, ref) => {
    const iframeRef = useRef<HTMLIFrameElement | null>(null);
    // IMPORTANT: keep readiness in ref to avoid React StrictMode/HMR race conditions.
    const isReadyRef = useRef(false);
    const [isReadyUI, setIsReadyUI] = useState(false);
    const [bootError, setBootError] = useState<string | null>(null);

    // Keep latest props without re-binding message handlers.
    const propsRef = useRef<Props>({ initialSmiles, onChange, onInit, height, className, style });
    propsRef.current = { initialSmiles, onChange, onInit, height, className, style };

    // Promise resolver map (requestId -> {resolve,reject,timeoutId})
    type Resolver = { resolve: (v: MoleculeData) => void; reject: (e: any) => void; timeoutId: number };
    const promiseMap = useRef<Map<string, Resolver>>(new Map());
    const pingInterval = useRef<number | null>(null);

    const generateId = () => Math.random().toString(36).substring(2, 9);

    const stopPing = useCallback(() => {
      if (pingInterval.current) {
        window.clearInterval(pingInterval.current);
        pingInterval.current = null;
      }
    }, []);

    const markReady = useCallback(() => {
      if (!isReadyRef.current) {
        isReadyRef.current = true;
        setIsReadyUI(true);
      }
      stopPing();
      setBootError(null);
    }, [stopPing]);

    const initHandshake = useCallback(() => {
      // reset
      isReadyRef.current = false;
      setIsReadyUI(false);
      setBootError(null);
      stopPing();

      // Parent-driven PING loop: bulletproof against READY miss (StrictMode/HMR/cache).
      pingInterval.current = window.setInterval(() => {
        const win = iframeRef.current?.contentWindow;
        if (!win) return;
        win.postMessage({ type: "PING" }, window.location.origin);
      }, 200);
    }, [stopPing]);

    const requestViaPostMessage = useCallback((type: string, payload?: any) => {
      return new Promise<MoleculeData>((resolve, reject) => {
        const win = iframeRef.current?.contentWindow;
        if (!isReadyRef.current || !win) {
          reject(new Error("Ketcher is not ready yet. Handshake incomplete."));
          return;
        }

        const requestId = generateId();

        const timeoutId = window.setTimeout(() => {
          const r = promiseMap.current.get(requestId);
          if (r) {
            promiseMap.current.delete(requestId);
            r.reject(new Error(`Ketcher request timeout (ID: ${requestId})`));
          }
        }, 5000);

        promiseMap.current.set(requestId, { resolve, reject, timeoutId });

        win.postMessage({ type, requestId, payload }, window.location.origin);
      });
    }, []);

    const getMoleculeViaPostMessage = useCallback(() => requestViaPostMessage("GET_MOLECULE"), [requestViaPostMessage]);

    const getSmilesViaPostMessage = useCallback(
      () => requestViaPostMessage("GET_SMILES").then((r) => r.smiles),
      [requestViaPostMessage]
    );

    useEffect(() => {
      // Hard timeout: avoid infinite "Loading..." overlays.
      isReadyRef.current = false;
      setIsReadyUI(false);
      setBootError(null);
      const bootTimer = window.setTimeout(() => {
        setBootError(
          "Ketcher boot timeout (20s). Check that public/ketcher/ contains the OFFICIAL Ketcher standalone build (index.html + static/*)."
        );
      }, 20000);

      const handleMessage = (event: MessageEvent) => {
        // SECURITY: same-origin only (recommended)
        if (event.origin !== window.location.origin) return;
        // Source pinning: only accept messages from the current iframe window
        if (!iframeRef.current || event.source !== iframeRef.current.contentWindow) return;
        const data = event.data || {};
        const { type } = data;

        if (type === "PONG") {
          markReady();
          window.clearTimeout(bootTimer);
          return;
        }

        if (type === "KETCHER_READY") {
          markReady();
          window.clearTimeout(bootTimer);

          // Provide a small API shim for the parent App (compat with existing App.jsx)
          // so App can call getSmiles/getMolfile without knowing about postMessage.
          propsRef.current.onInit?.({
            getSmiles: async () => await getSmilesViaPostMessage(),
            setMolecule: (smiles: string) => {
              iframeRef.current?.contentWindow?.postMessage(
                { type: "SET_MOLECULE", payload: { smiles } },
                window.location.origin
              );
            },
          });

          if (initialSmiles) {
            iframeRef.current?.contentWindow?.postMessage(
              { type: "SET_MOLECULE", payload: { smiles: initialSmiles } },
              window.location.origin
            );
          }
        } else if (type === "KETCHER_DATA") {
          // expected from iframe bridge:
          // { type: 'KETCHER_DATA', requestId, payload: { smiles, mol } }
          const requestId = data.requestId;
          const payload = data.payload || {};
          if (requestId && promiseMap.current.has(requestId)) {
            const r = promiseMap.current.get(requestId)!;
            window.clearTimeout(r.timeoutId);
            promiseMap.current.delete(requestId);
            r.resolve({ smiles: String(payload.smiles ?? ""), mol: String(payload.mol ?? "") });
          }
        } else if (type === "KETCHER_CHANGE") {
          if (onChange) onChange(data.payload);
        } else if (type === "KETCHER_ERROR") {
          // Route per-request errors to the waiting promise.
          const requestId = data.requestId;
          if (requestId && promiseMap.current.has(requestId)) {
            const r = promiseMap.current.get(requestId)!;
            window.clearTimeout(r.timeoutId);
            promiseMap.current.delete(requestId);
            const msg = typeof data?.payload === "string" ? data.payload : (data?.payload?.message || data?.error || "Ketcher error");
            r.reject(new Error(String(msg)));
            return;
          }

          // Non-request boot errors: show once and stop boot timer.
          // eslint-disable-next-line no-console
          console.error("Ketcher iframe error:", data);
          const msg = typeof data?.payload === "string"
            ? data.payload
            : typeof data?.payload?.message === "string"
            ? data.payload.message
            : typeof data?.error === "string"
            ? data.error
            : "Ketcher iframe error";
          setBootError(String(msg));
          window.clearTimeout(bootTimer);
        }
      };

      window.addEventListener("message", handleMessage);
      return () => {
        window.removeEventListener("message", handleMessage);
        window.clearTimeout(bootTimer);
        stopPing();
        // reject outstanding requests to avoid leaks
        promiseMap.current.forEach((r) => {
          window.clearTimeout(r.timeoutId);
          r.reject(new Error("Ketcher bridge unmounted"));
        });
        promiseMap.current.clear();
      };
    }, [initialSmiles, onChange, markReady, stopPing]);

    useImperativeHandle(ref, () => ({
      getMolecule: () => getMoleculeViaPostMessage(),
      setMolecule: (smiles: string) => {
        if (!isReadyRef.current || !iframeRef.current?.contentWindow) return;
        iframeRef.current.contentWindow.postMessage(
          { type: "SET_MOLECULE", payload: { smiles } },
          window.location.origin
        );
      },
    }));

    return (
      <div
        className={className}
        style={{
          width: "100%",
          height,
          border: "1px solid #d1d5db",
          borderRadius: 8,
          overflow: "hidden",
          position: "relative",
          background: "#fff",
          ...style,
        }}
      >
        {!isReadyUI && (
          <div
            style={{
              position: "absolute",
              inset: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              background: "#f9fafb",
              zIndex: 10,
            }}
          >
            <div style={{ color: "#6b7280", fontWeight: 600, padding: 16 }}>
              {bootError ? "Failed to load Chemical Editor" : "Loading Chemical Editor…"}
              <div style={{ fontSize: 12, fontWeight: 500, color: "#9ca3af", marginTop: 6 }}>
                {bootError ? bootError : "(WASM Engine Initializing)"}
              </div>
            </div>
          </div>
        )}

        <iframe
          ref={iframeRef}
          src="/ketcher/index.html"
          title="Ketcher Editor (Isolated)"
          style={{ width: "100%", height: "100%", border: "none" }}
          // Bulletproof handshake starts here.
          onLoad={initHandshake}
        />
      </div>
    );
  }
);

KetcherEditor.displayName = "KetcherEditor";
export default KetcherEditor;
