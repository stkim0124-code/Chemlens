import React, { Suspense, useMemo, useState } from "react";
import KetcherEditor from "./components/KetcherEditor.tsx";
const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

/** Error Boundary */
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }
  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }
  componentDidCatch(error, info) {
    console.error("UI crashed:", error, info);
  }
  render() {
    if (this.state.hasError) {
      return (
        <div style={styles.errorBox}>
          <div style={{ fontWeight: 800, marginBottom: 8 }}>
            UI Error (Ketcher or a component crashed)
          </div>
          <div style={{ color: "#b00020", whiteSpace: "pre-wrap" }}>
            {String(this.state.error)}
          </div>
          <div style={{ marginTop: 10, fontSize: 12, opacity: 0.85 }}>
            (이 경우에도 SMILES 텍스트 입력으로는 진행 가능합니다)
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

function Button({ children, onClick, disabled, variant = "default" }) {
  const st =
    variant === "primary"
      ? { ...styles.btn, ...styles.btnPrimary }
      : { ...styles.btn };
  return (
    <button style={st} onClick={onClick} disabled={disabled}>
      {children}
    </button>
  );
}

export default function App() {
  // Tabs
  const tabs = useMemo(
    () => [
      { key: "search", label: "🔎 구조 검색" },
      { key: "ingest", label: "📥 데이터 인제스트(프로토)" },
      { key: "db", label: "🗂️ DB 보기" },
      { key: "docs", label: "📚 문서 검색" },
    ],
    []
  );
  const [tab, setTab] = useState("search");

    // Input
  const [ketcherApi, setKetcherApi] = useState(null);
  const [smilesText, setSmilesText] = useState("");
  const [molfile, setMolfile] = useState("");
  const [cdxmlText, setCdxmlText] = useState("");

  // Search params
  const [topK, setTopK] = useState(5);
  const [minSim, setMinSim] = useState(0.25);

  // Results / status
  const [status, setStatus] = useState("");
  const [results, setResults] = useState([]);

  // Health / OCR / Docs / Cards
  const [health, setHealth] = useState(null);
  const [ocrInfo, setOcrInfo] = useState(null);
  const [cardsPreview, setCardsPreview] = useState([]);
  const [cardsLimit, setCardsLimit] = useState(200);

  const [docsList, setDocsList] = useState([]);
  const [docsLimit, setDocsLimit] = useState(50);
  const [docsOffset, setDocsOffset] = useState(0);
  const [docsQuery, setDocsQuery] = useState("");
  const [docsHits, setDocsHits] = useState([]);
  const [selectedDoc, setSelectedDoc] = useState(null);
  const [selectedPageNo, setSelectedPageNo] = useState(0);
  const [selectedPageText, setSelectedPageText] = useState("");

  async function fetchJson(url, opts) {
    const res = await fetch(url, opts);
    const ct = res.headers.get("content-type") || "";
    let data = null;
    if (ct.includes("application/json")) data = await res.json();
    else data = await res.text();
    if (!res.ok) {
      const detail = typeof data === "string" ? data : (data?.detail || JSON.stringify(data));
      throw new Error(detail || `HTTP ${res.status}`);
    }
    return data;
  }

  async function refreshHealth() {
    const h = await fetchJson(`${API_BASE}/api/health`);
    setHealth(h);
    try {
      const o = await fetchJson(`${API_BASE}/api/ocr/info`);
      setOcrInfo(o);
    } catch (e) {
      setOcrInfo({ error: String(e) });
    }
  }

  async function loadCardsPreview() {
    const r = await fetchJson(`${API_BASE}/cards?limit=${cardsLimit}`);
    setCardsPreview(r?.items || []);
  }

  async function loadDocs() {
    const r = await fetchJson(`${API_BASE}/api/docs?limit=${docsLimit}&offset=${docsOffset}`);
    setDocsList(Array.isArray(r) ? r : []);
  }

  async function searchDocs() {
    if (!docsQuery.trim()) {
      setDocsHits([]);
      return;
    }
    const r = await fetchJson(
      `${API_BASE}/api/docs/search?q=${encodeURIComponent(docsQuery.trim())}&limit=30&offset=0`
    );
    setDocsHits(r?.hits || []);
  }

  async function openDoc(docId) {
    const d = await fetchJson(`${API_BASE}/api/docs/${docId}`);
    setSelectedDoc(d);
    setSelectedPageNo(0);
    setSelectedPageText("");
  }

  async function openDocPage(docId, pageNo) {
    const p = await fetchJson(`${API_BASE}/api/docs/${docId}/page/${pageNo}`);
    setSelectedPageNo(pageNo);
    setSelectedPageText(p?.text || "");
  }


  async function readFileAsText(file) {
    return new Promise((resolve, reject) => {
      const fr = new FileReader();
      fr.onload = () => resolve(String(fr.result || ""));
      fr.onerror = reject;
      fr.readAsText(file);
    });
  }

  async function onUploadMol(e) {
    const file = e.target.files?.[0];
    if (!file) return;
    const txt = await readFileAsText(file);
    setMolfile(txt);
    setStatus(`Molfile loaded: ${file.name}`);
    e.target.value = "";
  }

  async function onUploadCDXML(e) {
    const file = e.target.files?.[0];
    if (!file) return;
    const txt = await readFileAsText(file);
    setCdxmlText(txt);
    setStatus(`CDXML loaded: ${file.name}`);
    e.target.value = "";
  }

  async function applyFromKetcherAndSearch() {
    if (!ketcherApi) {
      setStatus("Ketcher not initialized");
      return;
    }
    try {
      const raw = await ketcherApi.getSmiles();
      const smi = String(raw || "").trim();
      if (!smi) {
        setStatus("Ketcher에서 유효한 구조를 읽지 못했습니다. 분자 1개를 다시 그려주세요.");
        return;
      }

      let finalSmiles = smi;
      if (smi.includes(">>")) {
        finalSmiles = String(smi.split(">>")[1] || "").trim();
        if (!finalSmiles) {
          setStatus("반응식이 감지되었지만 생성물(Product)이 비어 있습니다. 화살표 오른쪽에 생성물을 넣어주세요.");
          return;
        }
        setStatus("💡 반응식이 감지되어 생성물(Product) 기준으로 검색합니다.");
      } else {
        setStatus("Ketcher 구조를 SMILES로 변환했습니다.");
      }

      setSmilesText(finalSmiles);
      setMolfile("");
      await searchSimilar({ kind: "smiles", value: finalSmiles });
    } catch (e) {
      setStatus(`Ketcher 읽기 실패: ${String(e)}`);
    }
  }

  async function callJson(url, body) {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const text = await r.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }
    if (!r.ok) throw new Error(data?.detail || data?.message || text || `HTTP ${r.status}`);
    return data;
  }

  async function searchSimilar(forcedQuery) {
    setStatus("검색 중...");
    setResults([]);

    // UI inputs:
    // - molfile textarea (Molfile)
    // - smilesText textarea (SMILES)
    // - forcedQuery (optional)
    const query =
      forcedQuery ??
      (smilesText?.trim()
        ? { kind: "smiles", value: smilesText.trim() }
        : null);

    if (!query) {
      setStatus("입력값이 없습니다. Ketcher에서 구조를 그리거나 SMILES를 입력하세요.");
      return;
    }

    try {
      let smiles = null;

      if (query.kind === "smiles") {
        smiles = query.value;
      } else if (typeof query === "string") {
        // legacy: if someone passes plain smiles
        smiles = query;
      } else if (query?.smiles) {
        smiles = query.smiles;
      }

      if (!smiles) {
        setStatus("SMILES를 만들 수 없습니다. Ketcher 또는 SMILES 입력을 확인하세요.");
        return;
      }

      const payload = {
        smiles,
        top_k: topK,
        min_tanimoto: minSim,
      };

      // New backend endpoint (no /api prefix)
      const data = await callJson(`${API_BASE}/search`, payload);

      const rows = data?.hits || [];
      setResults(Array.isArray(rows) ? rows : []);
      setStatus(
        Array.isArray(rows) && rows.length
          ? `결과 ${rows.length}개`
          : "조건 후보가 없습니다. (유사도 값을 낮추거나 DB를 확장하세요.)"
      );
    } catch (e) {
      setStatus(`검색 실패: ${String(e?.message || e || "unknown")}`);
    }
  }

  function renderResultCard(item, idx) {
    const title = item?.name || item?.title || item?.id || `Result #${idx + 1}`;
    const score = item?.tanimoto ?? item?.score ?? item?.similarity;
    const rxn = item?.reaction || item?.conditions || item?.text;

    return (
      <div key={idx} style={styles.card}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
          <div style={{ fontWeight: 800 }}>{title}</div>
          {score !== undefined && score !== null && (
            <span style={styles.pill}>sim: {Number(score).toFixed(3)}</span>
          )}
        </div>
        {rxn ? (
          <div style={{ marginTop: 8, fontSize: 13, whiteSpace: "pre-wrap", lineHeight: 1.4 }}>
            {typeof rxn === "string" ? rxn : JSON.stringify(rxn, null, 2)}
          </div>
        ) : (
          <div style={{ marginTop: 8, fontSize: 12, opacity: 0.7 }}>(상세 필드 없음)</div>
        )}
      </div>
    );
  }

  return (
    <div style={styles.page}>
      <div style={styles.header}>
        <div style={{ fontSize: 22, fontWeight: 900 }}>
          Private Lab-Intelligence (React + FastAPI)
        </div>
        <div style={{ marginTop: 4, opacity: 0.75 }}>
          CDXML/MOL 우선 · 로컬 DB 기반 유사 구조/반응(조건) 추천 데모 (Streamlit app.py UI 이식)
        </div>

        <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
          {tabs.map((t) => (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              style={{
                ...styles.tabBtn,
                ...(tab === t.key ? styles.tabBtnActive : null),
              }}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      <ErrorBoundary>
        {tab === "search" && (
          <div style={styles.grid}>
            <div style={styles.panel}>
              <div style={styles.panelTitle}>입력</div>

              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 10 }}>
                <Button variant="primary" onClick={() => setStatus("Ketcher에서 구조를 그리세요.")}>
                  구조 그리기(에디터)
                </Button>
                <Button onClick={() => setSmilesText("")}>SMILES 비우기</Button>

                <label style={{ ...styles.btn, cursor: "pointer" }}>
                  Molfile(.mol) 업로드
                  <input
                    type="file"
                    accept=".mol,.sdf,.txt"
                    style={{ display: "none" }}
                    onChange={onUploadMol}
                  />
                </label>

                <label style={{ ...styles.btn, cursor: "pointer" }}>
                  CDXML 업로드(프로토)
                  <input
                    type="file"
                    accept=".cdxml,.xml,.txt"
                    style={{ display: "none" }}
                    onChange={onUploadCDXML}
                  />
                </label>
              </div>

              <div style={{ marginTop: 10, fontSize: 12, opacity: 0.8 }}>
                SciFinder처럼 구조를 입력한 뒤 검색합니다. 현재 안정판 Route1은 SMILES 기반 검색만 사용합니다.
              </div>

              {/* Ketcher는 부모 컨테이너 높이가 0이면 "initialized"만 찍고 화면이 비어버립니다. 높이를 고정해 렌더링을 보장합니다. */}
              <div style={{ marginTop: 10, border: "1px solid #ddd", borderRadius: 12, overflow: "hidden", height: 420, minHeight: 420, background: "#fff" }}>
                <Suspense fallback={<div style={{ padding: 12 }}>Ketcher loading...</div>}>
                  <KetcherEditor height={420} onInit={(k) => setKetcherApi(k)} />
                </Suspense>
              </div>

              <div style={{ marginTop: 12, display: "flex", gap: 8, alignItems: "center" }}>
                <Button variant="primary" onClick={applyFromKetcherAndSearch}>
                  Apply (SMILES로 변환 → 검색)
                </Button>
                <div style={{ fontSize: 12, opacity: 0.7 }}>
                  {ketcherApi ? "Ketcher initialized" : "Ketcher not initialized"}
                </div>
              </div>

              <div style={{ marginTop: 12 }}>
                <div style={styles.subTitle}>대체 입력 (SMILES 텍스트)</div>
                <textarea
                  value={smilesText}
                  onChange={(e) => setSmilesText(e.target.value)}
                  placeholder="예: CCO, c1ccccc1, ... (SMILES)"
                  style={styles.textarea}
                />
              </div>

              <div style={{ marginTop: 12 }}>
                <div style={styles.subTitle}>Molfile (표시 전용 / Route1 검색 미사용)</div>
                <textarea
                  value={molfile}
                  onChange={(e) => setMolfile(e.target.value)}
                  placeholder="Molfile 내용"
                  style={styles.textareaSmall}
                />
              </div>

              {cdxmlText ? (
                <div style={{ marginTop: 12 }}>
                  <div style={styles.subTitle}>CDXML (프로토)</div>
                  <textarea value={cdxmlText} readOnly style={styles.textareaSmall} />
                </div>
              ) : null}

              <div style={{ marginTop: 12, display: "flex", gap: 8, alignItems: "center" }}>
                <Button variant="primary" onClick={() => searchSimilar()}>
                  검색
                </Button>
                <Button
                  onClick={() => {
                    setMolfile("");
                    setSmilesText("");
                    setCdxmlText("");
                    setResults([]);
                    setStatus("초기화 완료");
                  }}
                >
                  초기화
                </Button>
                <div style={{ fontSize: 12, opacity: 0.75 }}>
                  요청: top_k={topK}, min_tanimoto={minSim}
                </div>
              </div>

              {status ? <div style={styles.status}>{status}</div> : null}
            </div>

            <div style={styles.panel}>
              <div style={styles.panelTitle}>유사 조건 추천(로컬 DB)</div>

              <div style={{ marginTop: 10 }}>
                <div style={styles.sliderRow}>
                  <div style={styles.sliderLabel}>Top-K</div>
                  <input
                    type="range"
                    min={1}
                    max={50}
                    value={topK}
                    onChange={(e) => setTopK(Number(e.target.value))}
                    style={styles.slider}
                  />
                  <div style={styles.sliderValue}>{topK}</div>
                </div>

                <div style={styles.sliderRow}>
                  <div style={styles.sliderLabel}>최소 유사도(Tanimoto)</div>
                  <input
                    type="range"
                    min={0}
                    max={1}
                    step={0.01}
                    value={minSim}
                    onChange={(e) => setMinSim(Number(e.target.value))}
                    style={styles.slider}
                  />
                  <div style={styles.sliderValue}>{minSim.toFixed(2)}</div>
                </div>

                <div style={{ marginTop: 10, fontSize: 12, opacity: 0.75 }}>
                  좌측에서 구조를 입력한 뒤 “검색”을 누르면, 로컬 DB에서 유사 조건을 추천합니다.
                </div>
              </div>

              <div style={{ marginTop: 16 }}>
                {Array.isArray(results) && results.length ? (
                  results.map(renderResultCard)
                ) : (
                  <div style={{ marginTop: 10, opacity: 0.75 }}>
                    조건 후보가 없습니다. (유사도 컷을 낮추거나 DB를 확장하세요.)
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        
        {tab === "ingest" && (
          <div style={styles.grid}>
            <div style={styles.panel}>
              <div style={styles.panelTitle}>서버 상태</div>
              <div style={{ marginTop: 8, display: "flex", gap: 8, flexWrap: "wrap" }}>
                <Button
                  variant="primary"
                  onClick={async () => {
                    try {
                      setStatus("서버 상태 조회 중...");
                      await refreshHealth();
                      setStatus("서버 상태 업데이트 완료");
                    } catch (e) {
                      setStatus(`상태 조회 실패: ${String(e)}`);
                    }
                  }}
                >
                  상태 새로고침
                </Button>
              </div>

              <div style={styles.status}>
                <div style={{ fontWeight: 800, marginBottom: 6 }}>Health</div>
                <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>
{health ? JSON.stringify(health, null, 2) : "(아직 조회 안 함)"}
                </pre>
              </div>

              <div style={{ ...styles.status, marginTop: 10 }}>
                <div style={{ fontWeight: 800, marginBottom: 6 }}>OCR</div>
                <pre style={{ margin: 0, whiteSpace: "pre-wrap" }}>
{ocrInfo ? JSON.stringify(ocrInfo, null, 2) : "(아직 조회 안 함)"}
                </pre>
              </div>

              <div style={{ marginTop: 10, fontSize: 12, opacity: 0.75 }}>
                ※ OCR 엔진(paddle/tesseract) 미설치면 info에 그대로 표시됩니다. UI는 정상 동작하되 OCR 관련 기능만 제한됩니다.
              </div>
            </div>

            <div style={styles.panel}>
              <div style={styles.panelTitle}>인제스트</div>
              <div style={{ marginTop: 8, display: "flex", gap: 8, flexWrap: "wrap" }}>
                <Button
                  variant="primary"
                  onClick={async () => {
                    try {
                      setStatus("PDF 인제스트 실행 중...");
                      const r = await fetchJson(`${API_BASE}/api/ingest/pdfs`, {
                        method: "POST",
                        headers: { "content-type": "application/json" },
                        body: JSON.stringify({}),
                      });
                      setStatus(`완료: ${JSON.stringify(r)}`);
                    } catch (e) {
                      setStatus(`실패: ${String(e)}`);
                    }
                  }}
                >
                  PDF 인제스트(폴더)
                </Button>

                <Button
                  onClick={async () => {
                    try {
                      setStatus("전체 인제스트 실행 중...");
                      const r = await fetchJson(`${API_BASE}/api/ingest/all`, {
                        method: "POST",
                        headers: { "content-type": "application/json" },
                        body: JSON.stringify({}),
                      });
                      setStatus(`완료: ${JSON.stringify(r)}`);
                    } catch (e) {
                      setStatus(`실패: ${String(e)}`);
                    }
                  }}
                >
                  전체 인제스트
                </Button>
              </div>

              <div style={{ marginTop: 10, fontSize: 12, opacity: 0.75 }}>
                - PDF 인제스트는 backend/app/data/pdfs 폴더의 PDF를 docs DB로 색인합니다.<br />
                - 이미 docs DB가 채워져 있다면 “전체 인제스트”는 건너뛰고, “서버 상태”에서 docs_count를 확인하세요.
              </div>

              {status ? <div style={styles.status}>{status}</div> : null}

              <div style={{ marginTop: 12 }}>
                <div style={{ fontWeight: 800 }}>Molfile 업로드 → DB 카드화</div>
                <div style={{ marginTop: 8, display: "flex", gap: 8, flexWrap: "wrap" }}>
                  <input type="file" accept=".mol,.sdf,.txt" onChange={onUploadMol} />
                  <Button
                    variant="primary"
                    onClick={async () => {
                      try {
                        if (!molfile.trim()) {
                          setStatus("Molfile이 비어있습니다. 먼저 업로드하세요.");
                          return;
                        }
                        setStatus("업로드 중...");
                        const fd = new FormData();
                        fd.append("file", new Blob([molfile], { type: "text/plain" }), "upload.mol");
                        const r = await fetchJson(`${API_BASE}/upload/mol`, { method: "POST", body: fd });
                        setStatus(`완료: ${JSON.stringify(r)}`);
                      } catch (e) {
                        setStatus(`실패: ${String(e)}`);
                      }
                    }}
                  >
                    /upload/mol
                  </Button>
                </div>
              </div>
            </div>
          </div>
        )}

        {tab === "db" && (
          <div style={styles.grid}>
            <div style={styles.panel}>
              <div style={styles.panelTitle}>카드 미리보기</div>

              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 8, alignItems: "center" }}>
                <div style={{ fontSize: 12, opacity: 0.8 }}>limit</div>
                <input
                  type="number"
                  value={cardsLimit}
                  min={1}
                  max={5000}
                  onChange={(e) => setCardsLimit(Number(e.target.value || 200))}
                  style={{ width: 90 }}
                />
                <Button
                  variant="primary"
                  onClick={async () => {
                    try {
                      setStatus("카드 로딩...");
                      await loadCardsPreview();
                      setStatus("카드 로딩 완료");
                    } catch (e) {
                      setStatus(`카드 로딩 실패: ${String(e)}`);
                    }
                  }}
                >
                  불러오기
                </Button>
              </div>

              {status ? <div style={styles.status}>{status}</div> : null}

              <div style={{ marginTop: 12, fontSize: 12, opacity: 0.8 }}>
                /cards API는 현재 DB의 카드(반응/조건) 레코드를 그대로 반환합니다.
              </div>
            </div>

            <div style={styles.panel}>
              <div style={styles.panelTitle}>목록</div>
              <div style={{ marginTop: 10 }}>
                {Array.isArray(cardsPreview) && cardsPreview.length ? (
                  cardsPreview.map(renderResultCard)
                ) : (
                  <div style={{ opacity: 0.75 }}>
                    아직 로딩되지 않았습니다. “불러오기”를 누르세요.
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {tab === "docs" && (
          <div style={styles.grid}>
            <div style={styles.panel}>
              <div style={styles.panelTitle}>문서 목록 / 검색</div>

              <div style={{ marginTop: 8, display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                <Button
                  variant="primary"
                  onClick={async () => {
                    try {
                      setStatus("문서 목록 로딩...");
                      await loadDocs();
                      setStatus("문서 목록 로딩 완료");
                    } catch (e) {
                      setStatus(`문서 목록 실패: ${String(e)}`);
                    }
                  }}
                >
                  목록 불러오기
                </Button>

                <div style={{ fontSize: 12, opacity: 0.8 }}>limit</div>
                <input
                  type="number"
                  value={docsLimit}
                  min={1}
                  max={200}
                  onChange={(e) => setDocsLimit(Number(e.target.value || 50))}
                  style={{ width: 80 }}
                />
                <div style={{ fontSize: 12, opacity: 0.8 }}>offset</div>
                <input
                  type="number"
                  value={docsOffset}
                  min={0}
                  onChange={(e) => setDocsOffset(Number(e.target.value || 0))}
                  style={{ width: 80 }}
                />

                <input
                  value={docsQuery}
                  onChange={(e) => setDocsQuery(e.target.value)}
                  placeholder="키워드 검색 (예: Suzuki, Pd/C, Nrf2...)"
                  style={{ flex: 1, minWidth: 220, padding: "8px 10px", borderRadius: 10, border: "1px solid #ddd" }}
                />
                <Button
                  variant="primary"
                  onClick={async () => {
                    try {
                      setStatus("검색 중...");
                      await searchDocs();
                      setStatus("검색 완료");
                    } catch (e) {
                      setStatus(`검색 실패: ${String(e)}`);
                    }
                  }}
                >
                  검색
                </Button>
              </div>

              {status ? <div style={styles.status}>{status}</div> : null}

              <div style={{ marginTop: 10 }}>
                {docsHits?.length ? (
                  <div>
                    <div style={{ fontWeight: 800, marginBottom: 8 }}>검색 결과</div>
                    {docsHits.map((h, idx) => (
                      <div key={idx} style={styles.card}>
                        <div style={{ fontWeight: 800 }}>{h.doc_title}</div>
                        <div style={{ marginTop: 6, fontSize: 12, opacity: 0.75 }}>
                          doc_id={h.doc_id} · pages {h.page_from}–{h.page_to}
                        </div>
                        <div style={{ marginTop: 8, whiteSpace: "pre-wrap", fontSize: 13 }}>{h.snippet}</div>
                        <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <Button variant="primary" onClick={() => openDoc(h.doc_id)}>
                            문서 열기
                          </Button>
                          <a href={`${API_BASE}/api/docs/${h.doc_id}/pdf`} target="_blank" rel="noreferrer">
                            PDF 열기
                          </a>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>

              <div style={{ marginTop: 12 }}>
                <div style={{ fontWeight: 800, marginBottom: 8 }}>문서 목록</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 10 }}>
                  {docsList.map((d) => (
                    <div key={d.id} style={styles.card}>
                      <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                        <div style={{ width: 84, height: 108, border: "1px solid #eee", borderRadius: 8, overflow: "hidden", background: "#fafafa" }}>
                          <img
                            src={`${API_BASE}/api/docs/${d.id}/thumb`}
                            alt="thumb"
                            style={{ width: "100%", height: "100%", objectFit: "cover" }}
                            onError={(e) => {
                              e.currentTarget.style.display = "none";
                            }}
                          />
                        </div>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontWeight: 900 }}>{d.title}</div>
                          <div style={{ marginTop: 4, fontSize: 12, opacity: 0.75 }}>
                            doc_id={d.id} · pages={d.page_count}
                          </div>
                          <div style={{ marginTop: 8, display: "flex", gap: 8, flexWrap: "wrap" }}>
                            <Button variant="primary" onClick={() => openDoc(d.id)}>
                              열기
                            </Button>
                            <a href={`${API_BASE}/api/docs/${d.id}/pdf`} target="_blank" rel="noreferrer">
                              PDF
                            </a>
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                  {!docsList.length ? <div style={{ opacity: 0.75 }}>(목록을 불러오세요)</div> : null}
                </div>
              </div>
            </div>

            <div style={styles.panel}>
              <div style={styles.panelTitle}>문서 보기</div>

              {selectedDoc ? (
                <>
                  <div style={{ fontWeight: 900 }}>{selectedDoc.title}</div>
                  <div style={{ marginTop: 6, fontSize: 12, opacity: 0.75 }}>
                    doc_id={selectedDoc.id} · pages={selectedDoc.page_count}
                  </div>

                  <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                    <div style={{ fontSize: 12, opacity: 0.8 }}>page</div>
                    <input
                      type="number"
                      value={selectedPageNo}
                      min={0}
                      max={Math.max(0, (selectedDoc.page_count || 1) - 1)}
                      onChange={(e) => setSelectedPageNo(Number(e.target.value || 0))}
                      style={{ width: 80 }}
                    />
                    <Button
                      variant="primary"
                      onClick={async () => {
                        try {
                          setStatus("페이지 로딩...");
                          await openDocPage(selectedDoc.id, selectedPageNo);
                          setStatus("페이지 로딩 완료");
                        } catch (e) {
                          setStatus(`페이지 로딩 실패: ${String(e)}`);
                        }
                      }}
                    >
                      페이지 불러오기
                    </Button>
                  </div>

                  <div style={{ marginTop: 10, whiteSpace: "pre-wrap", fontSize: 13, lineHeight: 1.5 }}>
                    {selectedPageText ? selectedPageText : "(페이지를 불러오세요)"}
                  </div>
                </>
              ) : (
                <div style={{ opacity: 0.75 }}>왼쪽에서 문서를 선택하세요.</div>
              )}
            </div>
          </div>
        )}

        
        {tab !== "search" && tab !== "ingest" && tab !== "db" && tab !== "docs" && (
          <div style={styles.simplePanel}>
            <div style={styles.panelTitle}>
              {tabs.find((t) => t.key === tab)?.label}
            </div>
            <div style={{ marginTop: 8, opacity: 0.8 }}>
              (알 수 없는 탭 상태)
            </div>
          </div>
        )}
      </ErrorBoundary>
    </div>
  );
}

const styles = {
  page: {
    fontFamily:
      'system-ui, -apple-system, Segoe UI, Roboto, "Noto Sans KR", Arial, sans-serif',
    padding: 18,
    background: "#fff",
    color: "#111",
  },
  header: { marginBottom: 14 },
  tabBtn: {
    border: "1px solid #ddd",
    background: "#f6f6f6",
    padding: "8px 12px",
    borderRadius: 10,
    cursor: "pointer",
    fontWeight: 700,
  },
  tabBtnActive: { background: "#111", color: "#fff", borderColor: "#111" },
  grid: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
    gap: 14,
    alignItems: "start",
  },
  panel: {
    border: "1px solid #e5e5e5",
    borderRadius: 14,
    padding: 14,
    background: "#fff",
    minHeight: 740,
  },
  simplePanel: {
    border: "1px solid #e5e5e5",
    borderRadius: 14,
    padding: 14,
    background: "#fff",
  },
  panelTitle: { fontWeight: 900, fontSize: 16 },
  subTitle: { fontWeight: 800, fontSize: 13, marginBottom: 6 },
  btn: {
    border: "1px solid #ddd",
    background: "#fff",
    padding: "8px 10px",
    borderRadius: 10,
    cursor: "pointer",
    fontWeight: 700,
  },
  btnPrimary: { background: "#111", color: "#fff", borderColor: "#111" },
  pill: {
    fontSize: 12,
    border: "1px solid #ddd",
    padding: "2px 8px",
    borderRadius: 999,
    background: "#fafafa",
    opacity: 0.9,
    whiteSpace: "nowrap",
  },
  textarea: {
    width: "100%",
    minHeight: 76,
    borderRadius: 10,
    border: "1px solid #ddd",
    padding: 10,
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
    fontSize: 12,
    resize: "vertical",
  },
  textareaSmall: {
    width: "100%",
    minHeight: 90,
    borderRadius: 10,
    border: "1px solid #ddd",
    padding: 10,
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
    fontSize: 11.5,
    resize: "vertical",
  },
  status: {
    marginTop: 12,
    fontSize: 12,
    opacity: 0.8,
    background: "#fafafa",
    border: "1px solid #eee",
    padding: 10,
    borderRadius: 10,
  },
  sliderRow: {
    display: "grid",
    gridTemplateColumns: "140px 1fr 60px",
    gap: 10,
    alignItems: "center",
    marginTop: 10,
  },
  sliderLabel: { fontSize: 13, fontWeight: 800 },
  slider: { width: "100%" },
  sliderValue: {
    textAlign: "right",
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
    fontSize: 12,
    opacity: 0.85,
  },
  card: {
    border: "1px solid #eee",
    background: "#fff",
    borderRadius: 12,
    padding: 12,
    marginTop: 10,
    boxShadow: "0 1px 0 rgba(0,0,0,0.03)",
  },
  errorBox: { border: "1px solid #f3b4b4", background: "#fff5f5", padding: 14, borderRadius: 12 },
};