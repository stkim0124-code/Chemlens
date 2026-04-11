
import React, { useEffect, useState } from "react";

function Button({ children, onClick, disabled, variant = "default" }) {
  const base = {
    padding: "10px 14px",
    borderRadius: 10,
    border: "1px solid #d0d7de",
    background: variant === "primary" ? "#111827" : "#fff",
    color: variant === "primary" ? "#fff" : "#111827",
    cursor: disabled ? "not-allowed" : "pointer",
    fontWeight: 700,
  };
  return <button style={base} onClick={onClick} disabled={disabled}>{children}</button>;
}

const card = {
  border: "1px solid #e5e7eb",
  borderRadius: 12,
  padding: 14,
  background: "#fff",
  marginTop: 10,
};

const pill = {
  display: "inline-block",
  padding: "3px 8px",
  borderRadius: 999,
  background: "#f3f4f6",
  fontSize: 12,
  marginRight: 6,
  marginBottom: 6,
};

export default function NamedReactionsTab({ API_BASE = "" }) {
  const [query, setQuery] = useState("");
  const [limit, setLimit] = useState(20);
  const [status, setStatus] = useState("");
  const [summary, setSummary] = useState(null);
  const [hits, setHits] = useState([]);

  async function fetchJson(url) {
    const res = await fetch(url);
    const data = await res.json();
    if (!res.ok) throw new Error(data?.detail || JSON.stringify(data));
    return data;
  }

  async function loadSummary() {
    const data = await fetchJson(`${API_BASE}/api/named-reactions/summary`);
    setSummary(data);
  }

  async function runSearch(qOverride = null) {
    const q = (qOverride ?? query).trim();
    setStatus("검색 중...");
    try {
      const url = `${API_BASE}/api/named-reactions/search?q=${encodeURIComponent(q)}&limit=${limit}&offset=0`;
      const data = await fetchJson(url);
      setHits(data?.hits || []);
      setStatus(`결과 ${data?.hits?.length || 0}개`);
    } catch (e) {
      setStatus(`검색 실패: ${String(e)}`);
    }
  }

  useEffect(() => {
    loadSummary().catch(() => {});
  }, []);

  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1.2fr", gap: 16 }}>
      <div style={{ border: "1px solid #e5e7eb", borderRadius: 14, padding: 16, background: "#fff" }}>
        <div style={{ fontSize: 18, fontWeight: 800 }}>Named Reactions 검색</div>
        <div style={{ marginTop: 10, fontSize: 12, opacity: 0.8 }}>
          이번 단계는 named reactions.zip 결과만 evidence layer로 검색 테스트합니다.
        </div>
        <div style={{ marginTop: 12, display: "flex", gap: 8, flexWrap: "wrap" }}>
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="예: Aldol Reaction, Claisen, Grubbs catalyst..."
            style={{ flex: 1, minWidth: 220, padding: "10px 12px", borderRadius: 10, border: "1px solid #d1d5db" }}
          />
          <input
            type="number"
            min={1}
            max={100}
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value || 20))}
            style={{ width: 88, padding: "10px 12px", borderRadius: 10, border: "1px solid #d1d5db" }}
          />
        </div>
        <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
          <Button variant="primary" onClick={() => runSearch()}>검색</Button>
          <Button onClick={() => { setQuery(""); runSearch(""); }}>최신순 보기</Button>
          <Button onClick={() => loadSummary()}>요약 새로고침</Button>
        </div>
        <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
          {['Aldol Reaction', 'Claisen', 'Buchwald-Hartwig', 'Clemmensen Reduction'].map((q) => (
            <button key={q} style={{ ...pill, cursor: 'pointer', border: 'none' }} onClick={() => { setQuery(q); runSearch(q); }}>{q}</button>
          ))}
        </div>
        {status ? <div style={{ marginTop: 12, padding: 10, borderRadius: 10, background: '#f9fafb', fontSize: 13 }}>{status}</div> : null}
        <div style={{ marginTop: 14, padding: 12, borderRadius: 12, background: '#f9fafb', fontSize: 13 }}>
          <div style={{ fontWeight: 800, marginBottom: 8 }}>Summary</div>
          <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{summary ? JSON.stringify(summary, null, 2) : '(아직 로딩 안 함)'}</pre>
        </div>
      </div>
      <div style={{ border: "1px solid #e5e7eb", borderRadius: 14, padding: 16, background: "#fff" }}>
        <div style={{ fontSize: 18, fontWeight: 800 }}>검색 결과</div>
        {hits.length ? hits.map((h, idx) => (
          <div key={h.extract_id || idx} style={card}>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
              <div style={{ fontWeight: 800 }}>{h.reaction_family_name || h.reaction_family_name_norm || '(family 없음)'}</div>
              <div style={{ fontSize: 12, opacity: 0.75 }}>{h.source_zip} · p{h.page_no} · scheme {h.scheme_index}</div>
            </div>
            <div style={{ marginTop: 8 }}>
              <span style={pill}>{h.extract_kind || 'unknown'}</span>
              <span style={pill}>{h.section_type || 'section?'}</span>
              <span style={pill}>{h.scheme_role || 'role?'}</span>
              {h.page_kind ? <span style={pill}>{h.page_kind}</span> : null}
            </div>
            {h.transformation_text ? <div style={{ marginTop: 8, fontWeight: 700 }}>{h.transformation_text}</div> : null}
            {h.reactants_text ? <div style={{ marginTop: 8, fontSize: 13 }}><b>Reactants:</b> {h.reactants_text}</div> : null}
            {h.products_text ? <div style={{ marginTop: 6, fontSize: 13 }}><b>Products:</b> {h.products_text}</div> : null}
            {h.reagents_text ? <div style={{ marginTop: 6, fontSize: 13 }}><b>Reagents:</b> {h.reagents_text}</div> : null}
            {h.conditions_text ? <div style={{ marginTop: 6, fontSize: 13 }}><b>Conditions:</b> {h.conditions_text}</div> : null}
            {h.yield_text ? <div style={{ marginTop: 6, fontSize: 13 }}><b>Yield:</b> {h.yield_text}</div> : null}
            {h.notes_text ? <div style={{ marginTop: 6, fontSize: 12, opacity: 0.8, whiteSpace: 'pre-wrap' }}>{h.notes_text}</div> : null}
          </div>
        )) : <div style={{ marginTop: 14, opacity: 0.75 }}>아직 결과가 없습니다. 검색어를 넣거나 최신순 보기를 눌러주세요.</div>}
      </div>
    </div>
  );
}
