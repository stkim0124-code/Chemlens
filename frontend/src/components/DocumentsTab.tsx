// frontend/src/components/DocumentsTab.tsx
import React, { useEffect, useMemo, useState } from "react";
import { searchDocs, getDocPage, DocSearchHit } from "../api/docs";

function stripSnippet(snippet: string) {
  // 백엔드 snippet()이 [] 하이라이트를 넣습니다.
  // UI에서는 <mark>로 바꾸기
  return snippet
    .replaceAll("[", "<mark>")
    .replaceAll("]", "</mark>");
}

export default function DocumentsTab() {
  const [q, setQ] = useState("");
  const [loading, setLoading] = useState(false);
  const [hits, setHits] = useState<DocSearchHit[]>([]);
  const [error, setError] = useState<string | null>(null);

  const [open, setOpen] = useState(false);
  const [pageText, setPageText] = useState<string>("");
  const [pageTitle, setPageTitle] = useState<string>("");

  const canSearch = useMemo(() => q.trim().length > 0, [q]);

  async function onSearch() {
    if (!canSearch) return;
    setLoading(true);
    setError(null);
    try {
      const res = await searchDocs(q.trim(), 30);
      setHits(res.hits);
    } catch (e: any) {
      setError(e?.message ?? "검색 실패");
    } finally {
      setLoading(false);
    }
  }

  async function openHit(hit: DocSearchHit) {
    // page_from을 대표 페이지로 열기
    try {
      const p = await getDocPage(hit.doc_id, hit.page_from);
      setPageTitle(`${hit.doc_title} (p.${hit.page_from})`);
      setPageText(p.text || "");
      setOpen(true);
    } catch (e: any) {
      setError(e?.message ?? "페이지 로드 실패");
    }
  }

  return (
    <div style={{ padding: 16, display: "grid", gap: 12 }}>
      <div style={{ display: "flex", gap: 8 }}>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="예: aldol, Suzuki, protecting group, enolate..."
          style={{ flex: 1, padding: 10 }}
          onKeyDown={(e) => {
            if (e.key === "Enter") onSearch();
          }}
        />
        <button onClick={onSearch} disabled={!canSearch || loading}>
          {loading ? "검색중..." : "검색"}
        </button>
      </div>

      {error && (
        <div style={{ padding: 12, background: "#ffecec", border: "1px solid #ffb3b3" }}>
          {error}
        </div>
      )}

      <div style={{ display: "grid", gap: 10 }}>
        {hits.map((hit) => (
          <div
            key={hit.chunk_id}
            style={{
              border: "1px solid #ddd",
              borderRadius: 10,
              padding: 12,
              cursor: "pointer",
              background: "#fff",
            }}
            onClick={() => openHit(hit)}
          >
            <div style={{ fontWeight: 700, marginBottom: 6 }}>
              {hit.doc_title}
            </div>
            <div style={{ fontSize: 12, opacity: 0.8, marginBottom: 8 }}>
              page {hit.page_from}{hit.page_to !== hit.page_from ? `–${hit.page_to}` : ""}
            </div>
            <div
              style={{ lineHeight: 1.5 }}
              dangerouslySetInnerHTML={{ __html: stripSnippet(hit.snippet) }}
            />
          </div>
        ))}

        {!loading && hits.length === 0 && canSearch && (
          <div style={{ opacity: 0.7 }}>검색 결과가 없습니다.</div>
        )}
      </div>

      {open && (
        <div
          onClick={() => setOpen(false)}
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.35)",
            display: "grid",
            placeItems: "center",
            padding: 24,
          }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{
              width: "min(1000px, 95vw)",
              maxHeight: "85vh",
              overflow: "auto",
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              border: "1px solid #ddd",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <div style={{ fontWeight: 800 }}>{pageTitle}</div>
              <button onClick={() => setOpen(false)}>닫기</button>
            </div>
            <pre style={{ whiteSpace: "pre-wrap", lineHeight: 1.5, marginTop: 12 }}>
              {pageText}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}