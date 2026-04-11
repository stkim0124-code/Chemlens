import React from "react";

const chipStyle = {
  display: "inline-block",
  padding: "3px 8px",
  borderRadius: 999,
  fontSize: 11,
  fontWeight: 700,
  background: "#eef3ff",
  color: "#1f4db8",
  border: "1px solid #d7e3ff",
};

function matchLabel(matchType) {
  if (matchType === "tier1_similarity") return "직접 구조 히트";
  if (matchType === "tier2_generic") return "제네릭 구조 히트";
  if (matchType === "family_evidence") return "연결된 family evidence";
  if (matchType === "family_text") return "family 텍스트";
  return matchType || "evidence";
}

function queryModeLabel(mode) {
  if (mode === "reaction") return "반응식 모드";
  if (mode === "mixture") return "혼합물/다성분 모드";
  if (mode === "structure") return "단일 분자 모드";
  if (mode === "family") return "family 텍스트 모드";
  return mode || "검색 모드";
}

export default function EvidencePanel({ data, loading, error }) {
  if (loading) {
    return (
      <div style={{ marginTop: 16, padding: 12, border: "1px solid #ddd", borderRadius: 12, background: "#fff" }}>
        Named Reaction Evidence 불러오는 중...
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ marginTop: 16, padding: 12, border: "1px solid #f0c2c2", borderRadius: 12, background: "#fff7f7", color: "#8b1f1f" }}>
        Named Reaction Evidence 오류: {String(error)}
      </div>
    );
  }

  const results = data?.results || [];
  const queryComponents = data?.query_components || [];
  const reactionComponents = data?.reaction_components || null;

  return (
    <div style={{ marginTop: 16, border: "1px solid #ddd", borderRadius: 12, background: "#fff", overflow: "hidden" }}>
      <div style={{ padding: 12, borderBottom: "1px solid #eee", background: "#fafafa" }}>
        <div style={{ fontWeight: 800, fontSize: 16 }}>🧪 Named Reaction Evidence</div>
        <div style={{ marginTop: 6, fontSize: 12, opacity: 0.8 }}>
          구조검색 결과와 연결된 reaction family / extract / page evidence
        </div>
        {data ? (
          <>
            <div style={{ marginTop: 8, display: "flex", gap: 8, flexWrap: "wrap" }}>
              <span style={chipStyle}>{queryModeLabel(data.query_mode)}</span>
              <span style={chipStyle}>직접 {data.direct_count || 0}</span>
              <span style={chipStyle}>제네릭 {data.generic_count || 0}</span>
              <span style={chipStyle}>family {data.family_count || 0}</span>
            </div>

            {data.query_reaction_smiles ? (
              <div style={{ marginTop: 10, fontSize: 12, lineHeight: 1.45, wordBreak: "break-all" }}>
                <strong>Reaction SMILES:</strong> {data.query_reaction_smiles}
              </div>
            ) : null}

            {reactionComponents ? (
              <div style={{ marginTop: 8, fontSize: 12, lineHeight: 1.5 }}>
                <div><strong>Reactants:</strong> {(reactionComponents.reactants || []).join(" ; ") || "-"}</div>
                <div><strong>Agents:</strong> {(reactionComponents.agents || []).join(" ; ") || "-"}</div>
                <div><strong>Products:</strong> {(reactionComponents.products || []).join(" ; ") || "-"}</div>
              </div>
            ) : null}

            {!reactionComponents && queryComponents.length ? (
              <div style={{ marginTop: 8, fontSize: 12, lineHeight: 1.5 }}>
                <strong>Query components:</strong> {queryComponents.map((c) => `${c.role || "unknown"}:${c.smiles}`).join(" ; ")}
              </div>
            ) : null}
          </>
        ) : null}
      </div>

      <div style={{ padding: 12 }}>
        {!results.length ? (
          <div style={{ fontSize: 13, opacity: 0.75 }}>
            아직 연결된 evidence가 없습니다. 반응 화살표가 있다면 reactant와 product를 모두 배치해 보시고, 다른 구조 또는 유사도 컷을 시도해보세요.
          </div>
        ) : (
          results.map((item, idx) => (
            <div
              key={`${item.extract_id}-${idx}`}
              style={{
                border: "1px solid #ececec",
                borderRadius: 10,
                padding: 12,
                marginBottom: 10,
                background: "#fff",
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start" }}>
                <div>
                  <div style={{ fontWeight: 800, fontSize: 15 }}>
                    {item.reaction_family_name || "(family 없음)"}
                  </div>
                  <div style={{ marginTop: 4, fontSize: 12, opacity: 0.78 }}>
                    {matchLabel(item.match_type)} · {item.extract_kind || "unknown"} · {item.section_type || "section?"}
                  </div>
                </div>
                <div style={{ textAlign: "right", fontSize: 12 }}>
                  <div style={chipStyle}>score {Number(item.match_score || 0).toFixed(2)}</div>
                  <div style={{ marginTop: 6, opacity: 0.7 }}>
                    p.{item.page_no ?? "?"}
                  </div>
                </div>
              </div>

              {Array.isArray(item.matched_components) && item.matched_components.length ? (
                <div style={{ marginTop: 8, padding: 10, borderRadius: 8, background: "#f8fafc", border: "1px solid #ebeff5" }}>
                  <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 4 }}>Matched components</div>
                  {item.matched_components.map((mc, mIdx) => (
                    <div key={mIdx} style={{ fontSize: 12, lineHeight: 1.45, marginTop: mIdx ? 4 : 0 }}>
                      <strong>{mc.query_role || "query"}</strong> {mc.query_smiles || "?"} → <strong>{mc.matched_role || "indexed"}</strong> {mc.matched_smiles || "?"} ({matchLabel(mc.match_type)}, {Number(mc.match_score || 0).toFixed(2)})
                    </div>
                  ))}
                </div>
              ) : null}

              {item.transformation_text ? (
                <div style={{ marginTop: 10, fontSize: 13, lineHeight: 1.45 }}>
                  <strong>Transformation:</strong> {item.transformation_text}
                </div>
              ) : null}

              {item.reactants_text ? (
                <div style={{ marginTop: 8, fontSize: 13, lineHeight: 1.45 }}>
                  <strong>Reactants:</strong> {item.reactants_text}
                </div>
              ) : null}

              {item.products_text ? (
                <div style={{ marginTop: 6, fontSize: 13, lineHeight: 1.45 }}>
                  <strong>Products:</strong> {item.products_text}
                </div>
              ) : null}

              {(item.reagents_text || item.conditions_text || item.temperature_text || item.time_text || item.yield_text) ? (
                <div style={{ marginTop: 8, padding: 10, borderRadius: 8, background: "#fafcff", border: "1px solid #e8eefc" }}>
                  {item.reagents_text ? <div style={{ fontSize: 12, lineHeight: 1.45 }}><strong>Reagents:</strong> {item.reagents_text}</div> : null}
                  {item.conditions_text ? <div style={{ fontSize: 12, lineHeight: 1.45, marginTop: 4 }}><strong>Conditions:</strong> {item.conditions_text}</div> : null}
                  {(item.temperature_text || item.time_text || item.yield_text) ? (
                    <div style={{ marginTop: 4, fontSize: 12, lineHeight: 1.45 }}>
                      {item.temperature_text ? <span><strong>T:</strong> {item.temperature_text} </span> : null}
                      {item.time_text ? <span><strong>Time:</strong> {item.time_text} </span> : null}
                      {item.yield_text ? <span><strong>Yield:</strong> {item.yield_text}</span> : null}
                    </div>
                  ) : null}
                </div>
              ) : null}

              <div style={{ marginTop: 8, fontSize: 11, opacity: 0.7 }}>
                source: {item.source_zip || "?"} / {item.image_filename || "?"} / extract #{item.extract_id}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
