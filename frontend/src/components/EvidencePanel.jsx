import React from "react";

const chipBase = {
  display: "inline-block",
  padding: "3px 8px",
  borderRadius: 999,
  fontSize: 11,
  fontWeight: 700,
  border: "1px solid transparent",
  lineHeight: 1.2,
};

const chipStyle = {
  ...chipBase,
  background: "#eef3ff",
  color: "#1f4db8",
  borderColor: "#d7e3ff",
};

const mutedChipStyle = {
  ...chipBase,
  background: "#f6f7f9",
  color: "#576074",
  borderColor: "#e5e9f0",
};

function confidenceChip(confidence) {
  if (confidence === "높음") {
    return { ...chipBase, background: "#edf9f0", color: "#196c2e", borderColor: "#ccebd5" };
  }
  if (confidence === "중간") {
    return { ...chipBase, background: "#fff7e8", color: "#8a5a00", borderColor: "#f2dfb3" };
  }
  return { ...chipBase, background: "#f8f2ff", color: "#6b3fa0", borderColor: "#e5d7fb" };
}

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

function RowLabel({ label, value, strong = false }) {
  if (!value) return null;
  return (
    <div style={{ display: "grid", gridTemplateColumns: "74px 1fr", gap: 8, fontSize: 12, lineHeight: 1.5, marginTop: 4 }}>
      <div style={{ color: "#556074", fontWeight: 700 }}>{label}</div>
      <div style={{ color: "#1b2430", fontWeight: strong ? 700 : 500 }}>{value}</div>
    </div>
  );
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
          results.map((item, idx) => {
            const displayName = item.display_name_ko
              ? `${item.display_name_en} (${item.display_name_ko})`
              : (item.display_name_en || item.reaction_family_name || "(family 없음)");
            const yieldMeta = item.yield_summary ? `수율 ${item.yield_summary}` : null;
            const pageMeta = item.source_page || (item.page_no ? `p.${item.page_no}` : null);
            const rightMeta = [pageMeta, yieldMeta].filter(Boolean).join(" · ");
            return (
              <div
                key={`${item.extract_id}-${idx}`}
                style={{
                  border: "1px solid #ececec",
                  borderRadius: 12,
                  padding: 14,
                  marginBottom: 12,
                  background: "#fff",
                  boxShadow: "0 1px 2px rgba(15,23,42,0.03)",
                }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start" }}>
                  <div style={{ minWidth: 0, flex: 1 }}>
                    <div style={{ fontWeight: 800, fontSize: 15, color: "#111827", lineHeight: 1.35 }}>{displayName}</div>
                    <div style={{ marginTop: 6, display: "flex", gap: 6, flexWrap: "wrap" }}>
                      <span style={chipStyle}>{item.reaction_class_ko || "기타"}</span>
                      <span style={confidenceChip(item.confidence_label)}>{item.confidence_label || "참고용"}</span>
                      <span style={mutedChipStyle}>{matchLabel(item.match_type)}</span>
                      <span style={mutedChipStyle}>{item.extract_kind || "unknown"}</span>
                    </div>
                  </div>
                  <div style={{ textAlign: "right", flexShrink: 0 }}>
                    <div style={{ ...chipStyle, fontSize: 12 }}>score {Number(item.match_score || 0).toFixed(2)}</div>
                    {rightMeta ? <div style={{ marginTop: 6, fontSize: 11, color: "#6b7280", fontWeight: 700 }}>{rightMeta}</div> : null}
                  </div>
                </div>

                <div style={{ marginTop: 10, padding: 12, borderRadius: 10, background: "#f8fafc", border: "1px solid #ebeff5" }}>
                  <div style={{ fontSize: 12, fontWeight: 800, color: "#334155", marginBottom: 2 }}>실험 요약</div>
                  <RowLabel label="대표 변화" value={item.key_change_summary} strong />
                  <RowLabel label="핵심 시약" value={item.key_reagents_summary} />
                  <RowLabel label="핵심 조건" value={item.key_conditions_summary} />
                  <RowLabel label="기질 힌트" value={item.substrate_scope_hint} />
                  <RowLabel label="생성물 힌트" value={item.product_type_hint} />
                </div>

                <details style={{ marginTop: 10 }}>
                  <summary style={{ cursor: "pointer", fontSize: 12, fontWeight: 800, color: "#334155" }}>상세 evidence 보기</summary>
                  <div style={{ marginTop: 10, padding: 10, borderRadius: 8, background: "#fcfcfd", border: "1px solid #eef2f7" }}>
                    {Array.isArray(item.matched_components) && item.matched_components.length ? (
                      <div style={{ marginBottom: 10 }}>
                        <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 4 }}>Matched components</div>
                        {item.matched_components.map((mc, mIdx) => (
                          <div key={mIdx} style={{ fontSize: 12, lineHeight: 1.45, marginTop: mIdx ? 4 : 0 }}>
                            <strong>{mc.query_role || "query"}</strong> {mc.query_smiles || "?"} → <strong>{mc.matched_role || "indexed"}</strong> {mc.matched_smiles || "?"} ({matchLabel(mc.match_type)}, {Number(mc.match_score || 0).toFixed(2)})
                          </div>
                        ))}
                      </div>
                    ) : null}
                    <RowLabel label="반응 유형" value={item.transformation_text} />
                    <RowLabel label="출발물질" value={item.reactants_text} />
                    <RowLabel label="생성물" value={item.products_text} />
                    <RowLabel label="시약" value={item.reagents_text} />
                    <RowLabel label="조건" value={item.conditions_text} />
                    <RowLabel label="온도" value={item.temperature_text} />
                    <RowLabel label="시간" value={item.time_text} />
                    <RowLabel label="수율" value={item.yield_text} />
                    <RowLabel label="출처" value={`${item.source_zip || "?"} / ${item.image_filename || "?"} / extract #${item.extract_id}`} />
                  </div>
                </details>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
