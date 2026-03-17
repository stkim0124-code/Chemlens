// frontend/src/api/docs.ts
export type DocMeta = {
  id: number;
  title: string;
  file_path: string;
  page_count: number;
  created_at?: string | null;
  thumb_url?: string | null;
};

export type DocSearchHit = {
  chunk_id: number;
  doc_id: number;
  doc_title: string;
  page_from: number;
  page_to: number;
  snippet: string;
};

export type DocSearchResponse = {
  query: string;
  total: number;
  hits: DocSearchHit[];
};

const API_BASE = import.meta.env.VITE_API_BASE ?? ""; // 예: http://localhost:8000

export async function searchDocs(q: string, limit = 20): Promise<DocSearchResponse> {
  const url = `${API_BASE}/api/docs/search?q=${encodeURIComponent(q)}&limit=${limit}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`searchDocs failed: ${res.status}`);
  return res.json();
}

export async function getDoc(docId: number): Promise<DocMeta> {
  const url = `${API_BASE}/api/docs/${docId}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`getDoc failed: ${res.status}`);
  return res.json();
}

export async function getDocPage(docId: number, pageNo: number): Promise<{ doc_id:number; page_no:number; text:string }> {
  const url = `${API_BASE}/api/docs/${docId}/page/${pageNo}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`getDocPage failed: ${res.status}`);
  return res.json();
}