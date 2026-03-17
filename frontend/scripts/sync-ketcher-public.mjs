/**
 * NO-OP HOTFIX
 *
 * 왜 존재하나?
 * - 과거에 "predev": "node scripts/sync-ketcher-public.mjs" 같은 훅이 남아있을 때
 *   이 파일이 BOM/인코딩 문제로 SyntaxError(Invalid or unexpected token)를 내며
 *   dev 서버가 아예 못 뜨는 사례가 발생했습니다.
 *
 * 현재 권장 아키텍처(Option B):
 * - Ketcher Standalone 릴리즈(zip)를 frontend/public/ketcher/에 직접 풀어서 정적으로 서빙
 * - React는 iframe(/ketcher/index.html)로만 접근
 *
 * 즉, node_modules에서 복사/동기화하는 단계 자체가 필요 없습니다.
 *
 * 이 파일은 "남아있는 predev 훅"이 있어도 dev가 막히지 않도록 '아무것도 하지 않고' 종료합니다.
 */

console.log("[sync-ketcher-public] noop (Option B: static public/ketcher). Nothing to sync.");

export {}; // keep as ES module
