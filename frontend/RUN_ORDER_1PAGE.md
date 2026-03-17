# CHEMLENS Ketcher (Dependency Cut) – RUN ORDER (1 page)

## 0) 덮어쓰기
- 이 ZIP을 **frontend/** 루트에 그대로 덮어쓰기
  - 생성/갱신 파일:
    - `src/components/KetcherEditor.tsx`
    - `public/ketcher/index.html`

## 1) 캐시/서버 초기화 (필수)
1. 실행 중인 Vite 종료 (터미널에서 `Ctrl + C`)
2. Vite 캐시 삭제
   - `frontend/node_modules/.vite/` 폴더 삭제
3. 브라우저 캐시 무효화
   - DevTools(Network) → **Disable cache** 체크

## 2) 실행
```bash
npm install
npm run dev
```

## 3) 정상 동작 확인
- 브라우저에서 아래 2개 확인
  - `http://localhost:5173/ketcher/index.html` (단독 페이지)
    - 에디터 UI가 떠야 함
  - 앱 내부 구조 그리기(에디터) 영역
    - **Loading…**가 사라지고 에디터가 바로 보여야 함

## 4) 실패 시 가장 흔한 원인
- `public/ketcher/static/js/main.*.js` / `static/css/main.*.css` 파일이 없거나 경로가 틀린 경우
  - Ketcher Standalone ZIP(3.7.0)에서 **dist 내용을 public/ketcher로 통째로** 넣어야 함
