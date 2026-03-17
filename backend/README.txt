# docs_migration_pack

## 목적
- labint_docs.db의 `documents.file_path`에 걸린 UNIQUE 제약 때문에 `sync_docs_db.py --apply`가 실패하는 문제를 해결합니다.
- 해결 방식(방법 A): UNIQUE 제약 제거 + (성능용) 일반 INDEX 추가

## 설치 위치(권장)
C:\chemlens\backend\
└─ tools\
   ├─ migrate_docs_remove_unique.py
   └─ sync_docs_db.py

## 실행 순서(Windows)
1) 백업 + UNIQUE 제거
   conda activate chemlens
   cd /d C:\chemlens\backend
   python tools\migrate_docs_remove_unique.py --db "app\data\labint_docs.db"

2) PDF 매칭 반영(apply)
   python tools\sync_docs_db.py --db "app\data\labint_docs.db" --pdfs "app\data\pdfs" --apply

## 결과
- 이제 여러 documents row가 같은 PDF(file_path)를 공유해도 DB 충돌이 나지 않습니다.
- 문서 탭의 PDF 404/미표시 문제가 매칭 가능한 범위(현재 51개)에서 즉시 개선됩니다.
