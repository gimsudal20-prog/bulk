# 네이버 검색광고 마스터 툴

기능:
- 캠페인 대량 복사
- 광고그룹 통째 복사
- 캠페인/광고그룹 예산 일괄 변경
- 캠페인/광고그룹 스케줄 일괄 변경
- 키워드 입찰가 일괄 변경
- 캠페인 / 광고그룹 / 키워드 / 소재 / 확장소재 / 제외키워드 대량 등록
- 캠페인 / 광고그룹 / 키워드 / 소재 / 확장소재 / 제외키워드 대량 삭제
- 삭제 전 2차 경고창 + `DELETE` 직접 입력 확인

## 실행

```powershell
cd C:\Users\User\Downloads\naver_bulk_manager
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

브라우저:
- http://127.0.0.1:5000

## 필수 파일 구조

```text
naver_bulk_manager/
  app.py
  requirements.txt
  accounts.csv
  templates/
    index.html
  samples/
    *.csv
```

## 대량 삭제 권장 형식

### 캠페인 삭제
```csv
nccCampaignId
cmp-a001
```

### 광고그룹 삭제
```csv
nccAdgroupId
grp-a001
```

### 키워드 삭제
```csv
nccKeywordId,nccAdgroupId,keyword
nkw-a001,grp-a001,브랜드키워드
```

## 참고
- 키워드 삭제는 `nccKeywordId`가 가장 안전합니다.
- 제외키워드 삭제는 계정 상태/API 응답 형식에 따라 일부 실패할 수 있습니다.
- 삭제는 복구가 어렵기 때문에 먼저 소량 테스트 후 전체 실행을 권장합니다.
