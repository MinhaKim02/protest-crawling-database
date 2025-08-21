# Protest Crawling Database

서울/종로구 집회·통제 정보 크롤링 자동화 레포지토리입니다.  
Selenium + BeautifulSoup + VWorld API를 활용하여 매일 오전 8시 자동으로 데이터를 수집합니다.

## 📂 구조
- `crawler.py` : SPATIC 사이트에서 집회 정보를 수집하고 CSV로 저장
- `data/집회_정보.csv` : 최신 집회 데이터 (GitHub Actions에서 매일 업데이트)
- `.github/workflows/crawl.yml` : GitHub Actions 스케줄러 (매일 오전 8시 실행)

## 📊 CSV 스키마
모든 필드는 문자열이며, 다중 값은 JSON 문자열 형식입니다.

| 컬럼명       | 설명                          |
|--------------|-------------------------------|
| 년           | 연도 (YYYY)                   |
| 월           | 월 (MM)                       |
| 일           | 일 (DD)                       |
| start_time   | 시작 시간 (HH:MM)             |
| end_time     | 종료 시간 (HH:MM)             |
| 장소         | 장소 리스트 (JSON 문자열)     |
| 인원         | 참가 인원 (없으면 공란)       |
| 위도         | 위도 리스트 (JSON 문자열)     |
| 경도         | 경도 리스트 (JSON 문자열)     |
| 비고         | 비고/행진경로 등 텍스트       |

## ⚙️ 실행 방법
```bash
pip install -r requirements.txt
python crawler.py —out data/집회_정보.csv
