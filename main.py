from fastapi import FastAPI, Request
import pandas as pd
import datetime
import os
import ast
import glob
import pytz

app = FastAPI()

DATA_DIR = "data"  # 크롤러 저장 경로

@app.post("/today-protests")
async def today_protests(request: Request):
    body = await request.json()  # 카카오 요청 body (사용 안 해도 됨)

    # 오늘 날짜 파일명
    KST = pytz.timezone("Asia/Seoul")
    today_str = datetime.datetime.now(KST).strftime("%Y-%m-%d")
    file_name = f"집회_정보_{today_str}.csv"
    file_path = os.path.join(DATA_DIR, file_name)

    # 오늘 파일 없으면 가장 최신 CSV 찾기
    if not os.path.exists(file_path):
        print("❌ 오늘 파일 없음:", file_path)  # 🔹 로그 추가
        csv_files = glob.glob(os.path.join(DATA_DIR, "집회_정보_*.csv"))
        print("📂 data 폴더 안 CSV 파일 목록:", csv_files)  # 🔹 로그 추가
        if not csv_files:  # 아예 CSV가 없는 경우
            return {
                "version": "2.0",
                "template": {
                    "outputs": [
                        {"simpleText": {"text": "📢 등록된 집회 데이터가 없습니다."}}
                    ]
                }
            }
        # 가장 최신 파일 선택
        file_path = max(csv_files, key=os.path.getctime)
        today_str = os.path.basename(file_path).replace("집회_정보_", "").replace(".csv", "")
        print("✅ 대체 사용된 최신 파일:", file_path)  # 🔹 로그 추가

    # CSV 읽기
    df = pd.read_csv(file_path)
    total_count = len(df)

    # 메시지 만들기
    text = f"📢 {today_str} 종로구 집회 정보\n"
    text += f"총 {total_count}건의 집회가 예정되어 있습니다.\n\n"

    for _, row in df.iterrows():
        start = row.get("start_time", "")
        end = row.get("end_time", "")

        # 장소 처리
        locations = row.get("장소", "")
        if isinstance(locations, str) and locations.startswith("["):
            try:
                loc_list = ast.literal_eval(locations)
                locations = " - ".join(loc_list)
            except Exception:
                pass

        # 인원 처리 (없으면 생략)
        people = row.get("인원", "")
        people_text = f"\n👥 약 {people}명" if pd.notna(people) and str(people).strip() else ""

        text += f"🕒 {start}~{end}\n📍 {locations}{people_text}\n\n"

    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {"simpleText": {"text": text.strip()}}
            ]
        }
    }

# 📌 새로 추가: 오늘 + 내일 집회 정보
@app.post("/upcoming-protests")
async def upcoming_protests(request: Request):
    body = await request.json()

    KST = pytz.timezone("Asia/Seoul")
    today = datetime.datetime.now(KST).date()
    tomorrow = today + datetime.timedelta(days=1)

    # 최신 CSV 찾기
    csv_files = glob.glob(os.path.join(DATA_DIR, "집회_정보_*.csv"))
    if not csv_files:
        return {
            "version": "2.0",
            "template": {"outputs": [{"simpleText": {"text": "📢 등록된 집회 데이터가 없습니다."}}]}
        }
    file_path = max(csv_files, key=os.path.getctime)

    # CSV 읽고 날짜 컬럼 처리
    df = pd.read_csv(file_path)
    if {"년", "월", "일"}.issubset(df.columns):
        df["날짜"] = pd.to_datetime(df[["년", "월", "일"]].astype(str).agg("-".join, axis=1))
    else:
        return {
            "version": "2.0",
            "template": {"outputs": [{"simpleText": {"text": "❌ CSV에 날짜 컬럼이 없습니다."}}]}
        }

    # 오늘+내일 필터링
    df_filtered = df[df["날짜"].dt.date.isin([today, tomorrow])]
    if df_filtered.empty:
        return {
            "version": "2.0",
            "template": {"outputs": [{"simpleText": {"text": "📢 오늘과 내일 예정된 집회가 없습니다."}}]}
        }

    # 날짜별 그룹핑
    grouped = df_filtered.groupby(df_filtered["날짜"].dt.date)

    text = f"📢 오늘({today})과 내일({tomorrow})의 종로구 집회 정보\n"
    text += f"총 {len(df_filtered)}건의 집회가 예정되어 있습니다.\n\n"

    for date, rows in grouped:
        text += f"📅 {date}\n\n"
        for _, row in rows.iterrows():
            start = row.get("start_time", "")
            end = row.get("end_time", "")

            # 장소 처리
            locations = row.get("장소", "")
            if isinstance(locations, str) and locations.startswith("["):
                try:
                    loc_list = ast.literal_eval(locations)
                    locations = " - ".join(loc_list)
                except Exception:
                    pass

            # 인원 처리
            people = row.get("인원", "")
            people_text = f"👥 약 {people}명" if pd.notna(people) and str(people).strip() else ""

            # 출력 순서 📍 → 🕒 → 👥
            text += f"📍 {locations}\n🕒 {start}~{end}\n{people_text}\n\n"

        # 날짜 구분선 (〰️)
        text += "〰️〰️〰️〰️〰️〰️〰️〰️〰️〰️\n\n"


@app.get("/")
def home():
    return {"message": "✅ protest-crawling-database API is running!"}
