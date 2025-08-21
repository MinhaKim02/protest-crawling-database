from fastapi import FastAPI, Request
import pandas as pd
import datetime
import os
import ast

app = FastAPI()

DATA_DIR = "data"  # 크롤러 저장 경로

@app.post("/today-protests")
async def today_protests(request: Request):
    body = await request.json()  # 카카오 요청 body (사용 안 해도 됨)

    # 오늘 날짜 (예: 집회_정보_2025-08-22.csv)
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    file_name = f"집회_정보_{today_str}.csv"
    file_path = os.path.join(DATA_DIR, file_name)

    # CSV 없으면 안내
    if not os.path.exists(file_path):
        return {
            "version": "2.0",
            "template": {
                "outputs": [
                    {"simpleText": {"text": "📢 오늘은 등록된 집회 정보가 없습니다."}}
                ]
            }
        }

    # CSV 읽기
    df = pd.read_csv(file_path)
    total_count = len(df)

    # 메시지 만들기
    text = f"📢 오늘({today_str}) 종로구 집회 정보\n"
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

@app.get("/")
def home():
    return {"message": "✅ protest-crawling-database API is running!"}
