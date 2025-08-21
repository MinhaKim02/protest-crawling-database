from fastapi import FastAPI, Request
import pandas as pd
import datetime
import os
import ast
import glob

app = FastAPI()

DATA_DIR = "data"  # 크롤러 저장 경로

@app.post("/today-protests")
async def today_protests(request: Request):
    body = await request.json()  # 카카오 요청 body (사용 안 해도 됨)

    # 오늘 날짜 파일명
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
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

@app.get("/")
def home():
    return {"message": "✅ protest-crawling-database API is running!"}
