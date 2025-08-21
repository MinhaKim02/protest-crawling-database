import ast
from fastapi import FastAPI
import pandas as pd
from datetime import datetime

app = FastAPI()

@app.get("/today-protests")
def today_protests():
    today = datetime.now().strftime("%Y-%m-%d")
    url = f"https://raw.githubusercontent.com/MinhaKim02/protest-crawling-database/main/data/집회_정보_{today}.csv"

    try:
        df = pd.read_csv(url)
    except Exception:
        return {"message": f"📢 오늘({today})은 예정된 집회가 없습니다."}

    if df.empty:
        return {"message": f"📢 오늘({today})은 예정된 집회가 없습니다."}

    protests = []
    for _, row in df.iterrows():
        # 장소 컬럼 처리
        try:
            places = ast.literal_eval(row['장소'])
            if isinstance(places, list):
                place_text = " - ".join(places)
            else:
                place_text = str(places)
        except:
            place_text = str(row['장소'])

        # 혼잡도 아이콘
        people = int(row['인원'])
        if people > 1000:
            congestion = "🔴 매우 혼잡"
        elif people > 500:
            congestion = "🟡 혼잡"
        else:
            congestion = "🟢 원활"

        protest_info = (
            f"⏰ {row['start_time']} ~ {row['end_time']}\n"
            f"📍 장소: {place_text}\n"
            f"👥 예상 인원: {people}명 ({congestion})"
        )
        protests.append(protest_info)

    response_text = f"📅 오늘({today}) 종로구 집회 정보\n\n" + "\n\n".join(protests)

    return {"message": response_text}
