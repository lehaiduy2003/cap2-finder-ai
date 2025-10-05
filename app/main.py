from fastapi import FastAPI, HTTPException, Query
from typing import List
from .recommender import recommend

app = FastAPI(title="Roommate AI Recommender")

@app.get("/recommend")
def recommend_users(user_id: int = Query(...), top_n: int = Query(5)) -> List[dict]:
    result = recommend(user_id, top_n)
    if not result:
        raise HTTPException(status_code=404, detail="Không tìm thấy user hoặc không có kết quả.")
    return result