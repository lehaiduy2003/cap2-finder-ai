from fastapi import FastAPI, HTTPException, Query
from typing import List
from .recommender import recommend, get_user_gender, get_roommates
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Roommate AI Recommender")

origins = [
    "https://cap2-fe.vercel.app",
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/recommend")
def recommend_users(user_id: int = Query(...), top_n: int = Query(5)) -> List[dict]:
    result = recommend(user_id, top_n)
    if not result:
        raise HTTPException(status_code=404, detail="Không tìm thấy user hoặc không có kết quả.")
    return result


@app.get("/debug/user/{user_id}")
def debug_user(user_id: int):
    """Debug endpoint để kiểm tra user có tồn tại trong roommates không"""
    gender = get_user_gender(user_id)
    if gender is None:
        return {
            "exists": False,
            "message": f"User {user_id} không tồn tại trong bảng roommates. Hãy tạo roommate profile trước."
        }
    
    roommates = get_roommates(gender=gender, include_user_id=user_id)
    user_data = roommates[roommates['user_id'] == user_id]
    
    if user_data.empty:
        return {
            "exists": False,
            "message": f"User {user_id} không tìm thấy trong dữ liệu."
        }
    
    candidates_count = len(roommates[roommates['user_id'] != user_id])
    
    return {
        "exists": True,
        "user_id": user_id,
        "gender": gender,
        "candidates_same_gender": candidates_count,
        "user_data": user_data.iloc[0].to_dict()
    }


@app.get("/debug/db")
def debug_db():
    """Debug endpoint để kiểm tra kết nối database và dữ liệu"""
    from .db_connection import get_db_engine
    from sqlalchemy import text
    import pandas as pd
    
    engine = get_db_engine()
    if engine is None:
        return {"error": "Không thể kết nối database"}
    
    try:
        # Lấy tất cả user_id trong roommates
        query = text("SELECT id, user_id, gender, hometown, city FROM roommates LIMIT 10")
        df = pd.read_sql(query, con=engine)
        
        # Kiểm tra user_id = 159
        query159 = text("SELECT * FROM roommates WHERE user_id = 159")
        df159 = pd.read_sql(query159, con=engine)
        
        return {
            "connected": True,
            "total_records": len(df),
            "sample_data": df.to_dict(orient='records'),
            "user_159_exists": len(df159) > 0,
            "user_159_data": df159.to_dict(orient='records') if not df159.empty else None
        }
    except Exception as e:
        return {"error": str(e)}