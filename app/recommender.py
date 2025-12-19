import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import text
from .embedding_cache import get_embedding
from .db_connection import get_db_engine


def combine_features(row):
    return ' '.join(str(row.get(col, '')) for col in [
        'city', 'district', 'gender', 'hobbies', 'hometown',
        'job', 'more', 'rate_image', 'yob'
    ])


def get_user_gender(user_id: int):
    """Lấy gender của user từ database để tối ưu query"""
    engine = get_db_engine()
    if engine is None:
        return None
    
    query = text("SELECT gender FROM roommates WHERE user_id = :user_id")
    result = pd.read_sql(query, con=engine, params={"user_id": user_id})
    if result.empty:
        return None
    return result.iloc[0]['gender']


def get_roommates(gender: str = None, include_user_id: int = None):
    """
    Lấy danh sách roommates từ database
    Nếu có gender, chỉ lấy những người cùng gender (tối ưu performance)
    Nếu có include_user_id, luôn bao gồm user đó trong kết quả
    """
    engine = get_db_engine()
    if engine is None:
        return pd.DataFrame()

    if gender:
        # Lọc theo gender ngay trong SQL để giảm dữ liệu xử lý
        if include_user_id:
            query = text("SELECT * FROM roommates WHERE gender = :gender OR user_id = :user_id")
            df = pd.read_sql(query, con=engine, params={"gender": gender, "user_id": include_user_id})
        else:
            query = text("SELECT * FROM roommates WHERE gender = :gender")
            df = pd.read_sql(query, con=engine, params={"gender": gender})
    else:
        query = "SELECT * FROM roommates"
        df = pd.read_sql(query, con=engine)
    
    if df.empty:
        return df
    
    # No need to close engine - SQLAlchemy manages connection pooling
    df['combined'] = df.apply(combine_features, axis=1)
    return df


def recommend(user_id: int, top_n: int = 5):
    # Bước 1: Lấy gender của user trước (query nhỏ, nhanh)
    gender = get_user_gender(user_id)
    if gender is None:
        return []

    # Bước 2: Chỉ lấy dữ liệu cùng gender từ DB (giảm 1 nửa dữ liệu)
    # Bao gồm cả user_id để có embedding của user để so sánh
    roommates = get_roommates(gender=gender, include_user_id=user_id)
    if roommates.empty:
        return []

    # Tìm index của user trong dataframe đã lọc
    user_rows = roommates[roommates['user_id'] == user_id]
    if user_rows.empty:
        return []
    
    idx = user_rows.index[0]

    # Bước 3: Tính embeddings chỉ cho dữ liệu đã lọc (giảm 1 nửa thời gian AI)
    embeddings = np.array([
        get_embedding(row['user_id'], row['combined'])
        for _, row in roommates.iterrows()
    ])

    user_vec = embeddings[idx].reshape(1, -1)

    # Lọc candidates (loại bỏ chính user đó)
    mask = roommates['user_id'] != user_id
    candidates = roommates[mask].index.tolist()
    
    if not candidates:
        # Fallback: nếu không có candidate cùng gender, lấy tất cả (trừ user)
        roommates_all = get_roommates()
        if roommates_all.empty:
            return []
        user_rows_all = roommates_all[roommates_all['user_id'] == user_id]
        if user_rows_all.empty:
            return []
        idx_all = user_rows_all.index[0]
        embeddings_all = np.array([
            get_embedding(row['user_id'], row['combined'])
            for _, row in roommates_all.iterrows()
        ])
        user_vec_all = embeddings_all[idx_all].reshape(1, -1)
        candidates_all = roommates_all[roommates_all['user_id'] != user_id].index.tolist()
        scores = cosine_similarity(user_vec_all, embeddings_all[candidates_all])[0]
        scored = list(zip(candidates_all, scores))
        top = sorted(scored, key=lambda x: x[1], reverse=True)[:top_n]
        top_indices = [i for i, _ in top]
        return roommates_all.iloc[top_indices][[
            'user_id', 'gender', 'hometown', 'city', 'district', 'yob',
            'hobbies', 'job', 'more', 'rate_image'
        ]].to_dict(orient='records')

    # Tính similarity chỉ với candidates đã lọc
    scores = cosine_similarity(user_vec, embeddings[candidates])[0]
    scored = list(zip(candidates, scores))
    top = sorted(scored, key=lambda x: x[1], reverse=True)[:top_n]
    top_indices = [i for i, _ in top]

    return roommates.iloc[top_indices][[
        'user_id', 'gender', 'hometown', 'city', 'district', 'yob',
        'hobbies', 'job', 'more', 'rate_image'
    ]].to_dict(orient='records')