import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from .embedding_cache import get_embedding
from .db_connection import get_db_connection


def combine_features(row):
    return ' '.join(str(row.get(col, '')) for col in [
        'city', 'district', 'gender', 'hobbies', 'hometown',
        'job', 'more', 'rate_image', 'yob'
    ])


def get_roommates():
    db = get_db_connection()
    if db is None:
        return pd.DataFrame()

    query = "SELECT * FROM roommates"
    df = pd.read_sql(query, con=db)
    db.close()
    df['combined'] = df.apply(combine_features, axis=1)
    return df


def recommend(user_id: int, top_n: int = 5):
    roommates = get_roommates()
    if roommates.empty or user_id not in roommates['user_id'].values:
        return []

    idx = roommates.index[roommates['user_id'] == user_id][0]
    gender = roommates.loc[idx, 'gender']

    embeddings = np.array([
        get_embedding(row['user_id'], row['combined'])
        for _, row in roommates.iterrows()
    ])

    user_vec = embeddings[idx].reshape(1, -1)

    mask = (roommates['gender'] == gender) & (roommates['user_id'] != user_id)
    candidates = roommates[mask].index.tolist()
    if not candidates:
        candidates = roommates[roommates['user_id'] != user_id].index.tolist()

    scores = cosine_similarity(user_vec, embeddings[candidates])[0]
    scored = list(zip(candidates, scores))
    top = sorted(scored, key=lambda x: x[1], reverse=True)[:top_n]
    top_indices = [i for i, _ in top]

    return roommates.iloc[top_indices][[
        'user_id', 'gender', 'hometown', 'city', 'district', 'yob',
        'hobbies', 'job', 'more', 'rate_image'
    ]].to_dict(orient='records')