from sentence_transformers import SentenceTransformer
import numpy as np

model = SentenceTransformer('paraphrase-MiniLM-L12-v2')
embedding_cache = {}

def get_embedding(user_id: int, text: str) -> np.ndarray:
    if user_id in embedding_cache:
        return embedding_cache[user_id]
    vec = model.encode(text)
    embedding_cache[user_id] = vec
    return vec


def get_embedding_for_field(user_id: int, field: str, text: str) -> np.ndarray:
    """
    Lấy embedding cho một field cụ thể của user
    Cache key = f"{user_id}_{field}" để tránh conflict
    """
    cache_key = f"{user_id}_{field}"
    if cache_key in embedding_cache:
        return embedding_cache[cache_key]
    
    vec = model.encode(text)
    embedding_cache[cache_key] = vec
    return vec


def clear_cache():
    """Xóa cache khi cần"""
    global embedding_cache
    embedding_cache = {}