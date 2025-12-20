import pandas as pd
from sqlalchemy import text
from .db_connection import get_db_engine


# Trọng số ưu tiên theo thứ tự (tổng = 100)
WEIGHTS = {
    'hometown': 20,      # Ưu tiên 1: Quê quán - EXACT MATCH
    'yob': 18,           # Ưu tiên 2: Năm sinh - RANGE MATCH (±5 năm)
    'job': 16,           # Ưu tiên 3: Nghề nghiệp - PARTIAL MATCH
    'city': 14,          # Ưu tiên 4: Thành phố - EXACT MATCH
    'district': 12,      # Ưu tiên 5: Quận/Huyện - EXACT MATCH
    'hobbies': 10,       # Ưu tiên 6: Sở thích - PARTIAL MATCH
    'rate_image': 5,     # Ưu tiên 7: Mức quan tâm hình ảnh - RANGE MATCH (±1)
    'more': 5,           # Ưu tiên 8: Mô tả thêm - PARTIAL MATCH
}


def normalize_string(s):
    """Chuẩn hóa string để so sánh: lowercase, strip, remove extra spaces"""
    if s is None:
        return ''
    return str(s).strip().lower()


def exact_match(val1, val2):
    """So sánh chính xác 2 giá trị (sau khi chuẩn hóa)"""
    v1 = normalize_string(val1)
    v2 = normalize_string(val2)
    if not v1 or not v2:
        return 0.0
    return 1.0 if v1 == v2 else 0.0


def range_match_yob(yob1, yob2, max_diff=5):
    """So sánh năm sinh với khoảng chênh lệch cho phép"""
    try:
        y1 = int(yob1) if yob1 else 0
        y2 = int(yob2) if yob2 else 0
        if y1 == 0 or y2 == 0:
            return 0.0
        diff = abs(y1 - y2)
        if diff == 0:
            return 1.0
        elif diff <= max_diff:
            # Điểm giảm dần theo khoảng cách
            return 1.0 - (diff / (max_diff + 1))
        return 0.0
    except (ValueError, TypeError):
        return 0.0


def range_match_rate(rate1, rate2, max_diff=1):
    """So sánh rate_image với khoảng chênh lệch cho phép"""
    try:
        r1 = int(rate1) if rate1 else 0
        r2 = int(rate2) if rate2 else 0
        if r1 == 0 or r2 == 0:
            return 0.0
        diff = abs(r1 - r2)
        if diff == 0:
            return 1.0
        elif diff <= max_diff:
            return 0.5
        return 0.0
    except (ValueError, TypeError):
        return 0.0


def partial_match(val1, val2):
    """So sánh partial - kiểm tra có chứa từ khóa chung không"""
    v1 = normalize_string(val1)
    v2 = normalize_string(val2)
    if not v1 or not v2:
        return 0.0
    
    # Exact match
    if v1 == v2:
        return 1.0
    
    # Partial match - tách từ và so sánh
    words1 = set(v1.split())
    words2 = set(v2.split())
    
    if not words1 or not words2:
        return 0.0
    
    # Tính số từ chung
    common_words = words1 & words2
    if common_words:
        # Điểm dựa trên tỷ lệ từ chung
        return len(common_words) / max(len(words1), len(words2))
    
    # Kiểm tra substring
    if v1 in v2 or v2 in v1:
        return 0.5
    
    return 0.0


def calculate_rule_based_score(user_row, candidate_row):
    """
    Tính điểm RULE-BASED cho candidate so với user
    - EXACT MATCH: hometown, city, district
    - RANGE MATCH: yob (±5 năm), rate_image (±1)
    - PARTIAL MATCH: job, hobbies, more
    """
    total_score = 0.0
    field_scores = {}
    
    # 1. HOMETOWN - EXACT MATCH (weight: 20)
    hometown_score = exact_match(user_row.get('hometown'), candidate_row.get('hometown'))
    field_scores['hometown'] = round(hometown_score * WEIGHTS['hometown'], 2)
    total_score += field_scores['hometown']
    
    # 2. YOB - RANGE MATCH ±5 năm (weight: 18)
    yob_score = range_match_yob(user_row.get('yob'), candidate_row.get('yob'), max_diff=5)
    field_scores['yob'] = round(yob_score * WEIGHTS['yob'], 2)
    total_score += field_scores['yob']
    
    # 3. JOB - PARTIAL MATCH (weight: 16)
    job_score = partial_match(user_row.get('job'), candidate_row.get('job'))
    field_scores['job'] = round(job_score * WEIGHTS['job'], 2)
    total_score += field_scores['job']
    
    # 4. CITY - EXACT MATCH (weight: 14)
    city_score = exact_match(user_row.get('city'), candidate_row.get('city'))
    field_scores['city'] = round(city_score * WEIGHTS['city'], 2)
    total_score += field_scores['city']
    
    # 5. DISTRICT - EXACT MATCH (weight: 12)
    district_score = exact_match(user_row.get('district'), candidate_row.get('district'))
    field_scores['district'] = round(district_score * WEIGHTS['district'], 2)
    total_score += field_scores['district']
    
    # 6. HOBBIES - PARTIAL MATCH (weight: 10)
    hobbies_score = partial_match(user_row.get('hobbies'), candidate_row.get('hobbies'))
    field_scores['hobbies'] = round(hobbies_score * WEIGHTS['hobbies'], 2)
    total_score += field_scores['hobbies']
    
    # 7. RATE_IMAGE - RANGE MATCH ±1 (weight: 5)
    rate_score = range_match_rate(user_row.get('rate_image'), candidate_row.get('rate_image'), max_diff=1)
    field_scores['rate_image'] = round(rate_score * WEIGHTS['rate_image'], 2)
    total_score += field_scores['rate_image']
    
    # 8. MORE - PARTIAL MATCH (weight: 5)
    more_score = partial_match(user_row.get('more'), candidate_row.get('more'))
    field_scores['more'] = round(more_score * WEIGHTS['more'], 2)
    total_score += field_scores['more']
    
    return round(total_score, 2), field_scores


def get_user_gender(user_id: int):
    """Lấy gender của user từ database"""
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
    Nếu có gender, chỉ lấy những người cùng gender
    """
    engine = get_db_engine()
    if engine is None:
        return pd.DataFrame()

    if gender:
        if include_user_id:
            query = text("SELECT * FROM roommates WHERE gender = :gender OR user_id = :user_id")
            df = pd.read_sql(query, con=engine, params={"gender": gender, "user_id": include_user_id})
        else:
            query = text("SELECT * FROM roommates WHERE gender = :gender")
            df = pd.read_sql(query, con=engine, params={"gender": gender})
    else:
        query = "SELECT * FROM roommates"
        df = pd.read_sql(query, con=engine)
    
    return df


def recommend(user_id: int, top_n: int = 5):
    """
    Gợi ý roommate dựa trên RULE-BASED matching
    
    Thuật toán:
    1. Lấy gender của user
    2. Filter tất cả users cùng gender
    3. Tính điểm theo trọng số ưu tiên:
       - hometown (20): EXACT MATCH
       - yob (18): RANGE MATCH ±5 năm
       - job (16): PARTIAL MATCH
       - city (14): EXACT MATCH
       - district (12): EXACT MATCH
       - hobbies (10): PARTIAL MATCH
       - rate_image (5): RANGE MATCH ±1
       - more (5): PARTIAL MATCH
    4. Sắp xếp theo tổng điểm giảm dần
    5. Trả về top N kết quả
    """
    # Bước 1: Lấy gender của user
    gender = get_user_gender(user_id)
    if gender is None:
        return []

    # Bước 2: Lấy tất cả roommates cùng gender
    roommates = get_roommates(gender=gender, include_user_id=user_id)
    if roommates.empty:
        return []

    # Tìm user row
    user_rows = roommates[roommates['user_id'] == user_id]
    if user_rows.empty:
        return []
    
    user_row = user_rows.iloc[0].to_dict()

    # Lọc candidates (loại bỏ chính user đó)
    candidates_df = roommates[roommates['user_id'] != user_id]
    
    if candidates_df.empty:
        return []

    # Bước 3: Tính điểm RULE-BASED cho từng candidate
    scored_candidates = []
    
    for idx, candidate_row in candidates_df.iterrows():
        candidate_dict = candidate_row.to_dict()
        candidate_id = candidate_dict['user_id']
        
        # Tính điểm rule-based
        total_score, field_scores = calculate_rule_based_score(user_row, candidate_dict)
        
        scored_candidates.append({
            'idx': idx,
            'user_id': candidate_id,
            'total_score': total_score,
            'field_scores': field_scores,
            'row': candidate_dict
        })
    
    # Bước 4: Sắp xếp theo tổng điểm giảm dần
    scored_candidates.sort(key=lambda x: x['total_score'], reverse=True)
    
    # Bước 5: Lấy top N
    top_candidates = scored_candidates[:top_n]
    
    # Trả về kết quả với điểm số chi tiết
    results = []
    for candidate in top_candidates:
        row = candidate['row']
        result = {
            'user_id': int(row['user_id']),
            'gender': str(row['gender']),
            'hometown': str(row['hometown']),
            'city': str(row['city']),
            'district': str(row['district']),
            'yob': int(row['yob']) if row.get('yob') else None,
            'hobbies': str(row['hobbies']),
            'job': str(row['job']),
            'more': str(row['more']),
            'rate_image': int(row['rate_image']) if row.get('rate_image') else None,
            'match_score': candidate['total_score'],
            'score_details': candidate['field_scores']
        }
        results.append(result)
    
    return results
