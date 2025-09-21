import mysql.connector
from mysql.connector import Error

def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',          # Thay đổi nếu MySQL ở server khác
            port=3306,                 # Port mặc định của MySQL
            database='roomiego',  # Tên database của bạn
            user='root',      # Username MySQL
            password='root'   # Password MySQL
        )
        return connection
    except Error as e:
        print(f"Lỗi kết nối MySQL: {e}")
        return None