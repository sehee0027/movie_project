import requests
import mysql.connector
from datetime import datetime, timedelta

# 영화진흥위원회 API 정보
API_KEY = "your_api_key_here"  # 발급받은 인증키
BASE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"

# MySQL 연결 정보
MYSQL_HOST = "localhost"
MYSQL_USER = "your_username"
MYSQL_PASSWORD = "your_password"
MYSQL_DATABASE = "movie_sales"

# 1년치 과거 날짜 생성
def get_past_dates(days=365):
    today = datetime.now()
    dates = [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(days)]
    return dates

# 필요한 데이터 필드
FIELDS = ["movieCd", "movieNm", "openDt", "salesAmt", "salesAcc", "audiAcc", "scrnCnt", "showCnt"]

# MySQL 연결 함수
def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

# 데이터 정제 함수
def clean_data(movie_data):
    # 불필요한 키 제거 (이미 필요한 키만 FIELDS에 정의)
    cleaned_data = {key: movie_data.get(key, None) for key in FIELDS}

    # 결측값 처리 (예: 매출액이나 상영 횟수가 없으면 0으로 처리)
    if cleaned_data["salesAmt"] is None:
        cleaned_data["salesAmt"] = 0
    if cleaned_data["salesAcc"] is None:
        cleaned_data["salesAcc"] = 0
    if cleaned_data["audiAcc"] is None:
        cleaned_data["audiAcc"] = 0
    if cleaned_data["scrnCnt"] is None:
        cleaned_data["scrnCnt"] = 0
    if cleaned_data["showCnt"] is None:
        cleaned_data["showCnt"] = 0
    
    # 날짜 포맷 변환 (YYYY-MM-DD 형식으로 변환)
    if cleaned_data["openDt"]:
        cleaned_data["openDt"] = datetime.strptime(str(cleaned_data["openDt"]), "%Y%m%d").strftime("%Y-%m-%d")
    
    return cleaned_data

# 데이터 수집 및 저장 함수
def collect_box_office_data():
    dates = get_past_dates()  # 1년치 날짜 가져오기
    all_data = []

    # DB 연결
    db_conn = get_db_connection()
    cursor = db_conn.cursor()

    for target_date in dates:
        params = {
            "key": API_KEY,
            "targetDt": target_date
        }
        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            data = response.json()
            daily_box_office = data["boxOfficeResult"]["dailyBoxOfficeList"]

            # 필요한 데이터만 추출하고 정제
            for movie in daily_box_office:
                cleaned_data = clean_data(movie)
                cleaned_data["targetDt"] = target_date  # 날짜 추가

                # 데이터베이스에 삽입
                query = """
                    INSERT INTO daily_box_office (
                        targetDt, movieCd, movieNm, openDt, salesAmt, salesAcc, audiAcc, scrnCnt, showCnt
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, (
                    cleaned_data["targetDt"],
                    cleaned_data["movieCd"],
                    cleaned_data["movieNm"],
                    cleaned_data["openDt"] if cleaned_data["openDt"] else None,
                    cleaned_data["salesAmt"],
                    cleaned_data["salesAcc"],
                    cleaned_data["audiAcc"],
                    cleaned_data["scrnCnt"],
                    cleaned_data["showCnt"]
                ))

            # 변경 사항 커밋
            db_conn.commit()

        else:
            print(f"API 호출 실패: {target_date}, 상태 코드: {response.status_code}")

    # DB 연결 종료
    cursor.close()
    db_conn.close()

    print("데이터 수집 및 DB 저장 완료")

# 실행
collect_box_office_data()