import requests
import pymysql
from datetime import datetime

# 영화진흥위원회 API 정보
API_KEY = "temp"
BOX_OFFICE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
MOVIE_INFO_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

# MySQL 연결 정보
MYSQL_HOST = "temp"
MYSQL_USER = "temp"
MYSQL_PASSWORD = "temp!"
MYSQL_DB = "temp"


# 데이터 추출(Extract)
def fetch_daily_box_office(date):
    """일별 박스오피스 데이터를 API에서 가져옵니다."""
    params = {"key": API_KEY, "targetDt": date}
    response = requests.get(BOX_OFFICE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        return data["boxOfficeResult"]["dailyBoxOfficeList"]
    else:
        raise Exception(f"API 호출 실패: {response.status_code}")

def fetch_movie_info(movieCd):
    """영화 상세정보 데이터를 API에서 가져옵니다."""
    params = {"key": API_KEY, "movieCd": movieCd}
    response = requests.get(MOVIE_INFO_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        return data["movieInfoResult"]["movieInfo"]
    else:
        raise Exception(f"API 호출 실패: {response.status_code}")
    

# 데이터 변환(Transform)
def transform_data(box_office_data, movie_info_data):
    """박스오피스 데이터와 영화 상세정보 데이터를 정제합니다."""
    clean_records_box_office = []
    clean_records_movie_details = []
    clean_records_movie_and_nation = []
    clean_records_movie_and_genre = []
    clean_records_movie_and_director = []
    clean_records_movie_and_actor = []
    clean_records_movie_and_show_type = []
    clean_records_movie_and_company = []

    for movie in box_office_data:
        movieCd = movie.get("movieCd", "")
        rank = int(movie.get("rank", 0)) # 박스오피스 순위
        movieNm = movie.get("movieNm", "") # 영화명(국문)
        openDt = movie.get("openDt", None) # 개봉일 
        salesAmt = int(movie.get("salesAmt", 0)) # 해당일의 매출액 
        salesAcc = int(movie.get("salesAcc", 0)) # 누적 매출액 
        audiAcc = int(movie.get("audiAcc", 0)) # 누적 관객수 
        scrnCnt = int(movie.get("scrnCnt", 0)) # 해당일의 상영 스크린 수
        showCnt = int(movie.get("showCnt", 0)) # 해당일의 상영 횟수 

        # 정제된 박스오피스 데이터를 저장
        clean_records_box_office.append((
            movieCd, rank, movieNm, openDt, salesAmt, salesAcc, audiAcc, scrnCnt, showCnt
        ))

        # 영화 상세 정보
        info = movie_info_data.get(movieCd, {})
        nations = info.get("nations", []) # 국가: ["미국"], ["한국", "중국"]
        genres = info.get("genres", []) # 장르: ["액션"], ["드라마", "스릴러"]
        directors = info.get("directors", []) # 감독: [{"peopleNm": "봉준호"}]
        actors = info.get("actors", []) # 배우: [{"peopleNm": "송강호"}, {"peopleNm": "김혜수"}]
        showTypes = info.get("showTypes", []) # 상영형태: ["2D", "IMAX"]
        companies = info.get("companies", []) # 제작사: [{"companyNm": "CJ 엔터테인먼트"}]
        watchGradeNm = info.get("audits", [{}])[0].get("watchGradeNm", "") # 관람등급: "15세 관람가", "전체 관람가"

        # movie_details 데이터 수집
        clean_records_movie_details.append((movieCd, watchGradeNm))

        # 국가 관계 테이블 데이터 수집
        for nation in nations:
            nationNm = nation.get("nationNm", "")
            clean_records_movie_and_nation.append((movieCd, nationNm))

        # 장르 관계 테이블 데이터 수집
        for genre in genres:
            genreNm = genre.get("genreNm", "")
            clean_records_movie_and_genre.append((movieCd, genreNm))

        # 감독 관계 테이블 데이터 수집
        for director in directors:
            directorNm = director.get("peopleNm", "")
            clean_records_movie_and_director.append((movieCd, directorNm))

        # 배우 관계 테이블 데이터 수집
        for actor in actors[:5]:  # 상위 5명의 배우만 저장 
            actorNm = actor.get("peopleNm", "")
            clean_records_movie_and_actor.append((movieCd, actorNm))

        # 상영형태 관계 테이블 데이터 수집
        for showType in showTypes:
            showTypeNm = showType.get("showTypeNm", "")
            clean_records_movie_and_show_type.append((movieCd, showTypeNm))

        # 제작회사 관계 테이블 데이터 수집
        for company in companies:
            companyNm = company.get("companyNm", "")
            clean_records_movie_and_company.append((movieCd, companyNm))

    return clean_records_box_office, clean_records_movie_details, clean_records_movie_and_nation, clean_records_movie_and_genre, clean_records_movie_and_director, clean_records_movie_and_actor, clean_records_movie_and_show_type, clean_records_movie_and_company

def get_or_insert(cursor, table_name, column_name, value):
    """주어진 값이 테이블에 존재하지 않으면 삽입하고 해당 ID를 반환합니다."""
    # 국가 테이블에서 확인
    check_sql = f"SELECT {column_name}_id FROM {table_name} WHERE {column_name} = %s"
    cursor.execute(check_sql, (value,)) 
    result = cursor.fetchone()
    if result:
        return result[f"{column_name}_id"]  # 존재하는 경우 ID 반환
    else:
        # 삽입 후 생성된 ID 반환
        insert_sql = f"INSERT INTO {table_name} ({column_name}) VALUES (%s)"
        cursor.execute(insert_sql, (value,))
        cursor.connection.commit()  # 삽입 후 커밋
        return cursor.lastrowid  # 새로 생성된 ID 반환
    
# def get_or_insert_show_type(cursor, show_type_name):
#     """상영형태가 이미 존재하는지 확인하고, 없으면 추가하고 ID 반환"""
#     # 이미 존재하는지 확인
#     cursor.execute("SELECT show_type_id FROM show_types WHERE show_type = %s", (show_type_name,))
#     result = cursor.fetchone()
    
#     if result:
#         # 이미 존재하면 ID 반환
#         return result['show_type_id']
#     else:
#         # 없으면 새로 추가하고 ID 반환
#         cursor.execute("INSERT INTO show_types (show_type) VALUES (%s)", (show_type_name,))
#         cursor.connection.commit()  # 삽입 후 커밋
#         return cursor.lastrowid  # 새로 생성된 show_type_id 반환
    
# def get_or_insert_nation(cursor, nation_name):
#     """상영형태가 이미 존재하는지 확인하고, 없으면 추가하고 ID 반환"""
#     # 이미 존재하는지 확인
#     cursor.execute("SELECT nation_id FROM nation_nm WHERE nation_nm = %s", (nation_name,))
#     result = cursor.fetchone()
    
#     if result:
#         # 이미 존재하면 ID 반환
#         return result['nation_id']
#     else:
#         # 없으면 새로 추가하고 ID 반환
#         cursor.execute("INSERT INTO nation_nm (nation_nm) VALUES (%s)", (nation_name,))
#         cursor.connection.commit()  # 삽입 후 커밋
#         return cursor.lastrowid  # 새로 생성된 nation_id 반환

### 여기서부터 하는중 !!! ### 
# insert_movie_and_relation(cursor, movieCd, 'movie_and_nation', 'nation', clean_records_movie_and_nation)
def insert_movie_and_relation(cursor, movieCd, table_name, column_name, records):
    """영화와 관계 데이터를 삽입합니다."""
    for record in records:
        column_value = record[1]  # 해당 column의 값
        print(f"Processing record: {record}, column_value: {column_value}")

        if column_value is not None:
            # column_value에 해당하는 id 값을 조회
            select_sql = f"SELECT {column_name}_id FROM {table_name} WHERE {column_name}_id = %s"
            cursor.execute(select_sql, (column_value,))
            result = cursor.fetchone()

            if result is None:
                # 해당 column_value에 대한 id가 없다면 삽입하지 않고 건너뛰기
                print(f"No matching id found for {column_value}, skipping insert.")
                continue

            column_id = result[0]  # 조회된 id 값을 가져옴
            print(f"Inserting into {table_name} with movieCd: {movieCd}, column_id: {column_id}")
            
            # 관계 테이블에 삽입
            insert_sql = f"""
            INSERT INTO {table_name} (movie_id, {column_name}_id)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE {column_name}_id = VALUES({column_name}_id)
            """
            cursor.execute(insert_sql, (movieCd, column_id))
            cursor.connection.commit()  # 삽입 후 커밋





# 데이터 로드(Load)
def load_data_to_mysql():
    # MySQL 연결
    connection = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
    cursor = connection.cursor()

    # 1. 박스오피스 데이터 가져오기
    date = "20241125"  # 오늘 날짜
    box_office_data = fetch_daily_box_office(date)

    # 2. 영화 상세 정보 가져오기
    movie_info_data = {}
    for movie in box_office_data:
        movieCd = movie.get("movieCd")
        movie_info_data[movieCd] = fetch_movie_info(movieCd)

    # 3. 데이터 변환
    clean_records_box_office, clean_records_movie_details, clean_records_movie_and_nation, clean_records_movie_and_genre, clean_records_movie_and_director, clean_records_movie_and_actor, clean_records_movie_and_show_type, clean_records_movie_and_company = transform_data(box_office_data, movie_info_data)

    # 4. 박스오피스 데이터 삽입
    for record in clean_records_box_office:
        insert_sql = """
        INSERT INTO daily_box_office (movie_id, movie_rank, movie_nm, open_date, sales_amt, sales_acc, audi_acc, scrn_cnt, show_cnt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE sales_amt = VALUES(sales_amt)
        """
        cursor.execute(insert_sql, record)
        connection.commit()

    # 5. 영화 상세 데이터 삽입
    for record in clean_records_movie_details:
        insert_sql = """
        INSERT INTO movie_details (movie_id, watch_grade_nm)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE watch_grade_nm = VALUES(watch_grade_nm)
        """
        cursor.execute(insert_sql, record)
        connection.commit()

    # 6. 국가, 장르, 감독, 배우, 상영형태, 회사 관계 데이터 삽입
    insert_movie_and_relation(cursor, movieCd, 'movie_and_nation', 'nation', clean_records_movie_and_nation)
    insert_movie_and_relation(cursor, movieCd, 'movie_and_genre', 'genre', clean_records_movie_and_genre)
    insert_movie_and_relation(cursor, movieCd, 'movie_and_director', 'director', clean_records_movie_and_director)
    insert_movie_and_relation(cursor, movieCd, 'movie_and_actor', 'actor', clean_records_movie_and_actor)
    insert_movie_and_relation(cursor, movieCd, 'movie_and_show_type', 'show_type', clean_records_movie_and_show_type)
    insert_movie_and_relation(cursor, movieCd, 'movie_and_company', 'company', clean_records_movie_and_company)
    
    # 연결 종료
    connection.close()

# if __name__ == "__main__":
#     target_date = "20241125"  # 데이터를 수집할 날짜
#     try:
#         # 1. 일별 박스오피스 데이터 수집
#         box_office_data = fetch_daily_box_office(target_date)
        
#         # 2. 각 영화의 상세정보 수집
#         movie_info_data = {}
#         for movie in box_office_data:
#             movieCd = movie["movieCd"]
#             movie_info_data[movieCd] = fetch_movie_info(movieCd)

#         # 3. 데이터 정제
#         box_office_records, movie_details_records, movie_and_nation_records, movie_and_genre_records, movie_and_director_records, movie_and_actor_records, movie_and_show_type_records, movie_and_company_records = transform_data(
#             box_office_data, movie_info_data)

#         # 4. MySQL에 데이터 삽입
#         load_data_to_mysql(
#             box_office_records, movie_details_records, movie_and_nation_records, movie_and_genre_records,
#             movie_and_director_records, movie_and_actor_records, movie_and_show_type_records,
#             movie_and_company_records
#         )

#     except Exception as e:
#         print(f"오류 발생: {e}")

if __name__ == "__main__":
    target_date = "20241125"  # 데이터를 수집할 날짜
    try:
        # 1. 일별 박스오피스 데이터 수집
        box_office_data = fetch_daily_box_office(target_date)
        
        # 2. 각 영화의 상세정보 수집
        movie_info_data = {}
        for movie in box_office_data:
            movieCd = movie["movieCd"]
            movie_info_data[movieCd] = fetch_movie_info(movieCd)

        # 3. 데이터 정제
        box_office_records, movie_details_records, movie_and_nation_records, movie_and_genre_records, movie_and_director_records, movie_and_actor_records, movie_and_show_type_records, movie_and_company_records = transform_data(
            box_office_data, movie_info_data)

        # 4. MySQL에 데이터 삽입
        load_data_to_mysql()  # 수정된 함수를 호출, 매개변수는 필요하지 않음

    except Exception as e:
        print(f"오류 발생: {e}")