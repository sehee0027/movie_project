import requests
import pymysql
from datetime import datetime
from dotenv import load_dotenv 
import os 

# .env 파일 로드 
load_dotenv()

# 영화진흥위원회 API 정보
API_KEY = os.environ.get('API_KEY')
BOX_OFFICE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
MOVIE_INFO_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"

# MySQL 연결 정보
MYSQL_HOST = os.environ.get('MYSQL_HOST') 
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
MYSQL_DB = os.environ.get('MYSQL_DB')

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
def transform_data(box_office_data, movie_info_data, cursor):

    """박스오피스 데이터와 영화 상세정보 데이터를 정제합니다."""
    clean_records_box_office = []
    clean_records_movie_details = []
    # clean_records_movie_and_nation = []
    # clean_records_movie_and_genre = []
    # clean_records_movie_and_director = []
    # clean_records_movie_and_actor = []
    # clean_records_movie_and_show_type = []
    # clean_records_movie_and_company = []

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
        nations = [n.get("nationNm", "") for n in info.get("nations", [])] # 국가: ["미국"], ["한국", "중국"]
        genres = [g.get("genreNm", "") for g in info.get("genres", [])] # 장르: ["액션"], ["드라마", "스릴러"]
        directors = [d.get("peopleNm", "") for d in info.get("directors", [])] # 감독: [{"peopleNm": "봉준호"}]
        actors = [a.get("peopleNm", "") for a in info.get("actors", [])[:5]] # 배우: [{"peopleNm": "송강호"}, {"peopleNm": "김혜수"}]
        showTypes = [s.get("showTypeNm", "") for s in info.get("showTypes", [])] # 상영형태: ["2D", "IMAX"]
        companies = [c.get("companyNm", "") for c in info.get("companys", [])] # 제작사: [{"companyNm": "CJ 엔터테인먼트"}]
        watchGradeNm = [w.get("watchGradeNm", "") for w in info.get("audits", [])] # 관람등급: "15세 관람가", "전체 관람가"

        nation_str = ",".join(nations)
        genre_str = ",".join(genres)
        director_str = ",".join(directors)
        actor_str = ",".join(actors)
        show_type_str = ",".join(showTypes)
        company_str = ",".join(companies)
        watch_grade_str = ",".join(watchGradeNm)

        # 데이터 길이 확인
        # print(f"관람등급 데이터: {company_str}")
        # print(f"관람등급 데이터 길이: {len(company_str)}")

        # movie_details 데이터 수집
        clean_records_movie_details.append((
            movieCd, nation_str, genre_str, director_str, actor_str, show_type_str, company_str, watch_grade_str
        ))

        # # 국가 관계 테이블 데이터 수집
        # for nation in nations:
        #     # nationNm = nation.get("nationNm", "")
        #     clean_records_movie_and_nation.append((movieCd, nation))

        # # 장르 관계 테이블 데이터 수집
        # for genre in genres:
        #     # genreNm = genre.get("genreNm", "")
        #     clean_records_movie_and_genre.append((movieCd, genre))

        # # 감독 관계 테이블 데이터 수집
        # for director in directors:
        #     # directorNm = director.get("peopleNm", "")
        #     clean_records_movie_and_director.append((movieCd, director))

        # # 배우 관계 테이블 데이터 수집
        # for actor in actors[:5]:  # 상위 5명의 배우만 저장 
        #     # actorNm = actor.get("peopleNm", "")
        #     clean_records_movie_and_actor.append((movieCd, actor))

        # # 상영형태 관계 테이블 데이터 수집
        # for showType in showTypes:
        #     # showTypeNm = showType.get("showTypeNm", "")
        #     clean_records_movie_and_show_type.append((movieCd, showType))

        # # 제작회사 관계 테이블 데이터 수집
        # for company in companies:
        #     # companyNm = company.get("companyNm", "")
        #     clean_records_movie_and_company.append((movieCd, company))

        

    # return (clean_records_box_office, clean_records_movie_details, 
    #         clean_records_movie_and_nation, clean_records_movie_and_genre, 
    #         clean_records_movie_and_director, clean_records_movie_and_actor, 
    #         clean_records_movie_and_show_type, clean_records_movie_and_company)
    return clean_records_box_office, clean_records_movie_details

### 수정 !!! ### 
# def insert_movie_and_relation(cursor, movieCd, table_name, column_name, records):
#     """영화와 관계 데이터를 삽입합니다."""
#     for record in records:
#         column_value = record[1]  # 해당 column의 값: nation_nm
#         print(f"Processing record: {record}, column_value: {column_value}")

#         if column_value is not None:
#             # column_value에 해당하는 id 값을 조회
#             select_sql = f"SELECT {column_name}_id FROM {column_name}s WHERE {column_name}_nm = %s"
#             cursor.execute(select_sql, (column_value,))
#             result = cursor.fetchone()

#             if result is None:
#                 # 해당 column_value에 대한 id가 없다면 삽입하지 않고 건너뛰기
#                 print(f"No matching id found for {column_value}, skipping insert.")
#                 continue

#             column_id = result[0]  # 조회된 id 값을 가져옴 
#             print(f"Inserting into {column_name}s with movieCd: {movieCd}, column_id: {column_id}")
            
#             # 관계 테이블에 삽입
#             insert_sql = f"""
#             INSERT INTO {table_name} (movie_id, {column_name}_id)
#             VALUES (%s, %s)
#             ON DUPLICATE KEY UPDATE {column_name}_id = VALUES({column_name}_id)
#             """
#             cursor.execute(insert_sql, (movieCd, column_id))
#             cursor.connection.commit()  # 삽입 후 커밋

# 데이터 로드(Load)
def load_data_to_mysql():
    # MySQL 연결
    connection = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
    cursor = connection.cursor()

    # 1. 박스오피스 데이터 가져오기
    date = "20241127"  # 날짜 입력
    box_office_data = fetch_daily_box_office(date)

    # 2. 영화 상세 정보 가져오기
    movie_info_data = {}
    for movie in box_office_data:
        movieCd = movie.get("movieCd")
        movie_info_data[movieCd] = fetch_movie_info(movieCd)

    # 3. 데이터 변환
    # clean_records_box_office, clean_records_movie_details, clean_records_movie_and_nation, clean_records_movie_and_genre, clean_records_movie_and_director, clean_records_movie_and_actor, clean_records_movie_and_show_type, clean_records_movie_and_company = transform_data(box_office_data, movie_info_data, cursor)
    clean_records_box_office, clean_records_movie_details = transform_data(box_office_data, movie_info_data, cursor)

    # 4. 박스오피스 데이터 삽입
    for record in clean_records_box_office:
        insert_sql = """
        INSERT INTO daily_box_office (
            movie_id, movie_rank, movie_nm, open_date, sales_amt, sales_acc, audi_acc, scrn_cnt, show_cnt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE sales_amt = VALUES(sales_amt)
        """
        cursor.execute(insert_sql, record)
        connection.commit()

    # 5. 영화 상세 데이터 삽입
    for record in clean_records_movie_details:
        insert_sql = """
        INSERT INTO movie_details (
            movie_id, nation_nm, genre_nm, director_nm, actor_nm, show_type_nm, company_nm, watch_grade_nm) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            nation_nm = VALUES(nation_nm),
            genre_nm = VALUES(genre_nm),
            director_nm = VALUES(director_nm),
            actor_nm = VALUES(actor_nm),
            show_type_nm = VALUES(show_type_nm),
            company_nm = VALUES(company_nm),
            watch_grade_nm = VALUES(watch_grade_nm)
        """
        cursor.executemany(insert_sql, clean_records_movie_details)
        cursor.connection.commit()


    # # 6. 국가, 장르, 감독, 배우, 상영형태, 회사 관계 데이터 삽입
    # insert_movie_and_relation(cursor, movieCd, 'movie_and_nation', 'nation', clean_records_movie_and_nation)
    # insert_movie_and_relation(cursor, movieCd, 'movie_and_genre', 'genre', clean_records_movie_and_genre)
    # insert_movie_and_relation(cursor, movieCd, 'movie_and_director', 'director', clean_records_movie_and_director)
    # insert_movie_and_relation(cursor, movieCd, 'movie_and_actor', 'actor', clean_records_movie_and_actor)
    # insert_movie_and_relation(cursor, movieCd, 'movie_and_show_type', 'show_type', clean_records_movie_and_show_type)
    # insert_movie_and_relation(cursor, movieCd, 'movie_and_company', 'company', clean_records_movie_and_company)
    
    # 연결 종료
    connection.close()

if __name__ == "__main__":
    target_date = "20241125"  # 데이터를 수집할 날짜
    try:
        # MySQL 연결 생성
        connection = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
        cursor = connection.cursor() 

        # 1. 일별 박스오피스 데이터 수집
        box_office_data = fetch_daily_box_office(target_date)
        
        # 2. 각 영화의 상세정보 수집
        movie_info_data = {}
        for movie in box_office_data:
            movieCd = movie["movieCd"]
            movie_info_data[movieCd] = fetch_movie_info(movieCd)

        # 3. 데이터 정제
        # box_office_records, movie_details_records, movie_and_nation_records, movie_and_genre_records, movie_and_director_records, movie_and_actor_records, movie_and_show_type_records, movie_and_company_records = transform_data(
        #     box_office_data, movie_info_data, cursor)
        box_office_records, movie_details_records = transform_data(
            box_office_data, movie_info_data, cursor)

        # 4. MySQL에 데이터 삽입
        load_data_to_mysql()  
 
        # 연결 종료
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"오류 발생: {e}")