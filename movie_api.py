import requests
import pymysql
from datetime import datetime
from dotenv import load_dotenv 
import os 
from datetime import timedelta

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
def transform_data(box_office_data, movie_info_data, target_date):

    """박스오피스 데이터와 영화 상세정보 데이터를 정제합니다."""
    clean_records_box_office = []
    clean_records_movie_details = []

    # target_date가 datetime 객체로 변환되지 않았다면 변환
    if isinstance(target_date, str):
        target_date = datetime.strptime(target_date, "%Y%m%d")

    # 필요한 경우 다시 문자열로 변환
    target_date_str = target_date.strftime("%Y%m%d")

    for movie in box_office_data:
        movieCd = movie.get("movieCd", "")
        rank = int(movie.get("rank", 0)) # 박스오피스 순위
        movieNm = movie.get("movieNm", "") # 영화명(국문)
        openDt = movie.get("openDt", None) # 개봉일 
        salesAmt = int(movie.get("salesAmt", 0)) # 해당일의 매출액 
        audiCnt = int(movie.get("audiCnt", 0)) # 해당일의 관객수 
        scrnCnt = int(movie.get("scrnCnt", 0)) # 해당일의 상영 스크린 수
        showCnt = int(movie.get("showCnt", 0)) # 해당일의 상영 횟수 

        # 정제된 박스오피스 데이터를 저장
        clean_records_box_office.append((
            movieCd, target_date_str, rank, movieNm, openDt, salesAmt, audiCnt, scrnCnt, showCnt
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

        # movie_details 데이터 수집
        clean_records_movie_details.append((
            movieCd, nation_str, genre_str, director_str, actor_str, show_type_str, company_str, watch_grade_str
        ))

    return clean_records_box_office, clean_records_movie_details

### 1. 관계 테이블에 column_nm이 있는지 확인 
### - 관계 테이블에 column_nm이 있으면 기존 column_id를 가져옴 
### 2. 새로운 column_nm이면 추가함 
### 3. movie_and_relation 테이블에 매핑 

# 관계 테이블의 column_nm이 있는지 확인하고 column_id를 가져오는 함수
def get_relation_id(connection, table_nm, column_nm): 
    """관계 테이블에서 column_nm을 기준으로 column_id를 확인하고 없으면 추가"""
    cursor = connection.cursor()
    select_query = f"SELECT {table_nm}_id FROM {table_nm} WHERE {table_nm}_nm = %s"
    cursor.execute(select_query, (column_nm,))
    result = cursor.fetchone() 

    if result:
        # 이미 존재하는 경우 ID 반환
        return result[0]
    else:
        # 새로운 데이터를 추가하고 ID 반환
        insert_query = f"INSERT INTO {table_nm} ({table_nm}_nm) VALUES (%s)"
        cursor.execute(insert_query, (column_nm,))
        # connection.commit()
        return cursor.lastrowid
    
# 영화와 *의 관계를 movie_and_relation 테이블에 추가하는 함수 
def insert_relation(connection, movie_id, table_nm, column_id):
    """영화와 관계 테이블에 매핑"""
    cursor = connection.cursor()

    # 중복 확인 쿼리
    check_query = f"SELECT 1 FROM movie_and_{table_nm} WHERE movie_id = %s AND {table_nm}_id = %s"
    cursor.execute(check_query, (movie_id, column_id))
    if cursor.fetchone():
        print(f"영화 {movie_id}와 {table_nm} {column_id}은(는) 이미 존재합니다.")
        return  # 이미 존재하면 삽입하지 않음
    
    # 중복되지 않으면 삽입
    insert_query = f"INSERT INTO movie_and_{table_nm} (movie_id, {table_nm}_id) VALUES (%s, %s)"
    cursor.execute(insert_query, (movie_id, column_id))
    connection.commit()
    print(f"영화 {movie_id}와 {table_nm} {column_id} 처리 완료")
    # connection.commit()

def process_movie_relations(connection, movie_id, nations, genres, directors, actors, showTypes, companies):
    """각 영화에 대해 국가, 장르, 감독, 배우, 상영형태, 회사와 관계 테이블을 매핑"""
    
    # 국가 처리
    for nation in nations:
        nation_id = get_relation_id(connection, "nation", nation)
        insert_relation(connection, movie_id, "nation", nation_id)
        print(f"영화 {movie_id}와 국가 {nation} 처리 완료")
    
    # 장르 처리
    for genre in genres:
        genre_id = get_relation_id(connection, "genre", genre)
        insert_relation(connection, movie_id, "genre", genre_id)
        print(f"영화 {movie_id}와 장르 {genre} 처리 완료")
    
    # 감독 처리
    for director in directors:
        director_id = get_relation_id(connection, "director", director)
        insert_relation(connection, movie_id, "director", director_id)
        print(f"영화 {movie_id}와 감독 {director} 처리 완료")
    
    # 배우 처리
    for actor in actors:
        actor_id = get_relation_id(connection, "actor", actor)
        insert_relation(connection, movie_id, "actor", actor_id)
        print(f"영화 {movie_id}와 배우 {actor} 처리 완료")
    
    # 상영형태 처리
    for show_type in showTypes:
        show_type_id = get_relation_id(connection, "show_type", show_type)
        insert_relation(connection, movie_id, "show_type", show_type_id)
        print(f"영화 {movie_id}와 상영형태 {show_type} 처리 완료")
    
    # 제작사 처리
    for company in companies:
        company_id = get_relation_id(connection, "company", company)
        insert_relation(connection, movie_id, "company", company_id)
        print(f"영화 {movie_id}와 영화사 {company} 처리 완료")

# 데이터 로드(Load)
def load_data_to_mysql(box_office_records, movie_details_records, target_date):
    # MySQL 연결
    connection = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
    cursor = connection.cursor()

    # 1. 박스오피스 데이터 가져오기
    box_office_data = fetch_daily_box_office(target_date)

    # 2. 영화 상세 정보 가져오기
    movie_info_data = {}
    for movie in box_office_data:
        movieCd = movie.get("movieCd")
        movie_info_data[movieCd] = fetch_movie_info(movieCd)

    # 3. 데이터 변환
    clean_records_box_office, clean_records_movie_details = transform_data(box_office_data, movie_info_data, target_date)

    # 4. 박스오피스 테이블 삽입 (기본 정보)
    insert_box_office_sql = """
    INSERT INTO daily_box_office (
        movie_id, movie_nm, open_date
    ) VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        movie_nm = VALUES(movie_nm),
        open_date = VALUES(open_date)
    """
    for record in clean_records_box_office:
        cursor.execute(insert_box_office_sql, (record[0], record[3], record[4]))
    connection.commit()

    # 4-1. 일별 박스오피스 데이터 테이블 삽입 (상세 정보)
    insert_box_office_data_sql = """
    INSERT INTO daily_box_office_data (
        target_date, movie_id, movie_rank, sales_amt, audi_cnt, scrn_cnt, show_cnt
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        sales_amt = VALUES(sales_amt),
        audi_cnt = VALUES(audi_cnt),
        scrn_cnt = VALUES(scrn_cnt),
        show_cnt = VALUES(show_cnt)
    """
    for record in clean_records_box_office:
        cursor.execute(insert_box_office_data_sql, (record[1], record[0], record[2], record[5], record[6], record[7], record[8]))
    connection.commit()
    

    # 5. 영화 상세 데이터 삽입
    for record in clean_records_movie_details:
        insert_sql = """
        INSERT INTO movie_details (
            movie_id, nation_nm, genre_nm, director_nm, actor_nm, show_type_nm, company_nm, watch_grade_nm) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            nation_nm = IFNULL(VALUES(nation_nm), nation_nm),
            genre_nm = IFNULL(VALUES(genre_nm), genre_nm),
            director_nm = IFNULL(VALUES(director_nm), director_nm),
            actor_nm = IFNULL(VALUES(actor_nm), actor_nm),
            show_type_nm = IFNULL(VALUES(show_type_nm), show_type_nm),
            company_nm = IFNULL(VALUES(company_nm), company_nm),
            watch_grade_nm = IFNULL(VALUES(watch_grade_nm), watch_grade_nm)
        """
        cursor.executemany(insert_sql, clean_records_movie_details)
        cursor.connection.commit()
    
    # 연결 종료
    connection.close()

def load_movie_relations_to_mysql(box_office_data, movie_info_data):
    connection = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
    cursor = connection.cursor()

    # 데이터 정제
    clean_records_box_office, clean_records_movie_details = transform_data(box_office_data, movie_info_data, target_date)

    # 영화 상세 정보 처리
    for movie in clean_records_movie_details:
        print(f"처리 중인 영화: {movie}\n")
        movie_id = movie[0]  # 영화ID
        nations = movie[1].split(",") if movie[1] else []  # 국가 목록
        genres = movie[2].split(",") if movie[2] else []  # 장르 목록
        directors = movie[3].split(",") if movie[3] else []  # 감독 목록
        actors = movie[4].split(",") if movie[4] else []  # 배우 목록
        showTypes = movie[5].split(",") if movie[5] else []  # 상영형태 목록
        companies = movie[6].split(",") if movie[6] else []  # 제작사 목록

        # 관계 테이블에 데이터 삽입
        process_movie_relations(connection, movie_id, nations, genres, directors, actors, showTypes, companies)

    # 한 번에 커밋
    connection.commit()
    # 연결 종료
    connection.close()

# 날짜 목록 생성
def get_dates(start_date):
    """시작 날짜부터 설정 기간의 날짜 목록 생성"""
    # start_date가 문자열이라면 datetime 객체로 변환
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y%m%d")

    dates = []
    current_date = start_date  # datetime 객체로 초기화
    for i in range(38):  # 기간 설정 
        # 날짜를 "YYYYMMDD" 형식의 문자열로 저장
        dates.append(current_date.strftime("%Y%m%d"))
        # 다음 날짜로 이동
        current_date += timedelta(days=1)

    print("get_date: ", dates)
    return dates

if __name__ == "__main__":
    start_date = "20241101"  # 데이터를 수집할 시작 날짜    
    
    try:
        # MySQL 연결 생성
        connection = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
        cursor = connection.cursor() 

        # 날짜 생성
        week_dates = get_dates(start_date)
        for target_date in week_dates:
            # print(type(target_date)) # str
            # print(f"날짜 처리 중: {target_date}")

            # 1. 일별 박스오피스 데이터 수집
            box_office_data = fetch_daily_box_office(target_date)
            # print("박스오피스 데이터 수집 성공 ! \n")
            
            # 2. 각 영화의 상세정보 수집
            movie_info_data = {}
            for movie in box_office_data:
                movieCd = movie["movieCd"]
                movie_info_data[movieCd] = fetch_movie_info(movieCd)
            # print("영화 상세 데이터 수집 성공 ! \n")
        
            # 3. 데이터 정제
            box_office_records, movie_details_records = transform_data(
                box_office_data, movie_info_data, target_date)
            # print("정제된 데이터 확인 - 박스오피스:\n", box_office_records)
            # print("정제된 데이터 확인 - 영화 상세정보:\n", movie_details_records)
        
            # 4. MySQL에 데이터 삽입
            load_data_to_mysql(box_office_records, movie_details_records, target_date)  
            # print("박스오피스, 상세정보 테이블 적재 성공 \n")
            load_movie_relations_to_mysql(box_office_data, movie_info_data)
            # print("관계 테이블 적재 성공\n")
 
        # 연결 종료
        cursor.close()
        connection.close()

    except Exception as e:
        print(f"오류 발생: {e}")