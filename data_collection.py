import requests
import json


# 영화진흥위원회 API 정보
API_KEY = "test"  # 발급받은 인증키
# BASE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json" # 일별 박스오피스 
# BASE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json" # 영화 상세정보 
# BASE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/company/searchCompanyInfo.json" # 영화사 상세정보
# BASE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/people/searchPeopleList.json" # 영화인 목록 
BASE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/people/searchPeopleInfo.json" # 영화인 상세정보 

# 요청 파라미터 설정
params = {
    "key": API_KEY,
    # "targetDt": "20221111"  # 예시 날짜: YYYYMMDD 형식 
    # "movieCd": "20198429" # 영화 상세정보
    # "companyCd" : "20161801" # 영화사 상세정보 
    # "peopleNm": "드웨인 존슨" # 영화인 목록 
    "peopleCd": "10013266" # 영화인 상세정보 
}

# API 호출
response = requests.get(BASE_URL, params=params)

# 응답 확인
if response.status_code == 200:  # 성공
    data = response.json()  # JSON 데이터 파싱
    print("데이터 수집 성공!")
    print(json.dumps(data, indent=4, ensure_ascii=False))  # 보기 좋게 출력
else:
    print(f"API 호출 실패. 상태 코드: {response.status_code}")

# JSON 데이터를 파일로 저장
with open("people_info.json", "w", encoding="utf-8") as file:
    json.dump(data, file, indent=4, ensure_ascii=False)
print("데이터 저장 완료: people_info.json")
