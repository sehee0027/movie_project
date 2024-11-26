import requests
import json
from datetime import datetime, timedelta

# API 정보
API_KEY = "a180f9e7fadf4bee9e292b33558b1fed"  # 발급받은 API 키 입력
BASE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"

# 데이터 수집 함수
def collect_data(start_date, end_date, output_file):
    current_date = start_date

    all_data = []  # 수집한 데이터를 저장할 리스트

    while current_date <= end_date:
        params = {
            "key": API_KEY,
            "targetDt": current_date.strftime("%Y%m%d")
        }
        response = requests.get(BASE_URL, params=params)

        if response.status_code == 200:
            data = response.json()
            if "boxOfficeResult" in data:  # 유효한 데이터 확인
                all_data.append(data["boxOfficeResult"])
                print(f"{current_date.strftime('%Y-%m-%d')} 데이터 수집 성공!")
            else:
                print(f"{current_date.strftime('%Y-%m-%d')} 데이터 없음.")
        else:
            print(f"API 호출 실패: {response.status_code}")
        
        current_date += timedelta(days=1)

    # 데이터를 JSON 파일로 저장
    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(all_data, file, indent=4, ensure_ascii=False)

    print(f"모든 데이터 저장 완료: {output_file}")

# 실행
if __name__ == "__main__":
    start_date = datetime(2024, 11, 1)  # 수집 시작 날짜
    end_date = datetime(2023, 11, 20)  # 수집 종료 날짜
    output_file = "boxoffice_data.json"  # 저장할 파일 이름

    collect_data(start_date, end_date, output_file)

response = requests.get(BASE_URL, params=params)

print(f"API 호출 상태 코드: {response.status_code}")
print(f"API 응답 내용: {response.text}")