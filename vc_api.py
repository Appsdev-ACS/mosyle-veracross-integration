import requests
import pandas as pd
from datetime import datetime,timedelta

today = datetime.today().date()
tomorrow = today + timedelta(days=1)


def get_access_token(url,vc_client_id,vc_client_secret):
    """Fetch the access token from Veracross API."""
    data = {
        "grant_type": "client_credentials",
        "client_id": vc_client_id,
        "client_secret": vc_client_secret,
        "scope": "students:list staff_faculty:list"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(url, data=data, headers=headers)

    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print("Error fetching access token:", response.text)
        return None
    

    
def get_students(access_token,students_url,params_required):
    """Fetch all student data using pagination via headers."""
    access_token = access_token
    if not access_token:
        print("No access token")
        return

    all_students = []
    page = 1
    page_size = 1000 

    while True:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Page-Number": str(page),
            "X-Page-Size": str(page_size),
            "X-API-Value-Lists" : "include"

            # "X-API-Revision": "latest"  # Optional: Ensures the latest API version
        }
        params = {}
        if params_required:
            params = {
                "on_or_after_entry_date": tomorrow,
                "on_or_before_entry_date": tomorrow
            }

        response = requests.get(students_url, headers=headers,params=params)
        # print("got response for student list")

        if response.status_code == 200:
            students = response.json()
            if students["data"] == []:
                break

            grade_level = {item["id"] : item["description"] for item in students["value_lists"][1]["items"]}
            # print("grade_level",grade_level)

            for entry in students["data"]:

                if entry["grade_level"] in grade_level:
                    entry["grade_level"] = grade_level[entry["grade_level"]]
                entry["full_name"] = entry["first_name"] + " " + entry["last_name"]


            all_students.extend(students["data"])
            page += 1 
        else:
            print("Error fetching students:", response.text)
            break
    if params_required:
        page = 1
        while True:
            headers = {
                "Authorization": f"Bearer {access_token}",
                "X-Page-Number": str(page),
                "X-Page-Size": str(page_size),
                "X-API-Value-Lists" : "include"

                # "X-API-Revision": "latest"  # Optional: Ensures the latest API version
            }
            params = {}
            if params_required:
                params = {
                    "on_or_after_entry_date": tomorrow,
                    "on_or_before_entry_date": tomorrow,
                    "role": 7 #for future students
                }

            response = requests.get(students_url, headers=headers,params=params)
            # print("got response for student list")

            if response.status_code == 200:
                students = response.json()
                if students["data"] == []:
                    break

                grade_level = {item["id"] : item["description"] for item in students["value_lists"][1]["items"]}
                # print("grade_level",grade_level)

                for entry in students["data"]:

                    if entry["grade_level"] in grade_level:
                        entry["grade_level"] = grade_level[entry["grade_level"]]
                    entry["full_name"] = entry["first_name"] + " " + entry["last_name"]
                    

                all_students.extend(students["data"])
                page += 1 
            else:
                print("Error fetching students:", response.text)
                break
        


    df = pd.DataFrame(all_students)
    if df.empty:
        return df
    df = df[["id","full_name","email_1","grade_level"]]

    df = df.drop_duplicates()
    print(f"Total students fetched: {len(df)}")

    # print(df)
    return df
    # df.to_csv("yaseen samples")


def get_staff_faculty(VC_STAFF_URL,access_token,params_required):
    """Fetch all student data using pagination via headers."""
    print("calling staff list.....")
    access_token = access_token
    if not access_token:
        print("No access token")
        return

    all_staff = []
    page = 1
    page_size = 1000  
    while True:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Page-Number": str(page),
            "X-Page-Size": str(page_size),
            "X-API-Value-Lists" : "include"

            # "X-API-Revision": "latest"  # Optional: Ensures the latest API version
        }
        params = {}
        if params_required:
            params = {
                "on_or_before_date_hired": tomorrow,
                "on_or_after_date_hired": tomorrow
            }
        response = requests.get(VC_STAFF_URL, headers=headers,params=params)
        # print("got response for student list")

        if response.status_code == 200:
            staffs = response.json()
            if staffs["data"] == []:
                break


            faculty_type = {item["id"] : item["description"] for item in staffs["value_lists"][3]["items"]}

            for entry in staffs["data"]:

                if entry["faculty_type"] in faculty_type:
                    entry["faculty_type"] = faculty_type[entry["faculty_type"]]
                entry["full_name"] = entry["first_name"] + " " + entry["last_name"]


            all_staff.extend(staffs["data"])
            page += 1 
        else:
            print("Error fetching staff:", response.text)
            break
    if params_required:
        page = 1
        while True:
            headers = {
                "Authorization": f"Bearer {access_token}",
                "X-Page-Number": str(page),
                "X-Page-Size": str(page_size),
                "X-API-Value-Lists" : "include"

                # "X-API-Revision": "latest"  # Optional: Ensures the latest API version
            }
            params = {}
            if params_required:
                params = {
                    "on_or_before_date_hired": tomorrow,
                    "on_or_after_date_hired": tomorrow,
                    "role":27
                }

            response = requests.get(VC_STAFF_URL, headers=headers,params=params)
            # print("got response for student list")

            if response.status_code == 200:
                staffs = response.json()
                if staffs["data"] == []:
                    break


                faculty_type = {item["id"] : item["description"] for item in staffs["value_lists"][3]["items"]}

                for entry in staffs["data"]:

                    if entry["faculty_type"] in faculty_type:
                        entry["faculty_type"] = faculty_type[entry["faculty_type"]]
                    entry["full_name"] = entry["first_name"] + " " + entry["last_name"]


                all_staff.extend(staffs["data"])
                page += 1 
            else:
                print("Error fetching staff:", response.text)
                break
    print(f"Total staffs fetched: {len(all_staff)}")

    df = pd.DataFrame(all_staff)
    print(len(df))
    if df.empty:
        return df,df
    if params_required:
        df["date_hired"] = pd.to_datetime(df["date_hired"]).dt.date
        df = df[df["date_hired"] == tomorrow]
    df = df[["id","full_name","email_1","faculty_type"]]
    df = df.drop_duplicates()
    teacher_df = df[df["faculty_type"].str.contains("teacher", case=False, na=False)]
    staff_df = df[~df["faculty_type"].str.contains("teacher", case=False, na=False)]



    return staff_df,teacher_df
