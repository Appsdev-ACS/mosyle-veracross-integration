import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import pandas as pd
import os
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("Mosyle Integration")
import time


def get_token(AUTH_URL,EMAIL,PASSWORD,TOKEN):
    if not all([AUTH_URL,EMAIL,PASSWORD,TOKEN]):
        raise ValueError("Missing required parameters for auth token!")
    
    data = {
        "accessToken": TOKEN,
        "email": EMAIL,
        "password": PASSWORD
    }
    headers = {"Content-Type": "application/json"}

    response = requests.post(AUTH_URL, json=data, headers=headers)


    if response.status_code == 200:
        return response.headers.get("Authorization")

    else:
        print("Error fetching access token:", response.text)
        return None
    


def create_users(MOSYLE_USERS_URL, accessToken, jwt_token, users, operation, max_workers=5, batch_size=20):
    if users.empty:
        print("No users available to " + operation)
        return {
            "message": "No users are available to add",
            "status": "OK"
        }

    if not all([MOSYLE_USERS_URL, accessToken, jwt_token]):
        raise ValueError("Missing required parameters for create user!")

    updated_count = 0
    failures = []

    def post_user_batch(batch_df):
        elements_list = []
        for _, user_row in batch_df.iterrows():
            element = {
                "operation": operation,
                "id": user_row["id"],
                "name": user_row["full_name"],
                "type": user_row["type"],
                "email": user_row["email_1"],
                "welcome_email": 0
            }

            if user_row["type"] == "S":
                element["locations"] = [{"name": "ACS Abu Dhabi", "grade_level": user_row["grade_level"]}]
            else:
                element["locations"] = [{"name": "ACS Abu Dhabi"}]

            elements_list.append(element)

        user_data = {
            "accessToken": accessToken,
            "elements": elements_list
        }

        headers = {"Authorization": jwt_token, "Content-Type": "application/json"}

        # Retry logic with exponential backoff
        for attempt in range(5):
            try:
                resp = requests.post(MOSYLE_USERS_URL, json=user_data, headers=headers, timeout=15)
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning("429 Too Many Requests. Sleeping %s seconds", wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                logger.info(f"{operation} done for batch of {len(batch_df)} users")
                return {"success": True, "count": len(batch_df)}
            except Exception as e:
                last_error = str(e)
                time.sleep(1)
        return {"success": False, "error": last_error, "count": len(batch_df)}

    # Split users into batches
    batches = [users.iloc[i:i+batch_size] for i in range(0, len(users), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(post_user_batch, batch) for batch in batches]
        for future in as_completed(futures):
            result = future.result()
            if result["success"]:
                updated_count += result["count"]
            else:
                failures.append({"error": result["error"], "count": result["count"]})

    return {
        "status": "OK" if not failures else "partial",
        "updated": updated_count,
        "failed": len(failures),
        "failures": failures[:20],
    }








def list_users(MOSYLE_LIST_USERS_URL, accessToken, jwt_token, max_workers=5):
    if not all([MOSYLE_LIST_USERS_URL, accessToken, jwt_token]):
        raise ValueError("Missing required parameters for list user!")

    headers = {
        "Authorization": jwt_token,
        "Content-Type": "application/json"
    }

    # Helper to fetch one page
    def fetch_page(page):
        data = {
            "accessToken": accessToken,
            "options": {
                "specific_columns": ["type", "name", "email", "grades"],
                "page": page
            }
        }
        try:
            resp = requests.post(MOSYLE_LIST_USERS_URL, json=data, headers=headers, timeout=15)
            resp.raise_for_status()
            resp_json = resp.json()
            users = resp_json["response"]["users"]

            # for entry in users:
            #     if entry["type"] == "STUDENT":
            #         entry["grade"] = entry.get("grades", [None])[0]
            #         entry["type"] = "S"
            #     elif entry["type"] == "TEACHER":
            #         entry["type"] = "T"
            for entry in users:
                grades = entry.get("grades")
                if isinstance(grades, list) and grades:
                    entry["grade"] = grades[0]
                else:
                    entry["grade"] = None

                # Map type
                if entry["type"] == "STUDENT":
                    entry["type"] = "S"
                elif entry["type"] == "TEACHER":
                    entry["type"] = "T"


            return users, resp_json
        except Exception as e:
            print(f"Failed to fetch page {page}: {e}")
            return [], None

    # First page to get total_pages
    first_page_users, first_resp = fetch_page(1)
    if not first_resp:
        return pd.DataFrame()  # Failed first request

    total_records = first_resp["response"]["total"]
    page_size = first_resp["response"]["page_size"]
    total_pages = math.ceil(int(total_records) / page_size)

    all_users = first_page_users

    # Fetch remaining pages concurrently
    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_page, page): page for page in range(2, total_pages + 1)}
            for future in as_completed(futures):
                users_page, _ = future.result()
                all_users.extend(users_page)

    df = pd.DataFrame(all_users)
    if "grades" in df.columns:
        df = df.drop(columns=["grades"])

    return df







def delete_users(MOSYLE_USERS_URL, accessToken, jwt_token, users, max_workers=5, batch_size=20):
    if users.empty:
        print("No users available to delete")
        return {"message": "No users to delete", "status": "OK"}

    if not all([MOSYLE_USERS_URL, accessToken, jwt_token]):
        raise ValueError("Missing required parameters for delete user!")

    deleted_count = 0
    failures = []

    def delete_user_batch(batch_df):
        elements_list = [{"operation": "delete", "id": str(user_id)} for user_id in batch_df["id"]]
        user_data = {
            "accessToken": accessToken,
            "elements": elements_list
        }
        headers = {"Authorization": jwt_token, "Content-Type": "application/json"}

        # Retry with exponential backoff
        for attempt in range(5):
            try:
                resp = requests.post(MOSYLE_USERS_URL, json=user_data, headers=headers, timeout=15)
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning("429 Too Many Requests. Sleeping %s seconds", wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                resp_json = resp.json()

                # Count only elements with status "OK" as success
                batch_success_count = sum(1 for el in resp_json.get("elements", []) if el.get("status") == "OK")
                batch_failures = [
                    {"id": el.get("id"), "status": el.get("status")}
                    for el in resp_json.get("elements", [])
                    if el.get("status") != "OK"
                ]

                deleted_count_local = batch_success_count
                logger.info(f"Deleted {deleted_count_local}/{len(batch_df)} users in batch")
                return {"success": True, "count": deleted_count_local, "failures": batch_failures}

            except Exception as e:
                last_error = str(e)
                time.sleep(1)

        # If all retries fail
        return {"success": False, "error": last_error, "count": len(batch_df)}

    batches = [users.iloc[i:i+batch_size] for i in range(0, len(users), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(delete_user_batch, batch) for batch in batches]
        for future in as_completed(futures):
            result = future.result()
            if result["success"]:
                deleted_count += result.get("count", 0)
                failures.extend(result.get("failures", []))
            else:
                failures.append({"error": result["error"], "count": result["count"]})

    return {
        "status": "OK" if not failures else "partial",
        "deleted": deleted_count,
        "failed": len(failures),
        "failures": failures[:20],
    }
