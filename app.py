import pandas as pd
import os
import logging
from flask import Flask,jsonify
from dotenv import load_dotenv
from mosyle_api import get_token,create_users,list_users,delete_users
from vc_api import get_students,get_access_token,get_staff_faculty


app = Flask(__name__)
load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("Mosyle Integration")

VC_CLIENT_ID = os.getenv("VC_CLIENT_ID")
VC_CLIENT_SECRET = os.getenv("VC_CLIENT_SECRET")
VC_TOKEN_URL = "https://accounts.veracross.com/acsad/oauth/token"
VC_STAFF_URL = "https://api.veracross.com/ACSAD/v3/staff_faculty"
VC_STUDENTS_URL = "https://api.veracross.com/ACSAD/v3/students"


MOSYLE_EMAIL = os.getenv("MOSYLE_EMAIL")
MOSYLE_PASSWORD = os.getenv("MOSYLE_PASSWORD")
MOSYLE_TOKEN = os.getenv("MOSYLE_ACCESS_TOKEN")
MOSYLE_AUTH_URL = "https://managerapi.mosyle.com/v2/login?"
MOSYLE_USERS_URL = "https://managerapi.mosyle.com/v2/users"
MOSYLE_LIST_USERS_URL = "https://managerapi.mosyle.com/v2/listusers"



@app.route("/create_new_students")
def create_students():
    try:
        vc_access_token = get_access_token(url=VC_TOKEN_URL,vc_client_id=VC_CLIENT_ID,vc_client_secret=VC_CLIENT_SECRET)
        mosyle_jwt = get_token(AUTH_URL=MOSYLE_AUTH_URL,EMAIL=MOSYLE_EMAIL,PASSWORD=MOSYLE_PASSWORD,TOKEN=MOSYLE_TOKEN)
        students = get_students(access_token=vc_access_token,students_url=VC_STUDENTS_URL,params_required=True)
        students["type"] = "S"


        result = create_users(MOSYLE_USERS_URL = MOSYLE_USERS_URL,accessToken=MOSYLE_TOKEN,jwt_token=mosyle_jwt,users = students,operation="save")
        # create_user = {}
        # return create_user
        # result = create_students() 
        code = 200 if result["status"] in ("OK", "partial") else 500

        return jsonify(result), code
    except Exception as e:
        logger.exception("Job failed")
        return jsonify({"status": "error", "message": str(e)}), 500
    

@app.route("/create_new_staff_teacher")
def create_staffs():
    try:
        vc_access_token = get_access_token(url=VC_TOKEN_URL,vc_client_id=VC_CLIENT_ID,vc_client_secret=VC_CLIENT_SECRET)
        mosyle_jwt = get_token(AUTH_URL=MOSYLE_AUTH_URL,EMAIL=MOSYLE_EMAIL,PASSWORD=MOSYLE_PASSWORD,TOKEN=MOSYLE_TOKEN)
        staff_df,teacher_df = get_staff_faculty(access_token=vc_access_token,VC_STAFF_URL=VC_STAFF_URL,params_required=True)

        staff_df["type"] = "STAFF"
        teacher_df["type"] = "T"

        
        result_staff = create_users(MOSYLE_USERS_URL = MOSYLE_USERS_URL,accessToken=MOSYLE_TOKEN,jwt_token=mosyle_jwt,users = staff_df,operation="save")
        result_teacher = create_users(MOSYLE_USERS_URL = MOSYLE_USERS_URL,accessToken=MOSYLE_TOKEN,jwt_token=mosyle_jwt,users = teacher_df,operation="save")


        combined_result = {
            "status": "OK" if result_staff["status"] == "OK" and result_teacher["status"] == "OK" else "partial",
            "updated": result_staff.get("updated", 0) + result_teacher.get("updated", 0),
            "failed": result_staff.get("failed", 0) + result_teacher.get("failed", 0),
            "failures": result_staff.get("failures", []) + result_teacher.get("failures", [])
        }

        code = 200 if combined_result["status"] in ("OK", "partial") else 500

        return jsonify(combined_result), code
    except Exception as e:
        logger.exception("Job failed")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/cleanup")
def cleanup():
    try:
        vc_access_token = get_access_token(url=VC_TOKEN_URL,vc_client_id=VC_CLIENT_ID,vc_client_secret=VC_CLIENT_SECRET)
        mosyle_jwt = get_token(AUTH_URL=MOSYLE_AUTH_URL,EMAIL=MOSYLE_EMAIL,PASSWORD=MOSYLE_PASSWORD,TOKEN=MOSYLE_TOKEN)
        staff_df,teacher_df = get_staff_faculty(access_token=vc_access_token,VC_STAFF_URL=VC_STAFF_URL,params_required=False)
        students = get_students(access_token=vc_access_token,students_url=VC_STUDENTS_URL,params_required=False)
        mosyle_users = list_users(MOSYLE_LIST_USERS_URL=MOSYLE_LIST_USERS_URL,accessToken=MOSYLE_TOKEN,jwt_token=mosyle_jwt)
        print("got mosyle users!")

        staff_df["type"] = "STAFF"
        teacher_df["type"] = "T"
        students["type"] = "S"
        staff_df["grade_level"] = None
        teacher_df["grade_level"] = None
        staff_df = staff_df.drop(columns=["faculty_type"])
        teacher_df = teacher_df.drop(columns=["faculty_type"])

        vc_users_df = pd.concat([students, staff_df, teacher_df], ignore_index=True)
        vc_users_df = vc_users_df[vc_users_df["email_1"].str.contains("@acs.sch.ae", na=False)]
        if vc_users_df.empty or mosyle_users.empty:
            return {
            "status": "EMPTY DATA FRAME",
            "updated": 0,
            "failed": 1,
            "failures": [{"error": "One or both DataFrames are empty"}]
        }

        mosyle_users = mosyle_users.rename(columns={
                "name": "full_name",
                "email": "email_1",
                "grade": "grade_level"
            })



        #normalize type of id
        vc_users_df["id"] = vc_users_df["id"].astype(str)
        mosyle_users["id"] = mosyle_users["id"].astype(str)
        vc_users_df = vc_users_df.fillna("")
        mosyle_users = mosyle_users.fillna("")



        to_add_df =  vc_users_df[~vc_users_df["id"].isin(mosyle_users["id"])]
        print(to_add_df,"add")
        # to_add_df.to_csv("add.csv", index=False)

        to_delete_df = mosyle_users[~mosyle_users["id"].isin(vc_users_df["id"])]
        to_delete_df = to_delete_df[to_delete_df["type"].str.upper() != "ADMIN"]

        print(to_delete_df,"delete")
        # to_delete_df.to_csv("delete.csv", index=False)

        # Merge VC and Mosyle on id
        merged = vc_users_df.merge(mosyle_users, on="id", suffixes=("_vc", "_mosyle"))

        # Find rows where any field differs
        update_mask = (
            (merged["full_name_vc"] != merged["full_name_mosyle"]) |
            (merged["email_1_vc"] != merged["email_1_mosyle"]) |
            (merged["grade_level_vc"] != merged["grade_level_mosyle"]) |
            (merged["type_vc"] != merged["type_mosyle"])
        )

        to_update = merged[update_mask]
        to_update = to_update[to_update["type_mosyle"].str.upper() != "ADMIN"]
        # to_update.to_csv("update_mask.csv", index=False)


        # Keep only VC columns for update
        to_update = to_update[['id', 'full_name_vc', 'email_1_vc', 'grade_level_vc', 'type_vc']]

        # Optional: rename back to VC names
        to_update = to_update.rename(columns={
            "full_name_vc": "full_name",
            "email_1_vc": "email_1",
            "grade_level_vc": "grade_level",
            "type_vc": "type"
        })
        # to_update = to_update[vc_users_df.columns]
        print(to_update,"update")
        # to_update = to_update[to_update["type"].str.upper() != "ADMIN"]

        # to_update.to_csv("update.csv", index=False)

        result_updated = create_users(MOSYLE_USERS_URL = MOSYLE_USERS_URL,accessToken=MOSYLE_TOKEN,jwt_token=mosyle_jwt,users = to_update,operation="update")
        result_added = create_users(MOSYLE_USERS_URL = MOSYLE_USERS_URL,accessToken=MOSYLE_TOKEN,jwt_token=mosyle_jwt,users = to_add_df,operation="save")
        result_deleted = delete_users(MOSYLE_USERS_URL = MOSYLE_USERS_URL,accessToken=MOSYLE_TOKEN,jwt_token=mosyle_jwt,users = to_delete_df)

        # print(result_updated)




       
        combined_result = {
            "status": "OK" if result_updated["status"] == "OK" and result_added["status"] == "OK" and result_deleted["status"] == "OK" else "partial",
            "updated": result_updated.get("updated", 0) + result_added.get("updated", 0) ,
            "deleted": result_deleted.get("deleted", 0),
            "failed": result_updated.get("failed", 0) + result_added.get("failed", 0) + result_deleted.get("failed", 0),
            "failures": result_updated.get("failures", []) + result_added.get("failures", []) + result_deleted.get("failures", [])
        }

        code = 200 if combined_result["status"] in ("OK", "partial") else 500

        return jsonify(combined_result), code
    except Exception as e:
        logger.exception("Job failed")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run() #staging

        # Cloud Run sets PORT
    # port = int(os.environ.get("PORT", "8080"))
    # app.run(host="0.0.0.0", port=port)
