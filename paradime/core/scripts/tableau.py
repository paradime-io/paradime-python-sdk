import requests


def trigger_tableau_refresh(
    *,
    host: str,
    personal_access_token_name: str,
    personal_access_token_secret: str,
    site_name: str,
    workbook_name: str,
    api_version: str,
) -> str:
    auth_response = requests.post(
        f"{host}/api/{api_version}/auth/signin",
        json={
            "credentials": {
                "personalAccessTokenName": personal_access_token_name,
                "personalAccessTokenSecret": personal_access_token_secret,
                "site": {
                    "contentUrl": site_name
                }
            }
        },
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    )
    try:
        auth_response.raise_for_status()
    except Exception as e:
        raise Exception(auth_response.text) from e

    # Extract token to use for subsequent calls
    auth_token = auth_response.json()["credentials"]["token"]
    site_id = auth_response.json()["credentials"]["site"]["id"]

    # find the workbook id
    workbook_response = requests.get(
    f"{host}/api/{api_version}/sites/{site_id}/workbooks",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Tableau-Auth": auth_token
        },
        params={
            "filter": f"name:eq:{workbook_name}"
        },
    )

    try:
        workbook_response.raise_for_status()
    except Exception as e:
        raise Exception(workbook_response.text) from e

    workbooks_data = workbook_response.json()
    try:
        workbook_name = workbooks_data["workbooks"]["workbook"][0]["id"]
    except KeyError:
        raise Exception(f"Could not find workbook with name '{workbook_name}'")

    # Refresh the workbook
    refresh_trigger = requests.post(
        f"{host}/api/{api_version}/sites/{site_id}/workbooks/{workbook_name}/refresh",
        json={},
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Tableau-Auth": auth_token
        }
    )
    try:
        refresh_trigger.raise_for_status()
    except Exception as e:
        raise Exception(refresh_trigger.text) from e

    return refresh_trigger.text

