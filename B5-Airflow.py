import requests
import msal
from information import username, password

def request_access_token():
    app_id = '12b9fccf-24f6-4e6c-b23b-8a4f22cfdf5c'
    tenant_id = '40127cd4-45f3-49a3-b05d-315a43a9f033'

    authority_url = 'https://login.microsoftonline.com/' + tenant_id
    scopes = ['https://analysis.windows.net/powerbi/api/.default']

    # Step 1. Generate Power BI Access Token
    client = msal.PublicClientApplication(app_id, authority=authority_url)
    token_response = client.acquire_token_by_username_password(username=username, password=password, scopes=scopes)
    if not 'access_token' in token_response:
        raise Exception(token_response['error_description'])

    access_id = token_response.get('access_token')
    return access_id

access_id = request_access_token()

dataset_id = '560a5c84-f716-4405-9799-b14fe4684b07'
endpoint = f'https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes'
headers = {
    'Authorization': f'Bearer ' + access_id
}

response = requests.post(endpoint, headers=headers)
if response.status_code == 202:
    print('Dataset refreshed')
else:
    print(response.reason)
