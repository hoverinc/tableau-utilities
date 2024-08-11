import requests

# Set your Tableau Online details
server = 'https://10ay.online.tableau.com/'
site_id = 'motiveoperations'
token =
workbook_id = '1440313'  # The ID of the workbook you're interested in
api_version = '3.9'

headers = {
    'X-Tableau-Auth': token,
    'Content-Type': 'application/json'
}


# API endpoint to get workbook details
url = f'{server}/api/{api_version}/sites/{site_id}/workbooks/{workbook_id}/content'

response = requests.get(url, headers=headers)
workbook = response.json()

# Check if the response contains embedded data sources
if 'workbook' in workbook and 'connections' in workbook['workbook']:
    datasources = workbook['workbook']['connections']
    for datasource in datasources:
        print(f"Name: {datasource['name']}, Size: {datasource.get('size', 'N/A')} bytes")
else:
    print("No embedded data sources found for this workbook.")