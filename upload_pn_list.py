# USING PYTHON 3
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import datetime
import os
import zipfile

# Initialize today's date, report path and folder ID
today_date = datetime.datetime.now().strftime('%Y%m%d')
new_report_path = 'G:/My Drive/AUTOMATED_REPORTS/PN - Comm/' + today_date + '/'
folder_id = '1D2jnOnagNoIWEdkWR0Zhb6MXs0gPhG4z'

# Get file name
for x in range(len(os.listdir(new_report_path))):
    if os.listdir(new_report_path)[x].endswith('.zip'):
        filename = os.listdir(new_report_path)[x]
try:    
    filepath = new_report_path + filename
except:
    print('Zip file not found')
print('Get filename from get_pn_list.py')
print('File name: ' + filename)
print('File path: ' +  filepath)

# Google auth
gauth = GoogleAuth()
gauth.LoadCredentialsFile("client_secrets.txt")
if gauth.credentials is None:
    gauth.LocalWebserverAuth()
elif gauth.access_token_expired:
    gauth.Refresh()
else:
    gauth.Authorize()
gauth.SaveCredentialsFile("client_secrets.txt")

print('Authentication successful')

# Upload to Google Drive
drive = GoogleDrive(gauth)
file_metadata = {'name' : filename, 'parents': [ folder_id ]}
gfile = drive.CreateFile({'title': filename, 'mimeType':'text/*', "parents": [{"kind": "drive#fileLink","id": folder_id}]})
gfile.SetContentFile(filepath)
gfile.Upload()
print('Upload successful')
