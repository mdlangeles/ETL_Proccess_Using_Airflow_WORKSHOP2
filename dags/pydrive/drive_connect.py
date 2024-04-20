from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

my_credentials= '/../credentials_drive.json'

def login():
    
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(my_credentials)


    if gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile(my_credentials)
    else:
        gauth.Authorize()

    credentials = GoogleDrive(gauth)
    return credentials

def upload_csv(path, id_folder):
    credentials=login()
    file= credentials.CreateFile({'parents':[{'kind': 'drive#fileLink', 'id': id_folder}]})
    file['title']= path.split('/')[-1]
    file.SetContentFile(path)
    file.Upload()