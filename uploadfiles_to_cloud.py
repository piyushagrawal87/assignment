import os
import sys
from os import listdir
from oauth2client.service_account import ServiceAccountCredentials
from gcloud import storage

#Global variables
key_location = "C:/Users/Admin/Documents/LoyaltyOne Assignment/key/My First Project-fb57c15888fd.json"
project_id = 'causal-howl-182213'
bucket_id = 'practicebucketpiyush'
folder_location = r"C:\Users\Admin\Documents\LoyaltyOne Assignment\input"


def getCredentials(key_location):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(key_location)
    return credentials

def getClient(project_id, credentials):
    client = storage.Client(project=project_id,credentials=credentials)
    return client

def processFile(file, bucket):
    flag = True
    blob = bucket.blob(file)
    try:
        blob.upload_from_filename(filename=folder_location +'\\'+ file)
    except:
        flag = False
    return flag
    
if __name__ == "__main__":
    arg1 = sys.argv[1]
    if len(sys.argv) > 1:
        #Check of file exists
        if(os.path.isfile(folder_location + '\\' + arg1)):
            credentials = getCredentials(key_location)
            client = getClient(project_id, credentials)
            bucket = client.get_bucket(bucket_id)
            for obj in bucket.list_blobs():
                if obj.name == arg1:
                    print('File Exists in the cloud. It will be overwritten')
            status = processFile(arg1,bucket)
            if status:
                print(arg1+" processed")
            else:
                print(arg1+" failed")
    else:
        #folder exists
        if(os.path.isdir(folder_location)):
            files = [f for f in listdir(folder_location)]
            if not files:
                print("Empty Folder")
            else:
                credentials = getCredentials(key_location)
                client = getClient(project_id, credentials)
                bucket = client.get_bucket(bucket_id)
                print('Processing following files')
                for f in files:
                    status = processFile(f,bucket)
                    if status:
                        print(f+" processed")
                    else:
                        print(f+" failed")
        else:
            print("Folder Location doesn't exists. Exiting...")
            exit