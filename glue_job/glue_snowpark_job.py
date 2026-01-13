import sys
import requests
import boto3
from io import BytesIO

#Github Repository Details
GITHUB_REPO = "https://api.github.com/repos/{owner}/{repo}/contents/{folder}"
OWNER = "deacademygit"
REPO = "project-data"
FOLDER = "snowpark-data"
HEADERS = {"Accept": "application/vnd.github.v3+json"}

#S3 Bucket Details
BUCKET_NAME = "adh-snowpark-data-bucket"

#Initialize S3 Client
s3 = boto3.client('s3')

def fetch_github_files():
    """ Fetch file details from Github Folder"""
    url = GITHUB_REPO.format(owner = OWNER, repo = REPO, folder = FOLDER)
    response = requests.get(url, headers = HEADERS)
    
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch files:", respone.text)
        return []
        
def upload_to_s3(file_url, file_name):
    """ Download file from github and upload it to s3"""
    response = requests.get(file_url)
    if response.status_code == 200:
        s3.upload_fileobj(BytesIO(response.content), BUCKET_NAME, file_name)
        print(f"Uploaded {file_name} to S3 bucket {BUCKET_NAME}")
    else:
        print(f"Failed to download {file_name} : {response.text}")

def main():
    files = fetch_github_files()
    for file in files:
        if file["type"] == "file":
            file_url = file["download_url"]
            file_name = file["name"]
            upload_to_s3(file_url,file_name)
if __name__ == "__main__":
    main()
        
