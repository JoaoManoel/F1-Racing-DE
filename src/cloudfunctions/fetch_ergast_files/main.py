from zipfile import ZipFile
from io import BytesIO

import requests
from google.cloud import storage

def extract(content: BytesIO, bucket_name: str, folder_name: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    with ZipFile(content, 'r') as zip:
        for filename in zip.namelist():
            print(f'Uploading {filename}...')
            file = zip.read(filename)
            blob = bucket.blob(f'{folder_name}/{filename}')
            blob.upload_from_string(file)


def download(url: str) -> BytesIO:
    zip_files = requests.get(url, stream=True)
    return BytesIO(zip_files.content)

# tratar...
def main(request):
    body = request.get_json()
    print(body)

    bucket_name = body['bucket_name']
    folder_name = body['folder_name']

    content = download('https://ergast.com/downloads/f1db_csv.zip')
    extract(content, bucket_name, folder_name)

    return ('ok', 200)
