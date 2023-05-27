import argparse
import requests
from zipfile import ZipFile
from io import BytesIO


def extract(content: BytesIO, path) -> None:
    z = ZipFile(content)
    z.extractall(path)

def download(url: str) -> BytesIO:
    zip_files = requests.get(url, stream=True)
    return BytesIO(zip_files.content)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch latest Ergast csv files')
    parser.add_argument('filepath', type=str, help='the location where files will be saved')

    args = parser.parse_args()
    content = download('https://ergast.com/downloads/f1db_csv.zip')
    extract(content, args.filepath)
