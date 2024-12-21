import urllib.request
from pathlib import Path
from urllib.error import URLError
from urllib.error import HTTPError


def download_gh():
    try:
        url = "https://data.gharchive.org/2023-03-01-10.json.gz"
        head = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        dp = Path("Downloads/GitArchives")
        dp.mkdir(parents=True, exist_ok=True)
        file_name = "git_2023_03_01_10.json.gz"
        file_path = dp / file_name
        req = urllib.request.Request(url, headers=head)

        with urllib.request.urlopen(req) as response:
            with open(file_path, mode="wb") as file:
                file.write(response.read())
    except HTTPError as e:
       print(f"HTTP error because of {e.response}")
    except URLError as e:
        print(f"HTTP error because of {e.reason}")
    except:
        print("Some other error occured")
    else:
        return file_path.absolute()

if __name__ == '__main__':
    location = download_gh()
    print(location)
