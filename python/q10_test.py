import pytest
import q10

def test_download_gh():
    download_file = q10.download_gh()

    assert download_file is not None, "File wasn't downloaded"
    assert download_file.exists(), "Downloaded file does not exist"
    assert download_file.with_name("git_2023_03_01_10.json.gz"), "File not downloaded the way you expected"

if __name__ == '__main__':
    test_download_gh()
