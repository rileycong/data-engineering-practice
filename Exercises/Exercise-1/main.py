import aiohttp
import asyncio
import aiofiles
import os
from concurrent.futures import ThreadPoolExecutor
import subprocess

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

async def download_file(session, uri):
    """
    Download a file asynchronously and save it locally.
    """
    file_name = uri.split("/")[-1]
    file_path = f"downloads/{file_name}"

    try:
        async with session.get(uri) as response:
            if response.status == 200:
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(await response.read())
                print(f"Downloaded: {file_name}")
                return file_path
            else:
                print(f"Failed to download {uri}, HTTP status: {response.status}")
                return None
    except Exception as e:
        print(f"Error downloading {uri}: {e}")
        return None

def unzip_file(file_path, extract_dir):
    """
    Unzip a file to the specified directory.
    Runs in a separate thread to avoid blocking.
    """
    try:
        subprocess.run(["unzip", file_path, "-d", extract_dir], check=True)
        os.remove(file_path)
        print(f"Unzipped and removed: {file_path}")
    except Exception as e:
        print(f"Error unzipping {file_path}: {e}")

async def process_files(executor, files):
    """
    Process downloaded files using ThreadPoolExecutor for blocking tasks.
    """
    loop = asyncio.get_running_loop()
    tasks = [       # get the current running event and run unzip with ThreadPoolExecutor
        loop.run_in_executor(executor, unzip_file, file_path, "downloads")
        for file_path in files if file_path
    ]
    await asyncio.gather(*tasks) # gather for concurrent execution, await the program for all tasks to complete

async def main():
    # Create downloads directory if it doesn't exist
    os.makedirs("downloads", exist_ok=True)

    # Create an aiohttp session
    async with aiohttp.ClientSession() as session:
        # Download files concurrently
        download_tasks = [download_file(session, uri) for uri in download_uris]
        downloaded_files = await asyncio.gather(*download_tasks)

    # Use ThreadPoolExecutor for file extraction
    with ThreadPoolExecutor() as executor:
        await process_files(executor, downloaded_files)

    # Remove the __MACOSX folder if it exists
    if os.path.exists("downloads/__MACOSX"):
        with ThreadPoolExecutor() as executor:
            await asyncio.get_running_loop().run_in_executor(executor, subprocess.run, ["rm", "-rf", "downloads/__MACOSX"], {"check": True})

# Test if the files contain data
def test_files():
    for uri in download_uris:
        file_name = uri.split("/")[-1].replace(".zip", ".csv")
        file_path = f"downloads/{file_name}"
        assert os.path.exists(file_path)
        with open(file_path, "r") as f:
            assert len(f.readlines()) > 1

if __name__ == "__main__":
    asyncio.run(main())


# import requests
# import os

# download_uris = [
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
# ]


# def main():
#     # create a directory to store the downloaded files
#     if os.path.exists("downloads") == False:
#         os.makedirs("downloads")
    
#     for uri in download_uris:
#         file_name = uri.split("/")[-1]
#         file_path = f"downloads/{file_name}"
        
#         # download zip file from uri check if the download was successful
#         response = requests.get(uri)
#         if response.status_code == 200 and len(response.content) > 0:
#             with open(file_path, "wb") as f:
#                 f.write(response.content)
#         else:
#             print(f"Failed to download {uri}")
#             continue

#         # unzip the file to csvs
#         os.system(f"unzip {file_path} -d downloads")
#         os.system(f"rm {file_path}")
    
#     # delete _MACOSX folder
#     os.system("rm -rf downloads/__MACOSX")
    
# # test if the files contain data
# def test_files():
#     for uri in download_uris:
#         file_name = uri.split("/")[-1].replace(".zip", ".csv")
#         file_path = f"downloads/{file_name}"
#         assert os.path.exists(file_path)
#         with open(file_path, "r") as f:
#             assert len(f.readlines()) > 1

# if __name__ == "__main__":
#     main()

