import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import requests
import os

def download_file(url, folder="data"):
    # Create the downloads directory if it doesn't exist
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Extract the file name from the URL
    filename = url.split('/')[-1]
    filepath = os.path.join(folder, filename)

    # Make the request and save the content to a file
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded {filename} to {folder}/")
# URLs and download function as before

def download_files_concurrently(urls, folder="data"):
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all the URLs for downloading
        future_url_dict = {executor.submit(download_file, url, folder): url for url in urls}
        for future in concurrent.futures.as_completed(future_url_dict):
            url = future_url_dict[future]
            try:
                future.result()  # Wait for the download to finish
            except Exception as exc:
                print(f"{url} generated an exception: {exc}")

# Use the concurrent download function
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022"
urls = [f"{base_url}-{i:02}.parquet" for i in range(1, 13)]
download_files_concurrently(urls)
