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
yellow_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata"
green_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata"
fhv_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata"
yellow_urls = []
green_urls = []
for year in [2019, 2020]:
    for month in range(1, 13):
        yellow_urls.append(f"{yellow_url}_{year}-{month:02}.csv.gz")
        green_urls.append(f"{green_url}_{year}-{month:02}.csv.gz")

fhv_urls = [f"{fhv_url}_2019-{i:02}.csv.gz" for i in range(1, 13)]

#download_files_concurrently(yellow_urls, "data/ny_taxi")
#download_files_concurrently(green_urls, "data/ny_taxi")
#download_files_concurrently(fhv_urls, "data/ny_taxi")
download_file("https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2020-01.csv.gz", "data/ny_taxi")
