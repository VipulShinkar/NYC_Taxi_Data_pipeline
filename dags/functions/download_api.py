import os
import requests

def check_url(url: str) -> bool:
    """
    Checks if a URL is accessible.
    
    :param url: URL to check.
    :return: True if the URL is accessible, False otherwise.
    """
    print(f"Checking URL: {url}")
    try:
        response = requests.head(url)
        if response.status_code == 200:
            print(f"URL {url} is accessible.")
            return True
        else:
            print(f"URL {url} is not accessible. Status code: {response.status_code}")
            return False
    except requests.RequestException as e:
        print(f"Error checking URL {url}: {e}")
        return False

def download_trip_data(taxi_type: str, year: str, month: str, base_dir: str = '/usr/local/airflow/data') -> str:
    """
    Downloads the NYC Taxi trip data for the specified year, month, and taxi type.
    
    :param taxi_type: Type of taxi data to download (yellow, green, fhv, fhvhv).
    :param year: Year of the data to download.
    :param month: Month of the data to download.
    :param base_dir: Base directory where the year-month folder will be created.
    :return: Path to the downloaded file.
    """
    print(f"Starting download for {taxi_type} taxi data for {year}-{month}")

    # Directory for the specific year and month
    download_dir = os.path.join(base_dir, f"{year}-{month}")
    print(f"Download directory: {download_dir}")
    
    # Create the directory if it doesn't exist
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        print(f"Created directory: {download_dir}")
    else:
        print(f"Directory already exists: {download_dir}")
    
    # Define the base URL for trip data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Mapping between trip types and file names
    file_names = {
        "yellow": "yellow_tripdata_",
        "green": "green_tripdata_",
        "fhv": "fhv_tripdata_",
        "fhvhv": "fhvhv_tripdata_"
    }
    
    # Check if the requested trip type is valid
    if taxi_type not in file_names:
        raise ValueError(f"Invalid taxi type: {taxi_type}")
    
    # Construct the URL for the trip data file
    url = f"{base_url}{file_names[taxi_type]}{year}-{month}.parquet"
    print(f"Constructed URL: {url}")

    # Check if the URL is accessible
    if not check_url(url):
        raise Exception(f"URL {url} is not accessible.")
    
    try:
        print(f"Downloading data from {url}")
        # Download the trip data file
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        # Define the file path for saving
        file_path = os.path.join(download_dir, f"{taxi_type}_{year}-{month}.parquet")
        # Save the file
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"Successfully downloaded data to {file_path}")
        return file_path
    except Exception as e:
        print(f"Error downloading trip data from {url}: {e}")
        raise Exception(f"Error downloading trip data: {str(e)}")

# Uncomment below for local testing
# if __name__ == "__main__":
#     import sys
#     taxi_type = sys.argv[1]
#     year = sys.argv[2]
#     month = sys.argv[3]
#     download_trip_data(taxi_type, year, month)
