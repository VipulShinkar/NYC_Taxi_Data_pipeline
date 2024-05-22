import os
import requests

def validate_url(url: str) -> bool:
    """
    Validates if the URL is reachable by sending a HEAD request.
    
    :param url: The URL to validate.
    :return: True if the URL is reachable, False otherwise.
    """
    response = requests.head(url)
    return response.status_code == 200

def download_trip_data(taxi_type: str, year: str, month: str, base_dir: str = 'D:\\Airflow\\Data') -> str:
    """
    Downloads the NYC Taxi trip data for the specified year, month, and taxi type.
    
    :param taxi_type: Type of taxi data to download (yellow, green, fhv, fhvhv).
    :param year: Year of the data to download.
    :param month: Month of the data to download.
    :param base_dir: Base directory where the year-month folder will be created.
    :return: Path to the downloaded file.
    """
    # Directory for the specific year and month
    download_dir = os.path.join(base_dir, f"{year}-{month}")
    
    # Create the directory if it doesn't exist
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # Construct the URL based on provided taxi type, year, and month
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
    
    # Validate the URL
    if not validate_url(url):
        raise Exception(f"URL is not reachable: {url}")
    
    file_path = os.path.join(download_dir, f"{taxi_type}_tripdata_{year}-{month}.parquet")

    # Download the file
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            f.write(response.content)
        return file_path
    else:
        raise Exception(f"Failed to download file: {response.status_code} - {response.text}")

# # Example usage
# if __name__ == "__main__":
#     print(download_trip_data('yellow', '2024', '01'))
