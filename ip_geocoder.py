import os
import logging
import requests
import pandas as pd
import time


API_KEY = os.getenv("IPGEOLOCATION_API_KEY")
API_URL = os.getenv("IPGEOLOCATION_API_URL", "https://api.ipgeolocation.io/ipgeo")

# Implementing logging to output to both console and create a log file
logging.basicConfig(
    handlers=[logging.FileHandler("geocode_log.log"), logging.StreamHandler()],
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
)

# Function to get geolocation data for a single IP address with exponential backoff starting at 60 seconds
def geocode_ip(ip_address, retry_count=1, max_retries=5):
    base_delay = 60  # Start with 60 seconds
    params = {"apiKey": API_KEY, "ip": ip_address}
    try:
        response = requests.get(API_URL, params=params)

        if response.status_code == 200:
            logging.info(f"Geocoded IP: {ip_address} successfully.")
            return response.json()
        elif response.status_code == 429:
            if retry_count <= max_retries:
                delay = base_delay * (2 ** (retry_count - 1))  # Exponential backoff starting at 60 seconds
                logging.warning(f"API rate limit reached, retrying in {delay} seconds...")
                time.sleep(delay)
                return geocode_ip(ip_address, retry_count + 1, max_retries)
            else:
                logging.error(f"Max retries reached for IP: {ip_address}.")
                return None
        elif response.status_code == 423:
            logging.error(f"Bogus IP address: {ip_address} is from bogon space (status code 423).")
            return None
        else:
            logging.error(f"Failed to geocode IP: {ip_address} with status code {response.status_code}")
            return None
    except requests.RequestException as e:
        logging.error(f"Network error for IP {ip_address}: {e}")
        return None

# Function to process a list of IPs and capture full response data
def process_ip_list(file_path):
    ip_data = pd.read_csv(file_path, header=None, names=["ip"])
    results = []

    for ip in ip_data["ip"]:
        geo_data = geocode_ip(ip)
        if geo_data:
            results.append(geo_data)  # Appending the full JSON data for each IP

    logging.info(f"Total IPs processed successfully: {len(results)}")
    return pd.DataFrame(results)  # Converting list of JSON objects to DataFrame

# Saving the geolocation data to a new CSV file
if __name__ == "__main__":
    input_file = "ip_addresses.csv"
    output_file = "geocoded_ips.csv"
    
    # Checking if the input file exists
    if not os.path.exists(input_file):
        logging.error(f"The input file {input_file} does not exist.")
    else:
        geolocation_data = process_ip_list(input_file)
        geolocation_data.to_csv(output_file, index=False, encoding='utf-8')
        logging.info(f"Geolocation data saved to {output_file}")
