import pandas as pd
import time
import random
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
import sys
import os
import logging
from datetime import datetime

# Add the html_extractor directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'html_extractor')))
from backend.scripts.selenium.driver_setup_for_scrape import setup_driver_linkedin_singin

# Set up logging
logging.basicConfig(
    filename=f'C:/Users/MikelKulla/Mikel Documents/AI Automation Agency/OUTREACH SYSTEM/log_files/update_company_urls_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)  # Replace with your credentials file
client = gspread.authorize(creds)

# Open the Google Sheet
sheet_id = "1biy0oveY9nHwfruIdy5MC7FLobgfJZ1_OcHlOC6Ijhw"  # Replace with your Google Sheet ID
worksheet_name = "Sheet7"  # Replace with your worksheet name

# Function to get the redirected company URL
def get_company_url(company_id_url, driver):
    """
    Fetches the redirected LinkedIn company URL (name-based) from an ID-based URL.
    
    Parameters:
        company_id_url (str): The company URL to process (e.g., https://www.linkedin.com/company/12345).
        driver: Selenium WebDriver instance.
    
    Returns:
        str: The redirected URL or original URL if an error occurs.
    """
    try:
        if not company_id_url.startswith("https://www.linkedin.com"):
            company_id_url = "https://www.linkedin.com" + company_id_url
        
        driver.get(company_id_url)
        driver.add_human_behavior()
        time.sleep(random.uniform(1, 3))
        
        final_url = driver.current_url
        if "/company/" in final_url:
            logging.info(f"Successfully fetched URL: {company_id_url} -> {final_url}")
            return final_url
        else:
            logging.warning(f"Unexpected URL format for {company_id_url}: {final_url}")
            return company_id_url
    except WebDriverException as e:
        logging.error(f"Error processing {company_id_url}: {str(e)}")
        return company_id_url

def process_csv_and_update_urls(input_csv, output_csv, max_rows=1000, batch_size=10):
    """
    Processes a CSV file, updates 'Regular Company Url' with name-based URLs, and saves progress in real-time.
    
    Parameters:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to save the updated CSV file.
        max_rows (int): Maximum number of rows to process (default: 1000).
        batch_size (int): Number of rows to process before saving (default: 10).
    
    Returns:
        pd.DataFrame: The updated DataFrame.
    """
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_csv) or '.', exist_ok=True)

        # Check if output file exists and use it as input if available
        if os.path.exists(output_csv):
            logging.info(f"Output file '{output_csv}' exists, using it as input")
            input_csv = output_csv
        else:
            logging.info(f"No output file found, using input file '{input_csv}'")

        # Read the CSV file
        logging.info(f"Reading CSV: {input_csv}")
        df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)

        # Check if required column exists
        linkedin_column = "Regular Company Url"
        if linkedin_column not in df.columns:
            raise ValueError(f"Column '{linkedin_column}' not found in CSV")

        # Initialize Processed_URL column
        if 'Processed_URL' not in df.columns:
            logging.info("Processed_URL column not found, creating new column with False values")
            df['Processed_URL'] = False
        else:
            df['Processed_URL'] = df['Processed_URL'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)
            logging.info(f"Found Processed_URL column with {df['Processed_URL'].sum()} processed rows")

        # Initialize the WebDriver
        chromedriver_path = None  # Set to your chromedriver path if needed
        driver = setup_driver_linkedin_singin(chromedriver_path=chromedriver_path, browser="chrome", headless=False)

        try:
            # Process rows in batches
            total_rows = min(len(df), max_rows)
            logging.info(f"Total rows to process: {total_rows}")
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch = df.iloc[start_idx:end_idx]
                
                # Skip already processed rows
                unprocessed_mask = ~batch['Processed_URL']
                if not unprocessed_mask.any():
                    logging.info(f"Batch {start_idx}-{end_idx} already processed, skipping.")
                    continue

                logging.info(f"Processing batch {start_idx}-{end_idx} of {total_rows} rows")
                
                # Process unprocessed rows in the batch
                for idx in batch[unprocessed_mask].index:
                    company_id_url = df.at[idx, linkedin_column]
                    if pd.notna(company_id_url) and "/company/" in company_id_url:
                        logging.info(f"Processing row {idx + 1}/{total_rows}: {company_id_url}")
                        updated_url = get_company_url(company_id_url, driver)
                        df.at[idx, linkedin_column] = updated_url
                        df.at[idx, 'Processed_URL'] = True
                    else:
                        logging.info(f"Skipping invalid or missing URL in row {idx + 1}: {company_id_url}")
                        df.at[idx, 'Processed_URL'] = True  # Mark as processed to skip in future runs
                    
                    # Save progress after each row
                    df.to_csv(output_csv, index=False)
                    logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                    
                    time.sleep(random.uniform(1, 3))

        finally:
            # Clean up WebDriver
            driver.quit()

        # Write to Google Sheet
        output_worksheet_name = f"{worksheet_name}_Updated"
        try:
            output_sheet = client.open_by_key(sheet_id).worksheet(output_worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            output_sheet = client.open_by_key(sheet_id).add_worksheet(title=output_worksheet_name, rows=df.shape[0] + 1, cols=df.shape[1])

        # Convert DataFrame to list of lists for gspread
        output_data = [df.columns.values.tolist()] + df.values.tolist()
        output_sheet.update(output_data)
        logging.info(f"Updated data written to Google Sheet: {output_worksheet_name}")
        print(f"Updated data written to Google Sheet: {output_worksheet_name}")

        return df

    except FileNotFoundError:
        logging.error(f"Input CSV file '{input_csv}' not found.")
        print(f"Error: Input CSV file '{input_csv}' not found.")
        return None
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        print(f"Error processing CSV: {e}")
        return None

# Example usage
if __name__ == "__main__":
    input_path = "data/csv/filtered_url/updated_name/"
    input_csv = "Hospital_Health_Care.csv"
    output_path = "data/csv/filtered_url/updated_name/updated_url/"
    process_csv_and_update_urls(
        input_csv=f"{input_path}Updated_Name_Filtered_{input_csv}",
        output_csv=f"{output_path}Updated_URL_Updated_Name_Filtered_{input_csv}",
        max_rows=2000,
        batch_size=10
    )