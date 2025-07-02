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
import json
from datetime import datetime
from backend.config import Config
from backend.scripts.selenium.driver_setup_for_scrape import setup_driver_linkedin_singin, start_tor, stop_tor
from config.logging import setup_logging
from config.utils import load_csv

# Function to check for stop signal
def check_stop_signal():
    """
    Checks if a stop signal file exists for Step 4.
    
    Returns:
        bool: True if stop is requested, False otherwise.
    """
    stop_file = os.path.join(Config.TEMP_PATH, "stop_step4.txt")
    return os.path.exists(stop_file)

# Function to write progress
def write_progress(current_row, total_rows):
    """
    Writes progress to a JSON file for Step 4.
    
    Parameters:
        current_row (int): Current row being processed (1-based index).
        total_rows (int): Total number of rows to process.
    """
    progress_file = os.path.join(Config.TEMP_PATH, "progress_step4.json")
    try:
        with open(progress_file, "w") as f:
            json.dump({"current_row": current_row, "total_rows": total_rows}, f)
        logging.info(f"Updated progress: row {current_row}/{total_rows}")
    except Exception as e:
        logging.error(f"Failed to write progress: {e}")

# Function to get the redirected company URL
def get_company_url(company_id_url, driver):
    """
    Fetches the redirected LinkedIn company URL (name-based) from an ID-based URL.
    Flags rows for deletion if the redirected URL contains '/school/'.
    
    Parameters:
        company_id_url (str): The company URL to process.
        driver: Selenium WebDriver instance.
    
    Returns:
        tuple: (updated_url, delete_flag)
    """
    try:
        if not company_id_url.startswith("https://www.linkedin.com"):
            company_id_url = "https://www.linkedin.com" + company_id_url
        
        driver.get(company_id_url)
        driver.add_human_behavior()
        time.sleep(random.uniform(1, 3))
        
        final_url = driver.current_url
        if "/school/" in final_url:
            logging.info(f"School URL detected: {company_id_url} -> {final_url}. Marking for deletion.")
            return final_url, True
        elif "/company/unavailable/" in final_url:
            logging.info(f"Unavailable URL detected: {company_id_url} -> {final_url}. Marking for deletion.")
            return final_url, True
        elif "/company/" in final_url:
            logging.info(f"Successfully fetched URL: {company_id_url} -> {final_url}")
            return final_url, False
        else:
            logging.warning(f"Unexpected URL format for {company_id_url}: {final_url}")
            return company_id_url, False
    except WebDriverException as e:
        logging.error(f"Error processing {company_id_url}: {str(e)}")
        return company_id_url, False

def process_csv_and_update_urls(input_csv, output_csv, max_rows=2000, batch_size=10):
    """
    Processes a CSV file, updates 'Regular Company Url' with name-based URLs, deletes school-related rows,
    and saves progress in real-time. Reinitializes WebDriver for each batch. Checks for stop signal after each row.
    
    Parameters:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to save the updated CSV file.
        max_rows (int): Maximum number of rows to process.
        batch_size (int): Number of rows to process before saving and restarting WebDriver.
    
    Returns:
        pd.DataFrame: The updated DataFrame or None if an error occurs.
    """
    try:
        linkedin_column = "Regular Company Url"
        # Ensure output directory exists
        df, resolved_input_csv = load_csv(
            input_csv=input_csv,
            output_csv=output_csv,
            required_columns=[linkedin_column]
        )
        if df is None:
            return None

        # Initialize Processed_URL column
        if 'Processed_URL' not in df.columns:
            logging.info("Processed_URL column not found, creating new column with False values")
            df['Processed_URL'] = False
        else:
            df['Processed_URL'] = df['Processed_URL'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)
            logging.info(f"Found Processed_URL column with {df['Processed_URL'].sum()} processed rows")

        # Initialize progress file
        write_progress(0, min(len(df), max_rows))

        # Process rows in batches
        total_rows = min(len(df), max_rows)
        logging.info(f"Total rows to process: {total_rows}")
        for start_idx in range(0, total_rows, batch_size):
            # Initialize WebDriver for the batch
            driver = setup_driver_linkedin_singin(chromedriver_path=Config.CHROMEDRIVER_PATH, browser="chrome", headless=False)
            
            try:
                end_idx = min(start_idx + batch_size, total_rows)
                batch = df.iloc[start_idx:end_idx]
                
                # Skip already processed rows
                unprocessed_mask = ~batch['Processed_URL']
                if not unprocessed_mask.any():
                    logging.info(f"Batch {start_idx}-{end_idx} already processed, skipping.")
                    write_progress(end_idx, total_rows)
                    continue

                logging.info(f"Processing batch {start_idx}-{end_idx} of {total_rows} rows")
                
                # Process unprocessed rows in the batch
                for idx in batch[unprocessed_mask].index:
                    if check_stop_signal():
                        logging.info(f"Stop signal detected. Saving progress and exiting at row {idx + 1}.")
                        df.to_csv(output_csv, index=False)
                        write_progress(idx + 1, total_rows)
                        return df
                    company_id_url = df.at[idx, linkedin_column]
                    if pd.notna(company_id_url) and "/company/" in company_id_url:
                        # Strip 'about/' from the end of the URL if present
                        if company_id_url.endswith("about/"):
                            company_id_url = company_id_url[:-6]  # Remove 'about/'
                            logging.info(f"Stripped 'about/' from URL for row {idx + 1}: {company_id_url}")

                        logging.info(f"Processing row {idx + 1}/{total_rows}: {company_id_url}")
                        updated_url, delete_row = get_company_url(company_id_url, driver)
                        # Strip 'about/' from the end of the URL if present
                        if company_id_url.endswith("about/"):
                                company_id_url = company_id_url[:-6]  # Remove 'about/'
                                logging.info(f"Stripped 'about/' from URL for row {idx + 1}: {company_id_url}")
                        if delete_row:
                            df = df.drop(index=idx).reset_index(drop=True)
                            logging.info(f"Deleted row {idx + 1} due to school URL: {updated_url}")
                        elif updated_url == f"{company_id_url}/" or updated_url == company_id_url:
                            logging.warning(f"No Change for row {idx + 1} due to same URL format: {updated_url}")
                        else:
                            df.at[idx, linkedin_column] = updated_url
                            df.at[idx, 'Processed_URL'] = True
                    else:
                        logging.warning(f"Skipping invalid or missing URL in row {idx + 1}: {company_id_url}")
                        df.at[idx, 'Processed_URL'] = False
                    
                    # Save progress after each row
                    df.to_csv(output_csv, index=False)
                    logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                    
                    time.sleep(random.uniform(1, 3))
                
                # Update progress after batch
                write_progress(end_idx, total_rows)
            
            finally:
                # Clean up WebDriver after each batch
                try:
                    driver.quit()
                    logging.info(f"WebDriver closed for batch {start_idx}-{end_idx}")
                except Exception as e:
                    logging.error(f"Error closing WebDriver for batch {start_idx}-{end_idx}: {e}")
                # Clean up Tor (optional, uncomment if needed)
                # stop_tor(tor_process)

        # Log final row count and update progress
        logging.info(f"Final row count after processing: {len(df)}")
        write_progress(total_rows, total_rows)
        return df

    except FileNotFoundError:
        logging.error(f"Input CSV file '{input_csv}' not found.")
        print(f"Error: Input CSV file '{input_csv}' not found.")
        return None
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        print(f"Error processing CSV: {e}")
        return None
    finally:
        # Ensure progress is reset on completion or error
        write_progress(0, 0)

# Example usage
if __name__ == "__main__":
    input_csv = os.path.join(Config.UPDATED_NAME_PATH, "Updated_Name_Filtered_Professional Training and Coaching.csv")
    output_csv = os.path.join(Config.UPDATED_URL_PATH, "Updated_URL_Updated_Name_Filtered_Professional Training and Coaching.csv")
    process_csv_and_update_urls(
        input_csv=input_csv,
        output_csv=output_csv,
        max_rows=2000,
        batch_size=10  # Default is 10 to minimize the risk of getting errors from scraping. After each batch the driver is refreshed
    )