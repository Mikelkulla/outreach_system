import json
import uuid
import pandas as pd
import time
import random
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
import sys
import os
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote
import tldextract
from backend.scripts.selenium.driver_setup_for_scrape import kill_chrome_processes, setup_driver, setup_driver_linkedin_singin
from backend.config import Config  # Import Config for path management
from config.logging import setup_logging
from config.job_functions import write_progress, check_stop_signal
from config.utils import load_csv

def extract_company_info(first_name, company_url, index, driver, max_retries=3, retry_delay=3):
    """
    Extracts the About section text and website domain from a LinkedIn company URL with retry logic and LinkedIn login handling.

    Args:
        first_name (str): The first name of the lead.
        company_url (str): The LinkedIn company URL.
        index (int): Row index (for logging/debugging).
        driver (webdriver): Selenium WebDriver instance.
        max_retries (int): Maximum number of retry attempts.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        tuple: (dict, webdriver) - Contains result dictionary (First Name, Company URL, Website, About_Text, Row) and updated driver.
    """
    result = {
        "First Name": first_name,
        "Company URL": company_url,
        "Website": "None",
        "About_Text": "None",
        "Row": index + 1
    }

    if not company_url or "linkedin.com/company/" not in company_url:
        logging.warning(f"Skipping invalid or empty URL for {first_name}: {company_url}")
        return result, driver

    for attempt in range(1, max_retries + 1):
        try:
            driver.get(company_url)
            time.sleep(random.uniform(2, 4))  # Allow time for JavaScript content to load

            # Check for login page
            if "linkedin.com/login" in driver.current_url:
                logging.error(f"Login required for {company_url}. Attempting re-login...")
                driver.quit()
                time.sleep(2)
                driver = setup_driver_linkedin_singin()
                time.sleep(2)
                driver.get(company_url)
                time.sleep(random.uniform(2, 4))

            wait = WebDriverWait(driver, 10)

            # Extract website
            website = "None"
            try:
                website_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//section[contains(@class, "org-about-module__margin-bottom")]//dl//dt[contains(., "Website")]/following-sibling::dd[1]/a | //a[contains(@href, "http") and ancestor::dd[contains(., "Website")]]'))
                )
                raw_href = website_element.get_attribute("href").strip() if website_element else "None"

                if "linkedin.com/redir/redirect?" in raw_href:
                    parsed_url = urlparse(raw_href)
                    query_params = parse_qs(parsed_url.query)
                    encoded_url = query_params.get("url", [""])[0]
                    decoded_url = unquote(encoded_url)
                else:
                    decoded_url = raw_href

                if decoded_url != "None":
                    ext = tldextract.extract(decoded_url)
                    website = f"{ext.domain}.{ext.suffix}" if ext.domain and ext.suffix else "None"
                logging.info(f"Extracted website for {first_name}: {website}")
            except (TimeoutException, Exception) as e:
                logging.warning(f"Failed to extract website for {first_name}: {e}")
                website = "None"

            # Extract About section text
            about_text = "None"
            try:
                about_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//section[contains(@class, "org-about-module__margin-bottom")]//p[contains(@class, "break-words")] | //section[contains(@class, "org-about-module")]//p'))
                )
                about_text = about_element.text.strip() if about_element else "None"
                logging.info(f"Extracted About_Text for {first_name}: {about_text[:50]}...")
            except (TimeoutException, Exception) as e:
                logging.warning(f"Failed to extract About_Text for {first_name}: {e}")
                about_text = "None"

            result.update({
                "Website": website,
                "About_Text": about_text
            })
            return result, driver

        except Exception as e:
            logging.error(f"[Attempt {attempt}] Error processing {company_url} for {first_name}: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
                driver.quit()
                time.sleep(3)
                driver = setup_driver_linkedin_singin()
                time.sleep(4)
            else:
                return result, driver

    return result, driver

def process_csv_and_extract_info(input_csv, output_csv, max_rows=2000, batch_size=50, delete_no_website=True, offset=0, job_id=None, step_id='step5'):
    """
    Processes a CSV file, extracts About section text and website domain from Regular Company Url,
    and saves progress in real-time. Initializes and quits WebDriver for each row.
    Optionally deletes rows where no website was found. Includes an offset to skip initial rows.

    Parameters:
    -----------
    input_csv (str): Path to the input CSV file.
    output_csv (str): Path to save the updated CSV file.
    max_rows (int): Maximum number of rows to process.
    batch_size (int): Number of rows to process before logging batch completion.
    delete_no_website (bool): If True, delete rows where Website is "None" after processing.
    offset (int): Number of rows to skip at the start of the DataFrame.
    job_id (str): UUID of the job for progress tracking.
    step_id (str): step4 for job 4

    Returns:
    --------
    pd.DataFrame: The updated DataFrame or None if an error occurs.
    """
    try:
        # Use load_csv function
        linkedin_column = 'Regular Company Url'
        df, resolved_input_csv = load_csv(
            input_csv=input_csv,
            output_csv=output_csv,
            required_columns=[linkedin_column]
        )
        if df is None:
            return None

        # Initialize new columns
        if 'Website' not in df.columns:
            df['Website'] = "None"
        if 'About_Text' not in df.columns:
            df['About_Text'] = "None"
        if 'Processed_About_Website' not in df.columns:
            df['Processed_About_Website'] = False
        else:
            # Ensure Processed_About_Website is boolean
            df['Processed_About_Website'] = df['Processed_About_Website'].map({'True': True, 'False': False, True: True, False: False}).fillna(False) #.astype(bool)
            logging.info(f"Found Processed_About_Website column with {df['Processed_About_Website'].sum()} processed rows")

        # Initialize progress tracking
        write_progress(0, min(len(df), max_rows), job_id, step_id=step_id)

        # Apply offset to skip rows
        if offset < 0:
            logging.error("Offset cannot be negative")
            raise ValueError("Offset cannot be negative")
        if offset >= len(df):
            logging.info(f"Offset {offset} is greater than or equal to DataFrame length {len(df)}, no rows to process")
            return df

        # Adjust max_rows considering the offset
        total_rows = min(len(df) - offset, max_rows)
        if total_rows <= 0:
            logging.info(f"No rows to process after applying offset {offset} and max_rows {max_rows}")
            return df

        logging.info(f"Total rows to process after offset {offset}: {total_rows}")

        # Track if process was stopped
        stopped = False

        try:
            # Process rows in batches for logging purposes
            for start_idx in range(offset, offset + total_rows, batch_size):
                # Check for stop signal before processing batch
                if check_stop_signal(step_id):
                    logging.info("Stop signal detected, terminating process")
                    write_progress(start_idx + 1, total_rows + offset, job_id, step_id=step_id, stop_call=True)
                    df.to_csv(output_csv, index=False)
                    stopped = True
                    break

                end_idx = min(start_idx + batch_size, offset + total_rows)
                batch = df.iloc[start_idx:end_idx]
                
                # Skip already processed rows
                unprocessed_mask = ~batch['Processed_About_Website']
                if not unprocessed_mask.any():
                    logging.info(f"Batch {start_idx}-{end_idx} already processed, skipping.")
                    write_progress(end_idx, total_rows + offset, job_id, step_id=step_id)
                    continue

                logging.info(f"Processing batch {start_idx}-{end_idx} of {offset + total_rows} rows")
                
                # Initialize WebDriver for the batch
                driver = None
                try:
                    # kill_chrome_processes()
                    driver = setup_driver_linkedin_singin()
                    time.sleep(3)

                    # Process unprocessed rows in the batch
                    for idx in batch[unprocessed_mask].index:
                        if check_stop_signal(step_id):
                            logging.info(f"Stop signal detected, terminating process")
                            write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id, stop_call=True)
                            df.to_csv(output_csv, index=False)
                            stopped = True
                            break

                        # Check for session expiration
                        if driver.current_url.startswith("https://www.linkedin.com/login"):
                            logging.warning(f"Session expired for row {idx + 1}. Reinitializing WebDriver.")
                            driver.quit()
                            time.sleep(3)
                            driver = setup_driver_linkedin_singin()
                            time.sleep(3)

                        first_name = df.at[idx, 'First Name']
                        company_url = df.at[idx, linkedin_column]
                        if pd.notna(company_url) and "/company/" in company_url:
                            logging.info(f"Processing row {idx + 1}/{len(df)}: {company_url}")
                            
                            # Clean and append /about to URL
                            if company_url:
                                company_url = company_url.rstrip('/').replace('/about', '')
                                company_url = f"{company_url}/about"
                                logging.info(f"Modified URL for row {idx + 1}: {company_url}")

                            # Call extract_company_info and capture both result and driver
                            result, driver = extract_company_info(first_name, company_url, idx, driver)
                            df.at[idx, 'Website'] = result['Website']
                            df.at[idx, 'About_Text'] = result['About_Text']
                            df.at[idx, 'Processed_About_Website'] = result['Website'] != "None"
                        else:
                            logging.warning(f"Skipping invalid or missing URL in row {idx + 1}: {company_url}")
                            df.at[idx, 'Processed_About_Website'] = False
                        
                        # Save progress after each row
                        df.to_csv(output_csv, index=False)
                        logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                        write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id)

                        time.sleep(random.uniform(1, 2))
                except Exception as e:
                    logging.error(f"Failed to process batch {start_idx + 1}-{end_idx}: {e}")
                finally:
                    # Clean up WebDriver
                    if driver is not None:
                        try:
                            driver.quit()
                            logging.info("WebDriver closed successfully")
                        except Exception as e:
                            logging.error(f"Error closing WebDriver: {e}")
                        # # Ensure all Chrome processes are terminated
                        # kill_chrome_processes()
                        time.sleep(2)  # Wait to ensure processes are fully terminated
                
                if stopped:
                    break

        finally:
            # Clean up WebDriver after batch
            if driver is not None:
                try:
                    driver.quit()
                except:
                    logging.error("Error closing WebDriver")
                logging.info("WebDriver process closed")
            # kill_chrome_processes()
            # Only write final progress if not already stopped
            if not stopped:
                final_status = "stopped" if check_stop_signal(step_id) else "completed"
                final_row = total_rows if final_status == "completed" else max(0, min(total_rows, df.index[-1] + 1 if not df.empty else 0))
                write_progress(final_row, total_rows + offset, job_id, step_id=step_id, stop_call=(final_status == "stopped"))

        # Optionally delete rows where Website is "None"
        if delete_no_website and not stopped:
            initial_row_count = len(df)
            df = df[df['Website'] != "None"]
            deleted_rows = initial_row_count - len(df)
            if deleted_rows > 0:
                logging.info(f"Deleted {deleted_rows} rows where Website was 'None'")
                df.to_csv(output_csv, index=False)
                logging.info(f"Saved final DataFrame after deleting rows to {output_csv}")
            else:
                logging.info("No rows with Website 'None' found to delete")

        logging.info(f"Final row count after processing: {len(df)}")
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
        if not stopped:
            final_status = "stopped" if check_stop_signal(step_id) else "completed"
            final_row = total_rows if final_status == "completed" else max(0, min(total_rows, df.index[-1] + 1 if not df.empty else 0))
            write_progress(final_row, total_rows + offset, job_id, step_id=step_id, stop_call=(final_status == "stopped"))

# Example usage
if __name__ == "__main__":
    input_path = os.path.join(Config.DATA_CSV_PATH, "updated_name")
    input_csv = "Test_3.csv"
    output_path = os.path.join(Config.DATA_CSV_PATH, "domain_about")
    process_csv_and_extract_info(
        input_csv=os.path.join(input_path, f"Updated_Name_Filtered_{input_csv}"),
        output_csv=os.path.join(output_path, f"DomainAbout_Updated_Name_Filtered_{input_csv}"),
        max_rows=2000,
        batch_size=50,
        delete_no_website=True,
        offset=0,
        job_id=str(uuid.uuid4())
    )