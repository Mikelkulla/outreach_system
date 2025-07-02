import pandas as pd
import time
import random
import logging
from datetime import datetime
import os
import sys
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from selenium.webdriver.chrome.service import Service
import subprocess
from backend.scripts.selenium.driver_setup_for_scrape import restart_tor, setup_chrome_with_tor, start_tor, stop_tor
from backend.config import Config

# Set up logging
def setup_logging(log_dir=Config.LOG_PATH, log_prefix="find_email"):
    """
    Sets up logging with a timestamped log file in the specified directory.
    Creates the directory if it doesn't exist and falls back to the current directory if there's an error.
    
    Parameters:
        log_dir (str): Directory to save the log file.
        log_prefix (str): Prefix for the log file name.
    
    Returns:
        None
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{log_prefix}_{timestamp}.log")
    
    try:
        os.makedirs(log_dir, exist_ok=True)
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.info(f"Logging initialized to {log_file}")
    except (OSError, PermissionError) as e:
        fallback_log_file = f"{log_prefix}_{timestamp}.log"
        logging.basicConfig(
            filename=fallback_log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logging.error(f"Failed to save log to {log_file}: {e}")
        logging.info(f"Fallback logging initialized to {fallback_log_file}")
        print(f"Error: Could not save log to {log_file}. Using {fallback_log_file} instead.")

# Initialize logging
setup_logging()

def check_stop_signal():
    """
    Check if a stop signal file exists for Step 6.

    Returns:
        bool: True if stop signal file exists, False otherwise.
    """
    stop_file = os.path.join(Config.TEMP_PATH, "stop_step6.txt")
    return os.path.exists(stop_file)

def write_progress(current_row, total_rows):
    """
    Writes the current processing status to a JSON file for Step 6.
    
    Parameters:
        current_row (int): Current row being processed (1-based index, offset-adjusted).
        total_rows (int): Total number of rows to process.
    
    Returns:
        None
    """
    progress_file = os.path.join(Config.TEMP_PATH, "progress_step6.json")
    try:
        with open(progress_file, "w") as f:
            json.dump({"current_row": current_row, "total_rows": total_rows}, f)
        logging.info(f"Updated progress: row {current_row}/{total_rows}")
    except Exception as e:
        logging.error(f"Failed to write progress: {e}")

def find_email(full_name, company_name, driver, tor_process=None, max_retries=2, retry_delay=2):
    """
    Finds an email on Skrapp.io Email Finder for a given name and company with up to 3 retries.
    
    Parameters:
        full_name (str): Full name of the lead.
        company_name (str): Company name or website.
        driver (WebDriver): Selenium WebDriver instance.
        tor_process (subprocess.Popen, optional): Tor process instance.
        max_retries (int): Maximum number of retry attempts.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        tuple: (email, tor_process) where email is the found email address, "search_limit_reached", or None,
               and tor_process is the current Tor process.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"Attempt {attempt} to find email for {full_name} at {company_name}")
            driver.get("https://skrapp.io/email-finder")
            time.sleep(2)
            # Check for access denied (HTTP 403) or request blocked
            if "access to skrapp.io was denied" in driver.page_source.lower() or "http error 403" in driver.page_source.lower():
                logging.warning("Access denied (HTTP 403) detected. Restarting Tor...")
                if tor_process:
                    stop_tor(tor_process)
                tor_process = start_tor()
                time.sleep(5)  # Wait for Tor to initialize
                driver.quit()
                driver = setup_chrome_with_tor()
                driver.get("https://skrapp.io/email-finder")
                time.sleep(2)

            # Wait for the name field to be present
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "name"))
            )

            # Fill full name field
            full_name_field = driver.find_element(By.NAME, "name")
            time.sleep(1.5)
            full_name_field.clear()
            full_name_field.send_keys(full_name)
            time.sleep(1)

            # Wait for company field to be enabled
            company_field = driver.find_element(By.XPATH, "//input[@placeholder='Company name or website']")
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//input[@placeholder='Company name or website']"))
            )

            # If still disabled, try JavaScript to enable
            if company_field.get_attribute("disabled"):
                driver.execute_script("arguments[0].removeAttribute('disabled')", company_field)

            company_field.clear()
            company_field.send_keys(company_name)

            # Handle autocomplete if present
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "MuiAutocomplete-option"))
                )
                autocomplete_option = driver.find_element(By.CLASS_NAME, "MuiAutocomplete-option")
                autocomplete_option.click()
            except:
                pass  # No autocomplete or no options found

            # Click Find Email button
            find_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".MuiTypography-root.css-ekhccu"))
            )
            try:
                find_button.click()
            except:
                driver.execute_script("arguments[0].click();", find_button)

            # Wait for either email result, "no result" message, or "search limit reached" message
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((
                    By.XPATH, "//*[contains(text(),'No result found!') or contains(@class, 'css-1gotp7m') or contains(text(),'Search limit reached!')]"
                ))
            )

            # Check for "Search limit reached!" or "No result found!" message
            try:
                no_result_element = driver.find_element(By.XPATH, "//p[contains(text(), 'No result found!') or contains(text(), 'Search limit reached!')]")
                if no_result_element:
                    message_text = no_result_element.text.strip()
                    if "Search limit reached!" in message_text:
                        logging.warning(f"Search limit reached for {full_name} at {company_name}. Restarting Tor...")
                        if tor_process:
                            stop_tor(tor_process)
                        tor_process = start_tor()
                        time.sleep(5)  # Wait for Tor to initialize
                        driver.quit()
                        driver = setup_chrome_with_tor()
                        if attempt < max_retries:
                            logging.info(f"Retrying after Tor restart for {full_name} at {company_name}")
                            continue
                        else:
                            logging.warning(f"Max retries reached after search limit for {full_name} at {company_name}")
                            return "search_limit_reached", tor_process
                    elif "No result found!" in message_text:
                        logging.info(f"No result found for {full_name} at {company_name}")
                        return None, tor_process
            except:
                pass

            # Try to extract email
            try:
                email_element = driver.find_element(By.CSS_SELECTOR, ".MuiTypography-root.css-1gotp7m")
                email = email_element.text.strip()
                if not email or "@" not in email:
                    email = None
                    logging.info(f"No valid email found for {full_name} at {company_name}")
                else:
                    logging.info(f"Found email for {full_name}: {email}")
            except:
                email = None
                logging.info(f"No email element found for {full_name} at {company_name}")

            return email, tor_process

        except Exception as e:
            logging.error(f"Attempt {attempt} failed for {full_name} at {company_name}: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            continue

    logging.warning(f"Failed to find email for {full_name} at {company_name} after {max_retries} attempts")
    return None, tor_process

def process_csv_and_find_emails(input_csv, output_csv, max_rows=2000, batch_size=50, tor_restart_interval=30, offset=0, delete_no_email=True):
    """
    Processes a CSV file, finds emails using Skrapp.io, and updates the CSV with Email and Email_Found columns.
    Optionally deletes rows where Email_Found is False after processing.
    Restarts Tor every tor_restart_interval rows or when search limit is reached.
    Saves progress after each row and on stop.
    Skips initial rows based on offset.

    Parameters:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to save the updated CSV file.
        max_rows (int): Maximum number of rows to process.
        batch_size (int): Number of rows to process before logging batch completion.
        tor_restart_interval (int): Number of rows after which to restart the Tor process.
        offset (int): Number of rows to skip at the start of the DataFrame.
        delete_no_email (bool): If True, delete rows where Email_Found is False after processing.

    Returns:
        pd.DataFrame: The updated DataFrame or None if an error occurs.
    """
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)

        # Check if output file exists and use it as input if available
        if os.path.exists(output_csv):
            logging.info(f"Output file '{output_csv}' exists, using it as input")
            input_csv = output_csv
        else:
            logging.info(f"Using input file '{input_csv}'")

        # Read the CSV file
        logging.info(f"Reading CSV: {input_csv}")
        df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)

        # Check for required columns
        required_columns = ['Full Name', 'Website']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in CSV")

        # Initialize new columns
        if 'Email' not in df.columns:
            df['Email'] = ""
        if 'Email_Found' not in df.columns:
            df['Email_Found'] = False
        else:
            df['Email_Found'] = df['Email_Found'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)
            logging.info(f"Found Email_Found column with {df['Email_Found'].sum()} processed rows")

        # Initialize progress tracking
        write_progress(0, min(len(df), max_rows))

        # Apply offset
        if offset < 0:
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

        # Initialize Tor and WebDriver
        tor_process = start_tor()
        time.sleep(5)  # Wait for Tor to initialize
        driver = setup_chrome_with_tor()

        try:
            # Process rows in batches for logging purposes
            rows_since_last_tor_restart = 0
            for start_idx in range(offset, offset + total_rows, batch_size):
                if check_stop_signal():
                    logging.info("Stop signal detected, terminating process")
                    write_progress(start_idx + 1, total_rows + offset)
                    df.to_csv(output_csv, index=False)
                    break
                
                end_idx = min(start_idx + batch_size, offset + total_rows)
                batch = df.iloc[start_idx:end_idx]
                
                # Skip already processed rows
                unprocessed_mask = ~batch['Email_Found']
                if not unprocessed_mask.any():
                    logging.info(f"Batch {start_idx}-{end_idx} already processed, skipping.")
                    write_progress(end_idx, total_rows + offset)
                    continue

                logging.info(f"Processing batch {start_idx}-{end_idx} of {offset + total_rows} rows")
                
                # Process unprocessed rows in the batch
                for idx in batch[unprocessed_mask].index:
                    if check_stop_signal():
                        logging.info("Stop signal detected, terminating process")
                        write_progress(idx + 1, total_rows + offset)
                        df.to_csv(output_csv, index=False)
                        break
                    
                    full_name = df.at[idx, 'Full Name']
                    website = df.at[idx, 'Website']
                    
                    # Skip if website is None or empty
                    if not website or website == "None":
                        logging.warning(f"Skipping row {idx + 1}: No valid website for {full_name}")
                        df.at[idx, 'Email'] = ""
                        df.at[idx, 'Email_Found'] = False
                        df.to_csv(output_csv, index=False)
                        logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                        write_progress(idx + 1, total_rows + offset)
                        continue

                    # Check if Tor needs restarting
                    if rows_since_last_tor_restart >= tor_restart_interval:
                        logging.info("Restarting Tor process...")
                        stop_tor(tor_process)
                        tor_process = start_tor()
                        time.sleep(5)
                        driver.quit()
                        driver = setup_chrome_with_tor()
                        rows_since_last_tor_restart = 0
                        logging.info("Tor process restarted and new driver initialized")

                    logging.info(f"Processing row {idx + 1}/{len(df)}: {full_name} at {website}")
                    email, tor_process = find_email(full_name, website, driver, tor_process)
                    
                    # Handle search limit reached
                    if email == "search_limit_reached":
                        logging.info(f"Search limit reached for row {idx + 1}, restarting Tor and retrying")
                        stop_tor(tor_process)
                        tor_process = start_tor()
                        time.sleep(5)
                        driver.quit()
                        driver = setup_chrome_with_tor()
                        rows_since_last_tor_restart = 0
                        # Retry the same row
                        email, tor_process = find_email(full_name, website, driver, tor_process)
                    
                    df.at[idx, 'Email'] = email if email and email != "search_limit_reached" else ""
                    df.at[idx, 'Email_Found'] = bool(email and email != "search_limit_reached")
                    rows_since_last_tor_restart += 1

                    # Save progress after each row
                    df.to_csv(output_csv, index=False)
                    logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                    write_progress(idx + 1, total_rows + offset)
                    
                    time.sleep(random.uniform(1, 2))

        finally:
            # Clean up and reset progress
            try:
                driver.quit()
            except:
                logging.error("Error closing WebDriver")
            if tor_process:
                try:
                    stop_tor(tor_process)
                except:
                    logging.error("Error stopping Tor process")
            logging.info("WebDriver and Tor process closed")
            write_progress(0, 0)

        # Optionally delete rows where Email_Found is False
        if delete_no_email:
            initial_row_count = len(df)
            df = df[df['Email_Found'] == True]
            deleted_rows = initial_row_count - len(df)
            if deleted_rows > 0:
                logging.info(f"Deleted {deleted_rows} rows where Email_Found was False")
                df.to_csv(output_csv, index=False)
                logging.info(f"Saved final DataFrame after deleting rows to {output_csv}")
            else:
                logging.info("No rows with Email_Found False found to delete")

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

# Example usage
if __name__ == "__main__":
    input_path = os.path.join(Config.DATA_CSV_PATH, "domain_about")
    input_csv = "Test_3.csv"
    output_path = os.path.join(Config.DATA_CSV_PATH, "emails")
    process_csv_and_find_emails(
        input_csv=os.path.join(input_path, f"DomainAbout_Updated_Name_Filtered_{input_csv}"),
        output_csv=os.path.join(output_path, f"Emails_DomainAbout_Updated_Name_Filtered_{input_csv}"),
        max_rows=2000,
        batch_size=50,
        delete_no_email=True,
        offset=0
    )