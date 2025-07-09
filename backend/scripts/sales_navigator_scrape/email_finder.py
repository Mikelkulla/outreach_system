import uuid
import pandas as pd
import time
import random
import logging
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from backend.scripts.selenium.driver_setup_for_scrape import restart_driver_and_tor, setup_chrome_with_tor, start_tor, stop_tor
from backend.config import Config
from config.job_functions import write_progress, check_stop_signal
from config.utils import load_csv

def find_email(full_name, company_name, driver, tor_process=None, max_retries=2, retry_delay=2):
    """
    Finds an email on Skrapp.io Email Finder for a given name and company with up to 2 retries.

    Parameters:
        full_name (str): Full name of the lead.
        company_name (str): Company name or website.
        driver (WebDriver): Selenium WebDriver instance.
        tor_process (subprocess.Popen, optional): Tor process instance.
        max_retries (int): Maximum number of retry attempts.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        tuple: (email, status, tor_process) where email is the found email address or None,
               status is "found", "no_result", or "search_limit", and tor_process is the current Tor process.
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
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "MuiAutocomplete-option.css-146xefr"))
                )
                autocomplete_option = driver.find_element(By.CLASS_NAME, "MuiAutocomplete-option.css-146xefr")
                autocomplete_option.click()
                time.sleep(random.uniform(0.5, 1))
            except:
                pass  # No autocomplete or no options found

            # Click Find Email button
            find_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".MuiTypography-root.css-1ulaxtk"))
            )
            try:
                time.sleep(random.uniform(1, 3))
                find_button.click()
                time.sleep(random.uniform(3, 5))
            except:
                driver.execute_script("arguments[0].click();", find_button)
                time.sleep(random.uniform(3, 5))

            # Wait for either email result, "no result" message, or "search limit reached" message
            try:
                WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((
                        By.XPATH, "//*[contains(text(), 'No result found!') or contains(@class, 'css-17x39hc') or contains(text(),'Search limit reached!') or contains(@class, 'css-1buerh9')]"
                    ))
                )
                logging.info(f"Found Result element")
            except:
                logging.warning(f"Timeout waiting for result for {full_name} at {company_name}")

            # Check for "Search limit reached!" or "No result found!" message
            try:
                no_result_element = driver.find_element(By.XPATH, "//*[contains(text(), 'No result found!') or contains(text(), 'Search limit reached!') ]")
                if no_result_element:
                    message_text = no_result_element.text.strip()
                    limit_reached_element = driver.find_element(By.CSS_SELECTOR, ".MuiTypography-root.css-15kkj6n")
                    message_text_2 = limit_reached_element.text.strip()
                    if "Search limit reached!" in message_text or "Search limit reached!" in message_text_2:
                        return None, "search_limit", tor_process  # MODIFIED: Return "search_limit" status
                    elif "No result found!" in message_text:
                        logging.info(f"No result found for {full_name} at {company_name}")
                        return None, "no_result", tor_process  # MODIFIED: Return "no_result" status
            except:
                pass

            # Try to extract email
            try:
                logging.info("Getting Email")
                email_element = driver.find_element(By.XPATH, "//p[contains(@class, 'css-17x39hc')]")
                logging.debug(email_element)
                email = email_element.text.strip()
                logging.info(email)

                if not email or "@" not in email:
                    email = None
                    logging.info(f"No valid email found for {full_name} at {company_name}")
                    return None, "no_result", tor_process  # MODIFIED: Return "no_result" status
                else:
                    logging.info(f"Found email for {full_name}: {email}")
                    return email, "found", tor_process  # MODIFIED: Return "found" status
            except:
                logging.info(f"No email element found for {full_name} at {company_name}")
                return None, "no_result", tor_process  # MODIFIED: Return "no_result" status

        except Exception as e:
            logging.error(f"Attempt {attempt} failed for {full_name} at {company_name}: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            continue

    logging.warning(f"Failed to find email for {full_name} at {company_name} after {max_retries} attempts")
    return None, "no_result", tor_process  # MODIFIED: Default to "no_result" on failure

def process_csv_and_find_emails(input_csv, output_csv, max_rows=2000, batch_size=50, tor_restart_interval=30, offset=0, delete_no_email=True, job_id=None, step_id='step6'):
    """
    Processes a CSV file, finds emails using Skrapp.io, and updates the CSV with Email and Status columns.
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
        job_id (str): UUID of the job for progress tracking.
        step_id (str): Identifier for the processing step (default: 'step6').
    Returns:
        pd.DataFrame: The updated DataFrame or None if an error occurs.
    """
    try:
        df, resolved_input_csv = load_csv(
            input_csv=input_csv,
            output_csv=output_csv,
            required_columns=['Full Name', 'Website']
        )
        if df is None:
            return None
        
        # Initialize Status column
        if 'Email' not in df.columns:
            df['Email'] = ""
        if 'Status' not in df.columns:
            df['Status'] = ""
        else:
            logging.info(f"Found Status column with {len(df[df['Status'] == 'found'])} rows with emails found")

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

        # Initialize Tor and WebDriver
        tor_process = start_tor()
        time.sleep(5)  # Wait for Tor to initialize
        driver = setup_chrome_with_tor()

        # Track if process was stopped
        stopped = False

        try:
            # Process rows in batches for logging purposes
            rows_since_last_tor_restart = 0
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

                # Check for unprocessed or search_limit rows
                unprocessed_mask = batch['Status'].isin(['', 'search_limit'])
                if not unprocessed_mask.any():
                    logging.info(f"Batch {start_idx}-{end_idx} already processed, skipping.")
                    write_progress(end_idx, total_rows + offset, job_id, step_id=step_id)
                    continue

                logging.info(f"Processing batch {start_idx}-{end_idx} of {offset + total_rows} rows")

                # Process unprocessed rows in the batch
                for idx in batch[unprocessed_mask].index:
                    if check_stop_signal(step_id):
                        logging.info("Stop signal detected, terminating process")
                        write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id, stop_call=True)
                        df.to_csv(output_csv, index=False)
                        stopped = True
                        break

                    full_name = df.at[idx, 'Full Name']
                    website = df.at[idx, 'Website']

                    # Skip if website is None or empty
                    if not website or website == "None":
                        logging.warning(f"Skipping row {idx + 1}: No valid website for {full_name}")
                        df.at[idx, 'Email'] = ""
                        df.at[idx, 'Status'] = "no_result"  # Set Status to "no_result"
                        df.to_csv(output_csv, index=False)
                        logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                        write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id)
                        continue

                    # Check if Tor needs restarting
                    if rows_since_last_tor_restart >= tor_restart_interval:
                        logging.info("Restarting Tor process...")
                        driver, tor_process = restart_driver_and_tor(driver, tor_process, True, False)
                        rows_since_last_tor_restart = 0
                        logging.info("Tor process restarted and new driver initialized")

                    logging.info(f"Processing row {idx + 1}/{len(df)}: {full_name} at {website}")
                    email, status, tor_process = find_email(full_name, website, driver, tor_process)  # MODIFIED: Receive status

                    # Handle search limit reached
                    if status == "search_limit":
                        logging.info(f"Search limit reached for row {idx + 1}, restarting Tor and retrying")
                        driver, tor_process = restart_driver_and_tor(driver, tor_process, True, False)
                        rows_since_last_tor_restart = 0
                        # Retry the same row
                        email, status, tor_process = find_email(full_name, website, driver, tor_process)  # MODIFIED: Retry with status

                    df.at[idx, 'Email'] = email if email and status != "search_limit" else ""
                    df.at[idx, 'Status'] = status  # MODIFIED: Set Status column
                    rows_since_last_tor_restart += 1

                    # Save progress after each row
                    df.to_csv(output_csv, index=False)
                    logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                    write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id)

                    time.sleep(random.uniform(1, 2))

                if stopped:
                    break

        finally:
            # Clean up and set final status
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

            # Only write final progress if not already stopped
            if not stopped:
                final_status = "stopped" if check_stop_signal(step_id) else "completed"
                final_row = (total_rows + offset) if final_status == "completed" else max(0, min(total_rows, df.index[-1] + 1 if not df.empty else 0))
                write_progress(final_row, total_rows + offset, job_id, step_id=step_id, stop_call=(final_status == "stopped"))

        # Delete only rows with Status == "no_result" if delete_no_email is True
        if delete_no_email and not stopped:
            initial_row_count = len(df)
            df = df[df['Status'] != "no_result"]
            deleted_rows = initial_row_count - len(df)
            if deleted_rows > 0:
                logging.info(f"Deleted {deleted_rows} rows where Status was 'no_result'")
                df.to_csv(output_csv, index=False)
                logging.info(f"Saved final DataFrame after deleting rows to {output_csv}")
            else:
                logging.info("No rows with Status 'no_result' found to delete")

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
            final_row = (total_rows + offset) if final_status == "completed" else max(0, min(total_rows, df.index[-1] + 1 if not df.empty else 0))
            write_progress(final_row, total_rows + offset, job_id, step_id=step_id, stop_call=(final_status == "stopped"))

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
        offset=0,
        job_id=str(uuid.uuid4())
    )