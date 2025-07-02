import pandas as pd
import time
import random
import logging
import os
import uuid
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib3.exceptions import NewConnectionError as Urllib3NewConnectionError
from backend.scripts.selenium.driver_setup_for_scrape import start_tor, stop_tor, setup_chrome_with_tor
from backend.config import Config
from config.logging import setup_logging
from config.job_functions import write_progress, check_stop_signal
from config.utils import load_csv
from selenium.common.exceptions import WebDriverException


def verify_email_neverbounce():
    pass

def verify_email_scrapp(email, driver, tor_process=None, max_retries=2, retry_delay=2):
    """
    Verifies an email on Skrapp.io Email Verifier with up to 2 retries.

    Parameters:
        email (str): Email address to verify.
        driver (WebDriver): Selenium WebDriver instance.
        tor_process (subprocess.Popen, optional): Tor process instance.
        max_retries (int): Maximum number of retry attempts.
        retry_delay (int): Delay between retries in seconds.

    Returns:
        tuple: (email_status, syntax_status, server_status, tor_process) where statuses are "Valid", "Invalid", "Unknown", "Catch-All", "Pending", or None,
               and tor_process is the current Tor process.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"Attempt {attempt} to verify email: {email}")
            driver.get("https://skrapp.io/email-verifier")
            time.sleep(2)

            # Check for access denied (HTTP 403) or request blocked
            if "access to skrapp.io was denied" in driver.page_source.lower() or "http error 403" in driver.page_source.lower():
                logging.warning("Access denied (HTTP 403) detected. Restarting Tor...")
                return None, None, None, tor_process
                # try:
                #     driver.quit()
                # except WebDriverException as e:
                #     logging.warning(f"Failed to close WebDriver on HTTP 403: {e}")
                # if tor_process:
                #     stop_tor(tor_process)
                # tor_process = start_tor()
                # time.sleep(9)
                # driver = setup_chrome_with_tor()
                # driver.get("https://skrapp.io/email-verifier")
                # time.sleep(2)

            # Wait for the email field to be present
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "email"))
            )

            # Fill email field
            time.sleep(0.5)
            email_field = driver.find_element(By.NAME, "email")
            time.sleep(0.5)
            email_field.clear()
            email_field.send_keys(email)
            time.sleep(1)

            # Click Verify Email button
            verify_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".MuiTypography-root.css-ekhccu"))
            )
            try:
                verify_button.click()
            except:
                driver.execute_script("arguments[0].click();", verify_button)
            time.sleep(13)

            # Wait for verification result or error message
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((
                    By.XPATH, "//*[contains(@class, 'MuiBox-root css-pf10ra') or contains(text(), 'Too many requests sent')]"
                    ))
                )
            except:
                logging.warning(f"Timeout waiting for verification result for {email}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    continue
                return None, None, None, tor_process

            # Check for "Too many requests sent" message
            try:
                limit_element = driver.find_element(By.XPATH, "//div[contains(., 'Too many requests sent')]")
                if limit_element:
                    logging.warning(f"Too many requests sent for {email}. Restarting Tor...")
                    # try:
                    #     driver.quit()
                    # except WebDriverException as e:
                    #     logging.warning(f"Failed to close WebDriver on rate limit: {e}")
                    # if tor_process:
                    #     stop_tor(tor_process)
                    # tor_process = start_tor()
                    # time.sleep(15)
                    # driver = setup_chrome_with_tor()
                    # if attempt < max_retries:
                    #     logging.info(f"Retrying after Tor restart for {email}")
                    #     continue
                    # else:
                    #     logging.warning(f"Max retries reached after too many requests for {email}")
                    return None, None, None, tor_process
            except:
                pass

            # Check for verification messages
            email_status = None
            syntax_status = None
            server_status = None

            # Check for valid, invalid, or catch-all messages
            verification_messages = [
                ("Valid",       "//div[contains(@class, 'MuiBox-root css-1pmn8ky')]//p[contains(text(), 'Email is valid and successfully reachable.')]"),
                ("Invalid",     "//div[contains(@class, 'MuiBox-root css-pp4hnw')]//p[contains(text(), 'Email is invalid and cannot be reached.')]"),
                ("Catch-All",   "//div[contains(@class, 'MuiBox-root css-1pmn8ky')]//p[contains(text(), 'Email is reachable, but it')]"),
                ("Catch-All",   "//div[contains(@class, 'MuiBox-root css-1b3tdk3')]//p[contains(text(), 'Email is reachable, but it')]")
            ]

            for status, xpath in verification_messages:
                try:
                    driver.find_element(By.XPATH, xpath)
                    email_status = status
                    logging.info(f"{status} email confirmation found for {email}")
                    break
                except Exception as e:
                    logging.info(f"Error: {e}")
                    continue

            if not email_status:
                logging.info(f"No valid, invalid, or catch-all email confirmation message found for {email}")
                return None, None, None, tor_process

            # Extract Email Status
            try:
                email_status_element = driver.find_element(By.XPATH, "//div[contains(@class, 'sc-fYsHOw dMtDzI')]//p[contains(text(), 'Email Status')]/following-sibling::div[contains(@class, 'sc-Qotzb dnVSuG')]//span")
                email_status_text = email_status_element.text.strip()
                email_status = email_status_text if email_status_text in ["Valid", "Invalid", "Catch-All", "Unknown", "Pending"] else email_status
            except:
                logging.info(f"Email Status not found for {email}")

            # Extract Email Syntax Format
            try:
                syntax_status_element = driver.find_element(By.XPATH, "//div[contains(@class, 'sc-fYsHOw dMtDzI')]//p[contains(text(), 'Email Syntax')]/following-sibling::div[contains(@class, 'sc-iQQCXo dXoCvN')]")
                syntax_status = syntax_status_element.text.strip()
                if syntax_status not in ["Valid", "Invalid"]:
                    syntax_status = None
            except:
                logging.info(f"Email Syntax Format not found for {email}")

            # Extract Mailbox Server Status
            try:
                server_status_element = driver.find_element(By.XPATH, "//div[contains(@class, 'sc-fYsHOw dMtDzI')]//p[contains(text(), 'Mailbox Server')]/following-sibling::div[contains(@class, 'sc-iQQCXo dXoCvN')]")
                server_status = server_status_element.text.strip()
                if server_status not in ["Valid", "Invalid"]:
                    server_status = None
            except:
                logging.info(f"Mailbox Server Status not found for {email}")

            logging.info(f"Verification results for {email}: Email Status={email_status}, Syntax Format={syntax_status}, Server Status={server_status}")
            return email_status, syntax_status, server_status, tor_process

        except (Urllib3NewConnectionError, WebDriverException) as e:
            logging.error(f"Attempt {attempt} failed for {email}: {e}")
            if attempt < max_retries:
                # logging.info(f"Resetting WebDriver due to connection error for {email}")
                # try:
                #     driver.quit()
                # except WebDriverException as quit_e:
                #     logging.warning(f"Failed to close WebDriver on connection error: {quit_e}")
                # driver = setup_chrome_with_tor()
                time.sleep(retry_delay)
                continue
            else:
                logging.warning(f"Max retries reached for {email} after connection error")
                return None, None, None, tor_process

    logging.warning(f"Failed to verify email {email} after {max_retries} attempts")
    return None, None, None, tor_process

def process_csv_and_verify_emails(input_csv, output_csv, max_rows=2000, batch_size=50, tor_restart_interval=30, offset=0, delete_invalid=True, job_id=None, step_id='step7'):
    """
    Processes a CSV file, verifies emails using Skrapp.io, and updates the CSV with Email_Processed, Email Status,
    Email Syntax Format, Mailbox Server Status, and Verified_Email columns.
    Skips rows already marked as processed (Email_Processed=True).
    Optionally deletes rows where Email Status is "Invalid" after processing.
    Restarts Tor every tor_restart_interval rows among unprocessed rows.
    Saves progress after each row.
    Skips initial rows based on offset.

    Parameters:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to save the updated CSV file.
        max_rows (int): Maximum number of rows to process.
        batch_size (int): Number of rows to process before logging batch completion.
        tor_restart_interval (int): Number of unprocessed rows after which to restart the Tor process.
        offset (int): Number of rows to skip at the start of the DataFrame.
        delete_invalid (bool): If True, delete rows where Email Status is "Invalid" after processing.
        job_id (str): UUID of the job for progress tracking.
        step_id (str): Identifier for the processing step (default: 'step7').

    Returns:
        pd.DataFrame: The updated DataFrame or None if an error occurs.
    """
    try:
        df, resolved_input_csv = load_csv(
            input_csv=input_csv,
            output_csv=output_csv,
            required_columns=['Email']
        )
        if df is None:
            return None

        # Initialize columns
        for col in ['Email Status', 'Email Syntax Format', 'Mailbox Server Status', 'Email_Processed', 'Verified_Email']:
            if col not in df.columns:
                df[col] = "" if col not in ['Email_Processed', 'Verified_Email'] else False
        df['Email_Processed'] = df['Email_Processed'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)
        df['Verified_Email'] = df['Verified_Email'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)
        logging.info(f"Found Email_Processed column with {df['Email_Processed'].sum()} processed rows")

        # Initialize progress tracking
        write_progress(0, min(len(df), max_rows), job_id, step_id=step_id)

        # Apply offset
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
        time.sleep(7)
        driver = setup_chrome_with_tor()

        # Track if process was stopped
        stopped = False

        try:
            rows_since_last_tor_restart = 0
            for start_idx in range(offset, offset + total_rows, batch_size):
                if check_stop_signal(step_id):
                    logging.info("Stop signal detected, terminating process")
                    write_progress(start_idx + 1, total_rows + offset, job_id, step_id=step_id, stop_call=True)
                    df.to_csv(output_csv, index=False)
                    stopped = True
                    break

                end_idx = min(start_idx + batch_size, offset + total_rows)
                batch = df.iloc[start_idx:end_idx]

                # Check for unprocessed rows
                unprocessed_mask = ~batch['Email_Processed']
                if not unprocessed_mask.any():
                    logging.info(f"Batch {start_idx}-{end_idx} already processed, skipping.")
                    write_progress(end_idx, total_rows + offset, job_id, step_id=step_id)
                    continue

                logging.info(f"Processing batch {start_idx}-{end_idx} of {offset + total_rows} rows")

                for idx in batch[unprocessed_mask].index:
                    if check_stop_signal(step_id):
                        logging.info("Stop signal detected, terminating process")
                        write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id, stop_call=True)
                        df.to_csv(output_csv, index=False)
                        stopped = True
                        break

                    email = df.at[idx, 'Email'].strip()
                    
                    if not email or "@" not in email:
                        logging.info(f"Skipping row {idx + 1}: Invalid or empty email")
                        df.at[idx, 'Email_Processed'] = True
                        df.at[idx, 'Email Status'] = "Invalid"
                        df.at[idx, 'Email Syntax Format'] = None
                        df.at[idx, 'Mailbox Server Status'] = None
                        df.at[idx, 'Verified_Email'] = False
                        df.to_csv(output_csv, index=False)
                        logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                        write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id)
                        continue

                    # Check if Tor needs restarting
                    if rows_since_last_tor_restart >= tor_restart_interval:
                        logging.info("Restarting Tor process...")
                        try:
                            driver.quit()
                        except WebDriverException as e:
                            logging.warning(f"Failed to close WebDriver on periodic Tor restart: {e}")
                        stop_tor(tor_process)
                        time.sleep(2)
                        tor_process = start_tor()
                        time.sleep(10)
                        driver = setup_chrome_with_tor()
                        rows_since_last_tor_restart = 0
                        logging.info("Tor process restarted and new driver initialized")

                    logging.info(f"Processing row {idx + 1}/{len(df)}: {email}")
                    email_status, syntax_status, server_status, tor_process = verify_email_scrapp(email, driver, tor_process)
                    # Handle rate limit
                    if email_status is None:
                        logging.info(f"Rate limit reached for row {idx + 1}, restarting Tor and retrying")
                        stop_tor(tor_process)
                        tor_process = start_tor()
                        time.sleep(5)
                        driver.quit()
                        driver = setup_chrome_with_tor()
                        rows_since_last_tor_restart = 0
                        email_status, syntax_status, server_status, tor_process = verify_email_scrapp(email, driver, tor_process)

                    # Update DataFrame with statuses
                    df.at[idx, 'Email Status'] = email_status
                    df.at[idx, 'Email Syntax Format'] = syntax_status
                    df.at[idx, 'Mailbox Server Status'] = server_status
                    df.at[idx, 'Email_Processed'] = email_status is not None
                    df.at[idx, 'Verified_Email'] = (email_status == "Valid" and syntax_status == "Valid" and server_status == "Valid")

                    rows_since_last_tor_restart += 1
                    
                    # Reset WebDriver after each verification to prevent stale sessions
                    try:
                        driver.quit()  # Close WebDriver to avoid memory leaks or stale sessions
                    except WebDriverException as e:
                        logging.warning(f"Failed to close WebDriver after verification: {e}")  # Log if quit fails
                    driver = setup_chrome_with_tor()  # Reinitialize WebDriver for next email
                    logging.info(f"WebDriver reset for next email verification")  # Log reset for debugging

                    df.to_csv(output_csv, index=False)
                    logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                    write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id)
                    time.sleep(random.uniform(1, 2))

                if stopped:
                    break

        finally:
            try:
                driver.quit()
            except WebDriverException as e:
                logging.warning(f"Failed to close WebDriver in final cleanup: {e}")
            if tor_process:
                stop_tor(tor_process)
            logging.info("WebDriver and Tor process closed")

            if not stopped:
                final_status = "stopped" if check_stop_signal(step_id) else "completed"
                final_row = total_rows if final_status == "completed" else max(0, min(total_rows, df.index[-1] + 1 if not df.empty else 0))
                write_progress(final_row, total_rows + offset, job_id, step_id=step_id, stop_call=(final_status == "stopped"))

        if delete_invalid and not stopped:
            initial_row_count = len(df)
            df = df[df['Email Status'] != "Invalid"]
            deleted_rows = initial_row_count - len(df)
            if deleted_rows > 0:
                logging.info(f"Deleted {deleted_rows} rows where Email Status was 'Invalid'")
                df.to_csv(output_csv, index=False)
                logging.info(f"Saved final DataFrame after deleting rows to {output_csv}")
            else:
                logging.info("No rows with Email Status 'Invalid' found to delete")

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

if __name__ == "__main__":
    setup_logging()
    input_path = os.path.join(Config.DATA_CSV_PATH, "emails")
    input_csv = "Legal_Services.csv"
    output_path = os.path.join(Config.DATA_CSV_PATH, "verified")
    process_csv_and_verify_emails(
        input_csv=os.path.join(input_path, f"Emails_DomainAbout_Updated_Name_Filtered_{input_csv}"),
        output_csv=os.path.join(output_path, f"Verified_Emails_DomainAbout_Updated_Name_Filtered_{input_csv}"),
        max_rows=2000,
        batch_size=50,
        tor_restart_interval=5,
        offset=0,
        delete_invalid=True,
        job_id=str(uuid.uuid4()),
        step_id='step7'
    )