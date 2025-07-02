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
from config.job_functions import write_progress
from config.utils import load_csv
from selenium.common.exceptions import WebDriverException

def check_stop_signal():
    """
    Check if a stop signal file exists for Step 7.

    Returns:
        bool: True if stop signal file exists, False otherwise.
    """
    stop_file = os.path.join(Config.TEMP_PATH, "stop_step7.txt")
    return os.path.exists(stop_file)

def verify_email(email, driver, tor_process=None, max_retries=2, retry_delay=2):
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
            driver.get("https://skrapp.io/email-verifier")  # Load Skrapp.io email verifier page
            time.sleep(2)

            # Check for access denied (HTTP 403) or request blocked
            if "access to skrapp.io was denied" in driver.page_source.lower() or "http error 403" in driver.page_source.lower():
                logging.warning("Access denied (HTTP 403) detected. Restarting Tor...")  # Indicates website blocked our request
                try:
                    driver.quit()  # Close WebDriver before stopping Tor to prevent stale sessions
                except WebDriverException as e:
                    logging.warning(f"Failed to close WebDriver on HTTP 403: {e}")  # Log if quit fails
                if tor_process:
                    stop_tor(tor_process)  # Stop existing Tor process if running
                tor_process = start_tor()  # Start new Tor process to get a new IP
                time.sleep(9)  # Increased delay to ensure Tor stabilizes
                driver = setup_chrome_with_tor()  # Initialize new WebDriver with Tor
                driver.get("https://skrapp.io/email-verifier")  # Reload verifier page
                time.sleep(2)  # Brief pause to ensure page loads

            # Wait for the email field to be present
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "email"))
            )  # Ensure email input field is loaded before proceeding

            # Fill email field
            time.sleep(0.5)  # Pause to mimic human behavior and avoid detection
            email_field = driver.find_element(By.NAME, "email")
            time.sleep(0.5)
            email_field.clear()  # Clear any existing text in the input field
            email_field.send_keys(email)  # Enter the email address
            time.sleep(1)  # Brief pause after entering email

            # Click Verify Email button
            verify_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".MuiTypography-root.css-ekhccu"))
            )  # Wait for the "Verify Email" button to be clickable
            try:
                verify_button.click()  # Attempt to click the button normally
            except:
                driver.execute_script("arguments[0].click();", verify_button)  # Use JavaScript click if normal click fails
            time.sleep(10)  # Wait for verification result to load
            # Wait for verification result or error message
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((
                    By.XPATH, "//*[contains(@class, 'MuiBox-root css-pf10ra') or contains(text(), 'Too many requests sent')]"
                ))
            )  # Wait for either the verification result or rate limit message

            # Check for "Too many requests sent" message
            try:
                limit_element = driver.find_element(By.XPATH, "//div[contains(text(), 'Too many requests sent')]")
                if limit_element:
                    logging.warning(f"Too many requests sent for {email}. Restarting Tor...")  # Rate limit detected
                    try:
                        driver.quit()  # Close WebDriver before stopping Tor to prevent stale sessions
                    except WebDriverException as e:
                        logging.warning(f"Failed to close WebDriver on rate limit: {e}")  # Log if quit fails
                    if tor_process:
                        stop_tor(tor_process)  # Stop current Tor process
                    tor_process = start_tor()  # Start new Tor process
                    time.sleep(15)  # Increased delay to stabilize Tor
                    driver = setup_chrome_with_tor()  # Reinitialize WebDriver
                    if attempt < max_retries:
                        logging.info(f"Retrying after Tor restart for {email}")  # Retry if attempts remain
                        continue
                    else:
                        logging.warning(f"Max retries reached after too many requests for {email}")  # Give up after max retries
                        return None, None, None, tor_process  # Return None statuses and current Tor process
            except:
                pass  # No rate limit message found, proceed to check verification result

            # Check for "Email is valid and successfully reachable." or "Email is invalid and cannot be reached." message
            try:
                valid_message = driver.find_element(By.XPATH, ""
                "//div[contains(@class, 'MuiBox-root css-1pmn8ky')]//p[contains(text(), 'Email is valid and successfully reachable.')] | //div[contains(@class, 'MuiBox-root css-pp4hnw')]//p[contains(text(), 'Email is invalid and cannot be reached.')] | //div[contains(@class, 'MuiBox-root css-1pmn8ky')]//p[contains(text(), 'Email is reachable, but it's a catch-all address, and the recipient's existence is uncertain.')]")
                message_text = valid_message.text.strip()  # Get the verification message text
                if message_text in ["Email is valid and successfully reachable.", "Email is invalid and cannot be reached.", "Email is reachable, but it's a catch-all address, and the recipient's existence is uncertain."]:
                    logging.info(f"{'Valid' if 'valid' in message_text.lower() else 'Invalid'} email confirmation found for {email}: {message_text}")  # Log result
                    # Extract Email Status
                    email_status = None
                    try:
                        email_status_element = driver.find_element(By.XPATH, "//div[contains(@class, 'sc-fYsHOw dMtDzI')]//p[contains(text(), 'Email Status')]/following-sibling::div[contains(@class, 'sc-Qotzb dnVSuG')]//span")
                        email_status_text = email_status_element.text.strip()  # Get Email Status text
                        email_status = email_status_text if email_status_text in ["Valid", "Invalid", "Catch-All", "Unknown", "Pending"] else None  # Validate status
                        if message_text in ["Email is reachable, but it's a catch-all address, and the recipient's existence is uncertain."]:
                            email_status = "Catch-All"
                    except:
                        logging.info(f"Email Status not found for {email}")  # Log if status element is missing

                    # Extract Email Syntax Format
                    syntax_status = None
                    try:
                        syntax_status_element = driver.find_element(By.XPATH, "//div[contains(@class, 'sc-fYsHOw dMtDzI')]//p[contains(text(), 'Email Syntax')]/following-sibling::div[contains(@class, 'sc-iQQCXo dXoCvN')]")
                        syntax_status_text = syntax_status_element.text.strip()  # Get Syntax Format text
                        syntax_status = syntax_status_text if syntax_status_text in ["Valid", "Invalid"] else None  # Validate status
                    except:
                        logging.info(f"Email Syntax Format not found for {email}")  # Log if syntax element is missing

                    # Extract Mailbox Server Status
                    server_status = None
                    try:
                        server_status_element = driver.find_element(By.XPATH, "//div[contains(@class, 'sc-fYsHOw dMtDzI')]//p[contains(text(), 'Mailbox Server')]/following-sibling::div[contains(@class, 'sc-iQQCXo dXoCvN')]")
                        server_status_text = server_status_element.text.strip()  # Get Server Status text
                        server_status = server_status_text if server_status_text in ["Valid", "Invalid"] else None  # Validate status
                    except:
                        logging.info(f"Mailbox Server Status not found for {email}")  # Log if server element is missing

                    logging.info(f"Verification results for {email}: Email Status={email_status}, Syntax Format={syntax_status}, Server Status={server_status}")
                    return email_status, syntax_status, server_status, tor_process  # Return statuses and Tor process
                else:
                    logging.info(f"No valid or invalid email confirmation message found for {email}")  # Unexpected message text
                    return None, None, None, tor_process  # Return None statuses
            except Exception as e:
                logging.info(f"No valid or invalid email confirmation message found for {email}, {e}")  # No verification message found
                return None, None, None, tor_process  # Return None statuses

        except (Urllib3NewConnectionError, WebDriverException) as e:
            logging.error(f"Attempt {attempt} failed for {email}: {e}")  # Log connection or WebDriver errors
            if attempt < max_retries:
                logging.info(f"Resetting WebDriver due to connection error for {email}")  # Reset WebDriver for retry
                try:
                    driver.quit()  # Attempt to close WebDriver to prevent stale session
                except WebDriverException as quit_e:
                    logging.warning(f"Failed to close WebDriver on connection error: {quit_e}")  # Log if quit fails
                driver = setup_chrome_with_tor()  # Reinitialize WebDriver
                time.sleep(retry_delay)  # Wait before retrying
                continue
            else:
                logging.warning(f"Max retries reached for {email} after connection error")  # Max retries exhausted
                return None, None, None, tor_process  # Return None statuses

    logging.warning(f"Failed to verify email {email} after {max_retries} attempts")  # Max retries exhausted
    return None, None, None, tor_process  # Return None statuses and Tor process

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
                df[col] = "" if col not in ['Email_Processed', 'Verified_Email'] else False  # Initialize missing columns
        df['Email_Processed'] = df['Email_Processed'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)  # Convert to boolean
        df['Verified_Email'] = df['Verified_Email'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)  # Convert to boolean
        logging.info(f"Found Email_Processed column with {df['Email_Processed'].sum()} processed rows")  # Log processed row count

        # Initialize progress tracking
        write_progress(0, min(len(df), max_rows), job_id, step_id=step_id)

        # Apply offset
        if offset < 0:
            logging.error("Offset cannot be negative")
            raise ValueError("Offset cannot be negative")  # Validate offset
        if offset >= len(df):
            logging.info(f"Offset {offset} is greater than or equal to DataFrame length {len(df)}, no rows to process")
            return df  # Return if offset is too large

        # Adjust max_rows considering the offset
        total_rows = min(len(df) - offset, max_rows)  # Calculate rows to process
        if total_rows <= 0:
            logging.info(f"No rows to process after applying offset {offset} and max_rows {max_rows}")
            return df  # Return if no rows to process

        logging.info(f"Total rows to process after offset {offset}: {total_rows}")

        # Initialize Tor and WebDriver
        tor_process = start_tor()  # Start Tor process
        time.sleep(7)  # Increased delay to ensure Tor stability
        driver = setup_chrome_with_tor()  # Initialize WebDriver with Tor

        # Track if process was stopped
        stopped = False

        try:
            rows_since_last_tor_restart = 0  # Track unprocessed rows since last Tor restart
            for start_idx in range(offset, offset + total_rows, batch_size):
                # Check for stop signal
                if check_stop_signal():
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
                    if check_stop_signal():
                        logging.info("Stop signal detected, terminating process")
                        write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id, stop_call=True)
                        df.to_csv(output_csv, index=False)
                        stopped = True
                        break

                    email = df.at[idx, 'Email'].strip()  # Get and clean email
                    if not email or "@" not in email:
                        logging.info(f"Skipping row {idx + 1}: Invalid or empty email")  # Skip malformed emails
                        df.at[idx, 'Email_Processed'] = True  # Mark as processed
                        df.at[idx, 'Email Status'] = "Invalid"  # Mark as invalid
                        df.at[idx, 'Email Syntax Format'] = None  # No syntax status
                        df.at[idx, 'Mailbox Server Status'] = None  # No server status
                        df.at[idx, 'Verified_Email'] = False  # Not fully valid
                        df.to_csv(output_csv, index=False)  # Save progress
                        logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                        write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id)
                        continue

                    # Check if Tor needs restarting
                    if rows_since_last_tor_restart >= tor_restart_interval:
                        logging.info("Restarting Tor process...")  # Periodic Tor restart to avoid bans
                        try:
                            driver.quit()  # Close WebDriver before stopping Tor
                        except WebDriverException as e:
                            logging.warning(f"Failed to close WebDriver on periodic Tor restart: {e}")  # Log if quit fails
                        stop_tor(tor_process)  # Stop current Tor process
                        tor_process = start_tor()  # Start new Tor process
                        time.sleep(10)  # Increased delay to ensure Tor stability
                        driver = setup_chrome_with_tor()  # Reinitialize WebDriver
                        rows_since_last_tor_restart = 0  # Reset counter
                        logging.info("Tor process restarted and new driver initialized")

                    logging.info(f"Processing row {idx + 1}/{len(df)}: {email}")
                    email_status, syntax_status, server_status, tor_process = verify_email(email, driver, tor_process)  # Verify email
                    # Handle rate limit
                    if email_status is None:
                        logging.info(f"Rate limit reached for row {idx + 1}, restarting Tor and retrying")
                        stop_tor(tor_process)
                        tor_process = start_tor()
                        time.sleep(5)
                        driver.quit()
                        driver = setup_chrome_with_tor()
                        rows_since_last_tor_restart = 0
                        email_status, syntax_status, server_status, tor_process = verify_email(email, driver, tor_process)

                    # Update DataFrame with statuses
                    df.at[idx, 'Email Status'] = email_status  # Store email status (Valid, Invalid, etc.)
                    df.at[idx, 'Email Syntax Format'] = syntax_status  # Store syntax status (Valid, Invalid)
                    df.at[idx, 'Mailbox Server Status'] = server_status  # Store server status (Valid, Invalid)
                    df.at[idx, 'Email_Processed'] = email_status is not None  # Mark as processed if status returned
                    df.at[idx, 'Verified_Email'] = (email_status == "Valid" and syntax_status == "Valid" and server_status == "Valid")  # Fully valid if all Valid

                    rows_since_last_tor_restart += 1  # Increment Tor restart counter for unprocessed rows

                    # # Reset WebDriver after each verification to prevent stale sessions
                    # try:
                    #     driver.quit()  # Close WebDriver to avoid memory leaks or stale sessions
                    # except WebDriverException as e:
                    #     logging.warning(f"Failed to close WebDriver after verification: {e}")  # Log if quit fails
                    # driver = setup_chrome_with_tor()  # Reinitialize WebDriver for next email
                    # logging.info(f"WebDriver reset for next email verification")  # Log reset for debugging

                    # Save progress after each row
                    df.to_csv(output_csv, index=False)  # Save DataFrame to CSV
                    logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                    write_progress(idx + 1, total_rows + offset, job_id, step_id=step_id)
                    time.sleep(random.uniform(1, 2))

                if stopped:
                    break

        finally:
            # Clean up
            try:
                driver.quit()  # Close WebDriver
            except WebDriverException as e:
                logging.warning(f"Failed to close WebDriver in final cleanup: {e}")  # Log if quit fails
            if tor_process:
                stop_tor(tor_process)  # Stop Tor process
            logging.info("WebDriver and Tor process closed")  # Log cleanup

            if not stopped:
                final_status = "stopped" if check_stop_signal() else "completed"
                final_row = total_rows if final_status == "completed" else max(0, min(total_rows, df.index[-1] + 1 if not df.empty else 0))
                write_progress(final_row, total_rows + offset, job_id, step_id=step_id, stop_call=(final_status == "stopped"))

        # Delete rows where Email Status is "Invalid" if specified
        if delete_invalid and not stopped:
            initial_row_count = len(df)
            df = df[df['Email Status'] != "Invalid"]
            deleted_rows = initial_row_count - len(df)
            if deleted_rows > 0:
                logging.info(f"Deleted {deleted_rows} rows where Email Status was 'Invalid'")
                df.to_csv(output_csv, index=False)  # Save updated DataFrame
                logging.info(f"Saved final DataFrame after deleting rows to {output_csv}")
            else:
                logging.info("No rows with Email Status 'Invalid' found to delete")

        logging.info(f"Final row count after processing: {len(df)}")
        return df  # Return updated DataFrame

    except FileNotFoundError:
        logging.error(f"Input CSV file '{input_csv}' not found.")
        print(f"Error: Input CSV file '{input_csv}' not found.")
        return None  # Return None for file errors
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        print(f"Error processing CSV: {e}")
        return None  # Return None for other errors
    finally:
        if not stopped:
            final_status = "stopped" if check_stop_signal() else "completed"
            final_row = total_rows if final_status == "completed" else max(0, min(total_rows, df.index[-1] + 1 if not df.empty else 0))
            write_progress(final_row, total_rows + offset, job_id, step_id=step_id, stop_call=(final_status == "stopped"))

# Example usage
if __name__ == "__main__":
    setup_logging()
    input_path = os.path.join(Config.DATA_CSV_PATH, "emails")
    input_csv = "Test.csv"
    output_path = os.path.join(Config.DATA_CSV_PATH, "emails/verified")
    process_csv_and_verify_emails(
        input_csv=os.path.join(input_path, f"Emails_DomainAbout_Updated_URL_Updated_Name_Filtered_{input_csv}"),
        output_csv=os.path.join(output_path, f"Verified_Emails_DomainAbout_Updated_URL_Updated_Name_Filtered_{input_csv}"),
        max_rows=2000,
        batch_size=50,
        tor_restart_interval=5,
        offset=0,
        delete_invalid=True,
        job_id=str(uuid.uuid4()),
        step_id='step7'
    )