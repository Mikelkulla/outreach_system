"""
This script is designed to verify email addresses listed in a CSV file using the Skrapp.io
email verification service. It automates the process of reading emails from a CSV,
submitting them to Skrapp.io via a Selenium-controlled web browser, parsing the verification
results, and updating the CSV with these results.

Key functionalities include:
- Reading email addresses from an input CSV file.
- Interacting with the Skrapp.io email verifier webpage using Selenium.
- Integration with Tor for IP address rotation to mitigate rate limiting or IP blocks
  by Skrapp.io. The script can start, stop, and restart Tor processes.
- Batch processing of emails to manage resources and save progress incrementally.
- Handling various verification outcomes from Skrapp.io, such as "Valid", "Invalid",
  "Catch-All", as well as statuses indicating operational issues like rate limits ("search_limit"),
  timeouts, or connection errors.
- Updating the input CSV file with new columns for "Email Status" and a flag "Email_Processed"
  to track verification status and progress.
- Skipping rows that have already been processed in previous runs.
- Optional deletion of rows where the email status is determined to be "Invalid".
- Progress tracking and graceful shutdown capabilities via an external stop signal,
  coordinated through `job_functions`.
- Configuration for file paths and logging is managed through imported `Config` and
  logging setup utilities.

The script uses pandas for CSV data manipulation, Selenium for web automation, and standard
Python libraries for logging, OS interaction, and process management. It is intended to be
run as part of a larger data processing pipeline, identified by `job_id` and `step_id`.
"""

import time
import random
import logging
import os
import uuid
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib3.exceptions import NewConnectionError as Urllib3NewConnectionError
from backend.scripts.selenium.driver_setup_for_scrape import restart_driver_and_tor, start_tor, stop_tor, setup_chrome_with_tor
from backend.config import Config
from config.logging import setup_logging
from config.job_functions import write_progress, check_stop_signal
from config.utils import load_csv
from selenium.common.exceptions import WebDriverException, TimeoutException

# --- Constants ---
SKRAPP_VERIFIER_URL = "https://skrapp.io/email-verifier"
SKRAPP_EMAIL_INPUT_NAME = "email"
SKRAPP_VERIFY_BUTTON_CSS = ".MuiTypography-root.css-1ulaxtk" # Specific CSS class for the verify button
SKRAPP_RESULT_OR_LIMIT_XPATH = "//*[contains(@class, 'MuiBox-root css-pf10ra') or contains(text(), 'Too many requests sent')]" # General area where results or limit messages appear
SKRAPP_RATE_LIMIT_TEXT_XPATH = "//div[contains(., 'Too many requests sent')]" # Specific text for rate limit

# Email Verification Statuses (as returned by this script)
STATUS_VALID = "Valid"
STATUS_INVALID = "Invalid"
STATUS_CATCH_ALL = "Catch-All"
STATUS_SEARCH_LIMIT = "search_limit"
STATUS_TIMEOUT = "timeout"
STATUS_CONNECTION_ERROR = "connection_error"
STATUS_NO_RESULT = "no_result"

# XPaths for specific Skrapp.io verification messages
# These are kept specific as the page structure dictates them.
VERIFICATION_MESSAGE_XPATHS = [
    (STATUS_VALID,       "//div[contains(@class, 'MuiBox-root css-1pmn8ky')]//p[contains(text(), 'Email is valid and successfully reachable.')]"),
    (STATUS_INVALID,     "//div[contains(@class, 'MuiBox-root css-pp4hnw')]//p[contains(text(), 'Email is invalid and cannot be reached.')]"),
    (STATUS_CATCH_ALL,   "//div[contains(@class, 'MuiBox-root css-1pmn8ky')]//p[contains(text(), 'Email is reachable, but it')]"),
    (STATUS_CATCH_ALL,   "//div[contains(@class, 'MuiBox-root css-1b3tdk3')]//p[contains(text(), 'Email is reachable, but it')]") # Another variant for Catch-All
]
# --- End Constants ---

# Placeholder for a potential alternative email verification function
def verify_email_neverbounce():
    """
    Placeholder function for verifying an email using NeverBounce.
    Currently not implemented.
    """
    pass

def verify_email_scrapp(email, driver, tor_process=None, max_retries=2, retry_delay=2):
    """
    Verifies an email on Skrapp.io Email Verifier with up to 2 retries, checking only the verification message.
    
    It navigates to the Skrapp.io verifier, inputs the email, clicks verify,
    and then parses the result page for known status messages. Handles common
    issues like access denial (potential IP block) by returning `STATUS_SEARCH_LIMIT`,
    which signals the calling function to cycle Tor.

    Parameters:
        email (str): Email address to verify.
        driver (WebDriver): Selenium WebDriver instance.
        tor_process (subprocess.Popen, optional): Tor process instance. (Passed through, not directly managed here).
        max_retries (int): Maximum number of retry attempts for verification.
        retry_delay (int): Delay in seconds between retries.

    Returns:
        tuple: (email_status, tor_process)
               - email_status (str): One of the defined STATUS_* constants
               - tor_process (subprocess.Popen): The current Tor process (passed through).
    """
    for attempt in range(1, max_retries + 1):
        try:
            # Navigate to Skrapp.io email verifier page
            logging.info(f"Attempt {attempt}/{max_retries} to verify email: {email} using Skrapp.io")
            driver.get(SKRAPP_VERIFIER_URL)
            time.sleep(random.uniform(3, 5)) # Random delay to mimic human behavior

            # Check for access denied (HTTP 403) or request blocked, potentially indicating IP block
            page_source_lower = driver.page_source.lower()
            if  "access to skrapp.io was denied" in page_source_lower or \
                    "http error 403" in page_source_lower or \
                    "your request has been blocked" in page_source_lower: # Added another common block message:
                logging.warning(f"Access denied or request blocked for {email} (Attempt {attempt}). Returning STATUS_SEARCH_LIMIT to trigger Tor restart.")
                return STATUS_SEARCH_LIMIT, tor_process

            # Wait for the email input field to be present on the page
            email_field = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.NAME, SKRAPP_EMAIL_INPUT_NAME))
            )

            time.sleep(random.uniform(0.5, 1))
            email_field.clear()
            time.sleep(random.uniform(0.2, 0.8))
            email_field.send_keys(email)
            time.sleep(random.uniform(0.5, 1))
            
            # Locate and click the "Verify Email" button
            verify_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, SKRAPP_VERIFY_BUTTON_CSS))
            )
            try:
                verify_button.click()
            except Exception as click_exc: # Fallback to JavaScript click
                logging.warning(f"Direct click failed for verify button (Attempt {attempt}): {click_exc}. Trying JavaScript click.")
                driver.execute_script("arguments[0].click();", verify_button)

            # Wait for verification result area to appear or a rate limit message
            logging.debug(f"Waiting for verification result for {email} (Attempt {attempt})...")
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, SKRAPP_RESULT_OR_LIMIT_XPATH))
                )
                logging.debug(f"Result area or limit message located for {email}.")                
            except TimeoutException:
                # Handle timeout if no result or error message appears within the timeframe
                logging.warning(f"Timeout (25s) waiting for verification result area for {email} (Attempt {attempt}).")
                if attempt < max_retries:
                    logging.info(f"Retrying after delay of {retry_delay}s...")
                    time.sleep(retry_delay) # Wait before retrying
                    continue
                return STATUS_TIMEOUT, tor_process # Max retries for this specific timeout
            
            # Check specifically for "Too many requests sent" message (rate limiting)
            try:
                # Short wait for this specific message, as it might appear quickly.
                limit_element = WebDriverWait(driver, 7).until(
                    EC.presence_of_element_located((By.XPATH, SKRAPP_RATE_LIMIT_TEXT_XPATH))
                )
                if limit_element: # Element found
                    logging.warning(f"'Too many requests sent' detected for {email} (Attempt {attempt}). Returning STATUS_SEARCH_LIMIT to trigger Tor restart.")
                    return STATUS_SEARCH_LIMIT, tor_process
            except TimeoutException:
                logging.debug(f"Message not found, proceed to check for other statuses.")# Message not found, proceed to check for other statuses.
            except Exception as e_limit_check: 
                logging.debug(f"Non-critical error checking for rate limit message for {email}: {e_limit_check}")

            # Check for different verification statuses (Valid, Invalid, Catch-All)
            email_status_found = None
            for status_constant, xpath_pattern in VERIFICATION_MESSAGE_XPATHS:
                try:
                    # Short timeout for finding status messages, page should be loaded.
                    WebDriverWait(driver, 13).until(
                        EC.presence_of_element_located((By.XPATH, xpath_pattern))
                    )                    
                    email_status_found = status_constant # Use the constant from the tuple
                    logging.info(f"'{email_status_found}' email status confirmed for {email} (Attempt {attempt}).")
                    break # Exit loop once a status is found
                except TimeoutException:
                    logging.debug(f"Status message for '{status_constant}' not found for {email} using XPath: {xpath_pattern}")
                    continue 
                except Exception as e_status_check: 
                    logging.warning(f"Error checking for '{status_constant}' status for {email}: {e_status_check}")
                    continue

            if not email_status_found:
                # If no specific status message is found after checking all possibilities
                logging.warning(f"No valid, invalid, or catch-all email confirmation message found for {email}.")
                return STATUS_NO_RESULT, tor_process

            logging.info(f"Verification result for {email}: Email Status={email_status_found}")
            return email_status_found, tor_process

        except (Urllib3NewConnectionError, WebDriverException) as e_wd:
            logging.error(f"WebDriver or Connection Error on attempt {attempt} for {email}: {e_wd}")
            if attempt < max_retries:
                logging.info(f"Retrying after delay of {retry_delay}s...")
                time.sleep(retry_delay)
                continue

            else:
                logging.warning(f"Max retries reached for {email} after WebDriver/Connection error. Returning STATUS_CONNECTION_ERROR.")
                return STATUS_CONNECTION_ERROR, tor_process
        except Exception as e_general: # Catch any other unexpected errors
            logging.error(f"Unexpected error on attempt {attempt} for {email}: {e_general}", exc_info=True)
            if attempt < max_retries:
                logging.info(f"Retrying after delay of {retry_delay}s...")
                time.sleep(retry_delay)
                continue
            else:
                logging.warning(f"Max retries reached for {email} after unexpected error. Returning STATUS_CONNECTION_ERROR as fallback.")
                return STATUS_CONNECTION_ERROR, tor_process

    # Fallback if all retries fail.
    logging.error(f"Failed to verify email {email} after {max_retries} attempts (exhausted loop). Defaulting to STATUS_CONNECTION_ERROR.")
    return STATUS_CONNECTION_ERROR, tor_process # Default to connection error if loop completes without success

def process_csv_and_verify_emails(input_csv, output_csv, max_rows=2000, batch_size=50, tor_restart_interval=30, offset=0, delete_invalid=True, job_id=None, step_id='step7'):
    """
    Processes a CSV file to verify email addresses contained within it using the Skrapp.io service.
    It reads emails from the specified input CSV, iteratively verifies them, and writes the
    results (verification status, processed flag) back to an output CSV.

    The function incorporates several key features:
    - **Incremental Processing**: Works through the CSV in batches, saving progress after each email
      is processed to allow for resumption in case of interruption.
    - **Skip Processed Rows**: Checks for a 'Email_Processed' column and skips rows already marked True.
    - **Tor Integration**: Manages a Tor process for IP rotation, restarting it at a configurable
      interval (`tor_restart_interval`) of processed emails to avoid IP-based rate limits.
    - **Error Handling**: Includes try-except blocks for robust operation, handling network issues,
      WebDriver problems, and Skrapp.io specific responses (like rate limits).
    - **Offset and Max Rows**: Allows processing a subset of the CSV using `offset` to skip initial
      rows and `max_rows` to limit the total number of rows processed in a run.
    - **Optional Deletion**: If `delete_invalid` is True, rows with an "Invalid" email status are
      removed from the DataFrame before final saving.
    - **Progress Reporting**: Uses `write_progress` and `check_stop_signal` for integration with
      an external job management system (via `job_id` and `step_id`).

    Parameters:
    -----------
    input_csv (str):
        Path to the input CSV file. This file must contain a column with email addresses,
        typically named 'Email'.
    output_csv (str):
        Path where the updated CSV file (with verification results) will be saved.
        Progress is saved to this file incrementally.
    max_rows (int, optional):
        The maximum number of rows to process from the input CSV in this run. Defaults to 2000.
    batch_size (int, optional):
        The number of rows to process in each batch. Logging occurs at batch boundaries.
        Note: WebDriver and Tor restarts are based on `tor_restart_interval`, not `batch_size`.
        Defaults to 50.
    tor_restart_interval (int, optional):
        The number of successfully processed (not skipped) emails after which the Tor process
        and WebDriver will be restarted. This helps in managing IP reputation. Defaults to 30.
    offset (int, optional):
        The number of rows to skip from the beginning of the input CSV. Defaults to 0.
    delete_invalid (bool, optional):
        If True, rows where the 'Email Status' is determined to be "Invalid" will be
        deleted from the DataFrame at the end of processing. Defaults to True.
    job_id (str, optional):
        A unique identifier for the current processing job, used for external progress tracking.
        Defaults to None.
    step_id (str, optional):
        An identifier for the current step within a larger job pipeline, used for external
        progress tracking and stop signal checks. Defaults to 'step7'.

    Returns:
    --------
    pd.DataFrame or None:
        The updated pandas DataFrame with email verification results if processing completes
        (or is stopped gracefully). Returns None if a critical error occurs during setup
        (e.g., input file not found).
    """
    try:
        # Load the input CSV file into a pandas DataFrame
        # The load_csv utility function handles file existence and basic structure checks.
        df, resolved_input_csv = load_csv(
            input_csv=input_csv,
            output_csv=output_csv,
            required_columns=['Email'] # Ensures the 'Email' column is present
        )
        if df is None: # load_csv returns None if loading fails
            return None

        # Initialize 'Email Status' and 'Email_Processed' columns if they don't exist.
        # 'Email_Processed' tracks whether an email has been attempted for verification.
        # It's mapped to boolean True/False and defaults to False.
        for col in ['Email Status', 'Email_Processed']:
            if col not in df.columns:
                df[col] = "" if col == 'Email Status' else False
        df['Email_Processed'] = df['Email_Processed'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)
        logging.info(f"Found Email_Processed column with {df['Email_Processed'].sum()} processed rows")

        # Initialize progress reporting for external monitoring
        # Reports total rows to be processed (considering max_rows and actual df length).
        write_progress(0, min(len(df), max_rows), job_id, step_id=step_id)

        # Apply offset: skip the first 'offset' number of rows.
        if offset < 0:
            logging.error("Offset cannot be negative.")
            raise ValueError("Offset cannot be negative")
        if offset >= len(df):
            logging.info(f"Offset {offset} is greater than or equal to DataFrame length {len(df)}. No rows to process.")
            return df # Return the original DataFrame as no processing is needed.

        # Calculate the total number of rows to actually process, considering offset and max_rows.
        total_rows_to_process_after_offset = min(len(df) - offset, max_rows)
        if total_rows_to_process_after_offset <= 0:
            logging.info(f"No rows to process after applying offset {offset} and max_rows {max_rows}.")
            return df

        logging.info(f"Total rows to process (after offset {offset}, up to max_rows {max_rows}): {total_rows_to_process_after_offset}")

        # Initialize Tor process and Selenium WebDriver configured to use Tor.
        # This is crucial for IP rotation to avoid rate limits from Skrapp.io.
        tor_process = start_tor()
        time.sleep(7) # Allow time for Tor to establish a connection
        driver = setup_chrome_with_tor()

        stopped = False # Flag to indicate if processing was stopped by an external signal.

        try:
            rows_since_last_tor_restart = 0 # Counter for Tor restart interval
            # Process DataFrame in batches. `offset` defines the starting point.
            # `total_rows_to_process_after_offset` defines how many rows from the offset point should be processed.
            for batch_start_idx in range(offset, offset + total_rows_to_process_after_offset, batch_size):
                if check_stop_signal(step_id):
                    logging.info("Stop signal detected, terminating process")
                    write_progress(batch_start_idx + 1, total_rows_to_process_after_offset + offset, job_id, step_id=step_id, stop_call=True)
                    df.to_csv(output_csv, index=False)
                    stopped = True
                    break

                batch_end_idx = min(batch_start_idx + batch_size, offset + total_rows_to_process_after_offset)
                current_batch_df_slice = df.iloc[batch_start_idx:batch_end_idx]

                # Check for unprocessed rows
                unprocessed_mask = ~current_batch_df_slice['Email_Processed']
                if not unprocessed_mask.any():
                    logging.info(f"Batch from index {batch_start_idx}-{batch_end_idx} already processed, skipping.")
                    write_progress(batch_end_idx, offset + total_rows_to_process_after_offset, job_id, step_id=step_id)
                    continue # Move to the next batch.

                logging.info(f"Processing batch: rows from index {batch_start_idx} to {batch_end_idx} (out of {offset + total_rows_to_process_after_offset} total to process).")

                try:
                    for idx in current_batch_df_slice[unprocessed_mask].index:
                        # Check for stop signal before processing each row.
                        if check_stop_signal(step_id):
                            logging.info("Stop signal detected, terminating process")
                            write_progress(idx + 1, total_rows_to_process_after_offset + offset, job_id, step_id=step_id, stop_call=True)
                            df.to_csv(output_csv, index=False)
                            stopped = True
                            break

                        email = df.at[idx, 'Email'].strip()
                        
                        if not email or "@" not in email:
                            logging.info(f"Skipping row {idx + 1}: Invalid or empty email")
                            df.at[idx, 'Email_Processed'] = True
                            df.at[idx, 'Email Status'] = STATUS_INVALID
                            df.to_csv(output_csv, index=False)
                            logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                            write_progress(idx + 1, total_rows_to_process_after_offset + offset, job_id, step_id=step_id)
                            continue

                        # Check if Tor needs restarting
                        if rows_since_last_tor_restart >= tor_restart_interval:
                            logging.info("Restarting Tor process...")
                            driver, tor_process = restart_driver_and_tor(driver, tor_process, True, False)
                            rows_since_last_tor_restart = 0
                            logging.info("Tor process restarted and new driver initialized")

                        logging.info(f"Processing row {idx + 1}/{len(df)}: {email}")
                        email_status, tor_process = verify_email_scrapp(email, driver, tor_process)
                        # Handle rate limit
                        if email_status == STATUS_SEARCH_LIMIT:
                            logging.info(f"Rate limit reached for row {idx + 1}, restarting Tor and retrying")
                            driver, tor_process = restart_driver_and_tor(driver, tor_process, True, False)
                            rows_since_last_tor_restart = 0
                            email_status, tor_process = verify_email_scrapp(email, driver, tor_process)

                        # Update DataFrame with status
                        df.at[idx, 'Email Status'] = email_status
                        # Set Email_Processed based on status
                        df.at[idx, 'Email_Processed'] = email_status in [ STATUS_VALID, STATUS_INVALID, STATUS_CATCH_ALL]
                        if email_status in [STATUS_SEARCH_LIMIT, STATUS_TIMEOUT, STATUS_CONNECTION_ERROR, STATUS_NO_RESULT]:
                            # rows_since_last_tor_restart = 0  # Reset counter for transient errors
                            pass
                        else:
                            rows_since_last_tor_restart += 1

                        # Reset WebDriver after each verification to prevent stale sessions
                        try:
                            driver.quit()
                        except WebDriverException as e:
                            logging.warning(f"Failed to close WebDriver after verification: {e}")
                        driver = setup_chrome_with_tor() 
                        logging.info(f"WebDriver reset for next email verification")

                        # Save progress to the output CSV file after processing each row.
                        df.to_csv(output_csv, index=False)
                        logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                        # Report progress for this row.
                        write_progress(idx + 1, total_rows_to_process_after_offset + offset, job_id, step_id=step_id)

                        time.sleep(random.uniform(1, 2)) # Small random delay between requests.
                except Exception as e:
                    logging.error(f"Error processing batch from index {batch_start_idx} to {batch_end_idx}: {e}", exc_info=True)
                finally:
                    # # Ensure WebDriver for the batch is closed.
                    # if driver is not None:
                    #     try:
                    #         driver.quit()
                    #         logging.info(f"WebDriver closed successfully for batch {batch_start_idx}-{batch_end_idx}.")
                    #     except Exception as e:
                    #         logging.error(f"Error closing WebDriver for batch {batch_start_idx}-{batch_end_idx}: {e}")
                    #     # kill_chrome_processes() # Optional: ensure all related browser processes are terminated.
                        time.sleep(2)  # Brief pause to ensure processes can terminate fully.
                if stopped:
                    break

        finally: # This 'finally' is for the main try-catch block of the function.
            # This block executes regardless of exceptions in the processing loop,
            # but not if WebDriver & Tor Process are managed solely within the batch loop's try-finally.
            # The driver & tor here would be the one from the last batch if not properly closed there.
            # However, current logic closes driver per batch. This might be redundant or a safeguard.
            if driver is not None: # Check if driver instance might still exist (e.g., if error before batch finally)
                try:
                    driver.quit()
                    time.sleep(2)
                    logging.info("WebDriver closed successfully in main finally block (safeguard).")
                except Exception as e:
                    logging.error(f"Error closing WebDriver in main finally block: {e}")
            try:
                if tor_process: # Check if tor proccess might still exist (e.g., if error before batch finally)
                    stop_tor(tor_process)
                    time.sleep(5)
                    logging.info("Tor process stopped successfully in main finally block (safeguard).")                    
            except Exception as e:
                    logging.error(f"Error stopped Tor process in main finally block: {e}")

            # Report final progress status (completed or stopped).
            if not stopped:
                # Determine the final processed row count for progress reporting.
                # If completed, it's total_rows_to_process_after_offset.
                # If stopped, it's based on the last processed index.
                final_status = "stopped" if check_stop_signal(step_id) else "completed"
                # Calculate the number of rows processed from the perspective of the 'offset' start.
                # If df is empty or offset is beyond df length, df.index[-1] would error.
                last_processed_row_absolute_index = df.index[-1] + 1 if not df.empty and offset < len(df) else offset
                # Effective rows processed in this run.
                effective_processed_count = max(0, last_processed_row_absolute_index)

                final_row_for_progress = (total_rows_to_process_after_offset + offset) if final_status == "completed" else effective_processed_count
                
                logging.info(f"Final Status: {final_status}. Reporting progress for {final_row_for_progress}/{total_rows_to_process_after_offset + offset} effective rows.")
                write_progress(final_row_for_progress, total_rows_to_process_after_offset + offset, job_id, step_id=step_id, stop_call=(final_status == "stopped"))

        if delete_invalid and not stopped:
            initial_row_count = len(df)
            df = df[df['Email Status'] != STATUS_INVALID]
            deleted_rows_count = initial_row_count - len(df)
            if deleted_rows_count > 0:
                logging.info(f"Deleted {deleted_rows_count} rows where Email Status was {STATUS_INVALID}")
                df.to_csv(output_csv, index=False) # Save the filtered DataFrame.
                logging.info(f"Saved final DataFrame after deleting rows to {output_csv}")
            else:
                logging.info(f"No rows with Email Status {STATUS_INVALID} found to delete")

        logging.info(f"Final row count after processing: {len(df)}")
        return df

    except FileNotFoundError:
        logging.error(f"Input CSV file '{input_csv}' not found.")
        print(f"Error: Input CSV file '{input_csv}' not found.") # Also print to console for direct script execution.
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred in process_csv_and_verify_emails: {e}", exc_info=True)
        print(f"Error processing CSV: {e}") # Also print to console.
        return None

# Example usage: This block executes if the script is run directly.
if __name__ == "__main__":
    # Configure logging for the script
    setup_logging()
    # Define input and output paths
    input_csv = "Legal_Services.csv"
    input_path = os.path.join(Config.DATA_CSV_PATH, "emails")
    output_path = os.path.join(Config.DATA_CSV_PATH, "verified")

    # Construct full input and output file paths.
    full_input_csv_path = os.path.join(input_path, f"Emails_DomainAbout_Updated_Name_Filtered_{input_csv}")
    full_output_csv_path = os.path.join(output_path, f"Verified_Emails_DomainAbout_Updated_Name_Filtered_{input_csv}")


    # Call the main processing function with example parameters.
    process_csv_and_verify_emails(
        input_csv=full_input_csv_path,
        output_csv=full_output_csv_path,
        max_rows=2000,       # Process up to 2000 rows from the CSV.
        batch_size=50,       # Process in batches of 50 rows.
        tor_restart_interval=5,
        offset=0,
        delete_invalid=True,
        job_id=str(uuid.uuid4()), # Generate a unique ID for this job run for tracking.
        step_id='step7'
    )