"""
This script processes a CSV file containing LinkedIn company URLs.
For each company URL, it uses Selenium to navigate to the company's "About" page
on LinkedIn and extracts two key pieces of information:
1. The company's official website URL.
2. The text content from the company's "About" section.

The script is designed to be robust, incorporating features such as:
- Retry mechanisms for network or page loading issues.
- Handling of LinkedIn login requirements by re-initializing the WebDriver session.
- Batch processing of URLs from the input CSV.
- Saving progress incrementally to an output CSV file.
- Optional deletion of rows from the output if a website URL could not be found.
- Progress tracking and the ability to be stopped gracefully via an external signal.

It relies on pandas for CSV manipulation, Selenium for web scraping, and tldextract
for parsing clean domain names from URLs. Configuration for paths and helper functions
for WebDriver setup and progress reporting are imported from other modules within the project.
"""
import uuid
import pandas as pd
import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import os
import logging
from urllib.parse import urlparse, parse_qs, unquote
import tldextract
from backend.scripts.selenium.driver_setup_for_scrape import restart_driver_and_tor, setup_driver_linkedin_singin
from backend.config import Config
from config.job_functions import write_progress, check_stop_signal
from config.utils import load_csv

def extract_company_info(first_name, company_url, index, driver, max_retries=3, retry_delay=3):
    """
    Extracts the "About" section text and website domain from a given LinkedIn company URL.

    This function navigates to the company's LinkedIn page, specifically targeting the "About"
    section. It attempts to find and extract the company's listed website and the descriptive
    text in their "About" section. Includes retry logic for robustness and handles cases
    where LinkedIn might require a login during the process.

    Args:
        first_name (str): The first name associated with the company entry (used for logging purposes).
        company_url (str): The LinkedIn URL of the company.
        index (int): The row index from the input CSV (used for logging and tracking).
        driver (webdriver): The Selenium WebDriver instance to use for browsing.
        max_retries (int): Maximum number of attempts to fetch and parse the page.
        retry_delay (int): Delay in seconds between retry attempts.

    Returns:
        tuple: A tuple containing:
            - result (dict): A dictionary with keys "First Name", "Company URL", "Website",
                             "About_Text", and "Row". "Website" and "About_Text" will
                             be "None" if not found.
            - driver (webdriver): The (potentially updated) Selenium WebDriver instance.
    """
    # Initialize result dictionary with default values
    result = {
        "First Name": first_name,
        "Company URL": company_url,
        "Website": "None", # Default if not found
        "About_Text": "None", # Default if not found
        "Row": index + 1
    }

    # Validate company_url: must exist and be a LinkedIn company URL
    if not company_url or "linkedin.com/company/" not in company_url:
        logging.warning(f"Skipping invalid or empty URL for {first_name} at row {index + 1}: {company_url}")
        return result, driver

    # Retry loop for fetching and parsing page content
    for attempt in range(1, max_retries + 1):
        try:
            logging.debug(f"Attempt {attempt} to get URL: {company_url} for {first_name}")
            driver.get(company_url)
            time.sleep(random.uniform(2, 4))  # Allow time for dynamic content like JavaScript to load

            # Check if LinkedIn redirected to a login page
            if "linkedin.com/login" in driver.current_url:
                logging.error(f"Login required for {company_url} (First Name: {first_name}). Attempting re-login...")
                # Re-initialize driver with login credentials
                driver, _ = restart_driver_and_tor(driver=driver, tor_process=None, use_tor=False, linkedin=True)
                time.sleep(2)                       # Wait for re-login process
                driver.get(company_url)             # Try accessing the company URL again
                time.sleep(random.uniform(2, 4))    # Allow page to load after re-login

            wait = WebDriverWait(driver, 10) # Explicit wait for elements

            # --- Extract Website ---
            website = "None"
            try:
                # XPath to locate the website link. It looks for a 'dt' containing "Website"
                # and gets the 'a' tag in the following 'dd', or a generic link within a 'dd' for "Website".
                website_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//section[contains(@class, "org-about-module__margin-bottom")]//dl//dt[contains(., "Website")]/following-sibling::dd[1]/a | //a[contains(@href, "http") and ancestor::dd[contains(., "Website")]]'))
                )
                raw_href = website_element.get_attribute("href").strip() if website_element else "None"

                # Handle LinkedIn's redirect URLs (e.g., linkedin.com/redir/redirect?url=ENCODED_URL&...)
                if "linkedin.com/redir/redirect?" in raw_href:
                    parsed_url = urlparse(raw_href)
                    query_params = parse_qs(parsed_url.query)
                    encoded_url = query_params.get("url", [""])[0]      # Get the encoded actual URL
                    decoded_url = unquote(encoded_url)                  # Decode the URL
                else:
                    decoded_url = raw_href # URL is direct

                # Extract the base domain (e.g., "example.com") from the full URL
                if decoded_url != "None":
                    ext = tldextract.extract(decoded_url)
                    website = f"{ext.domain}.{ext.suffix}" if ext.domain and ext.suffix else "None"
                logging.info(f"Extracted website for {first_name} (Row {index + 1}): {website}")
            except (TimeoutException, Exception) as e:
                logging.warning(f"Failed to extract website for {first_name} (Row {index + 1}) from {company_url}: {e}")
                website = "None" # Ensure it's "None" on failure

            # --- Extract About section text ---
            about_text = "None"
            try:
                # XPath to locate the "About" text, typically in a <p> tag within a specific section.
                # It targets paragraphs with class "break-words" inside "org-about-module__margin-bottom" section,
                # or any paragraph inside an "org-about-module" section as a fallback.
                about_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//section[contains(@class, "org-about-module__margin-bottom")]//p[contains(@class, "break-words")] | //section[contains(@class, "org-about-module")]//p'))
                )
                about_text = about_element.text.strip() if about_element else "None"
                logging.info(f"Extracted About_Text for {first_name} (Row {index + 1}): {about_text[:50]}...") # Log first 50 chars
            except (TimeoutException, Exception) as e:
                logging.warning(f"Failed to extract About_Text for {first_name} (Row {index + 1}) from {company_url}: {e}")
                about_text = "None" # Ensure it's "None" on failure

            # Update result dictionary with extracted data
            result.update({
                "Website": website,
                "About_Text": about_text
            })
            return result, driver # Successfully extracted, return result

        except Exception as e:
            logging.error(f"[Attempt {attempt}/{max_retries}] Error processing {company_url} for {first_name} (Row {index + 1}): {e}")
            if attempt < max_retries:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                # Restart driver if a severe error occurred, ensuring a fresh session for retry
                driver, _ = restart_driver_and_tor(driver, None, use_tor=False, linkedin=True)
                time.sleep(2) # Wait for driver to restart
            else:
                logging.error(f"Max retries reached for {company_url}. Skipping.")
                return result, driver # Return default result after max retries

    return result, driver # Should ideally be returned within the loop

def process_csv_and_extract_info(input_csv, output_csv, max_rows=2000, batch_size=50, delete_no_website=True, offset=0, job_id=None, step_id='step5'):
    """
    Processes a CSV file containing LinkedIn company URLs, extracts "About" text and website domains,
    and saves the enriched data to an output CSV.

    This function manages the overall workflow: loading data, iterating through rows in batches,
    invoking `extract_company_info` for each company, handling WebDriver lifecycle,
    saving progress, and optionally cleaning up rows where no website was found.

    Parameters:
    -----------
    input_csv (str): Path to the input CSV file. Must contain a column named 'Regular Company Url'.
    output_csv (str): Path where the updated CSV file will be saved.
    max_rows (int): Maximum number of rows from the input CSV to process.
    batch_size (int): Number of rows to process before potentially re-initializing WebDriver and logging.
    delete_no_website (bool): If True, rows where 'Website' remains "None" after processing
                              will be removed from the final output CSV.
    offset (int): Number of rows to skip from the beginning of the input CSV.
    job_id (str): A unique identifier for the current processing job (for external tracking).
    step_id (str): Identifier for the current step in a larger job pipeline (for external tracking).
                              step4 for job 4

    Returns:
    --------
    pd.DataFrame or None: The updated DataFrame with extracted information, or None if a
                          critical error occurs during setup (e.g., file not found).
    """
    try:
        # Load the input CSV file using a utility function.
        # 'Regular Company Url' is expected to be the column with LinkedIn company URLs.
        linkedin_column = 'Regular Company Url'
        df, resolved_input_csv = load_csv(
            input_csv=input_csv,
            output_csv=output_csv,
            required_columns=[linkedin_column]
        )
        if df is None: # load_csv returns None on failure (e.g., file not found)
            return None

        # Initialize new columns in the DataFrame if they don't already exist.
        if 'Website' not in df.columns:
            df['Website'] = "None"                          # To store extracted website domain
        if 'About_Text' not in df.columns:
            df['About_Text'] = "None"                       # To store extracted "About" section text
        # 'Processed_About_Website' tracks if a row has been attempted.
        # This helps in resuming interrupted jobs and skipping already processed rows.
        if 'Processed_About_Website' not in df.columns:
            df['Processed_About_Website'] = False
        else:
            # Ensure 'Processed_About_Website' is boolean, handling string representations if loaded from CSV.
            df['Processed_About_Website'] = df['Processed_About_Website'].map(
                {'True': True, 'False': False, True: True, False: False}
            ).fillna(False)
            logging.info(f"Found 'Processed_About_Website' column with {df['Processed_About_Website'].sum()} rows already marked as processed.")

        # Initialize progress reporting for external monitoring.
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

        stopped = False # Flag to indicate if processing was stopped by an external signal.

        try:
            # Process rows in batches for better resource management and logging.
            # The outer loop iterates from 'offset' up to 'offset + total_rows_to_process_after_offset'.
            for batch_start_idx in range(offset, offset + total_rows_to_process_after_offset, batch_size):
                # Check for an external stop signal before starting a new batch.
                if check_stop_signal(step_id):
                    logging.info("Stop signal detected. Terminating processing.")
                    # Report current progress before stopping.
                    write_progress(batch_start_idx, offset + total_rows_to_process_after_offset, job_id, step_id=step_id, stop_call=True)
                    df.to_csv(output_csv, index=False) # Save current state.
                    stopped = True
                    break # Exit the batch processing loop.

                batch_end_idx = min(batch_start_idx + batch_size, offset + total_rows_to_process_after_offset)
                current_batch_df_slice = df.iloc[batch_start_idx:batch_end_idx]
                
                # Identify rows within the current batch that haven't been processed yet.
                unprocessed_mask = ~current_batch_df_slice['Processed_About_Website']
                if not unprocessed_mask.any():
                    logging.info(f"Batch from index {batch_start_idx} to {batch_end_idx} already processed. Skipping.")
                    write_progress(batch_end_idx, offset + total_rows_to_process_after_offset, job_id, step_id=step_id)
                    continue # Move to the next batch.

                logging.info(f"Processing batch: rows from index {batch_start_idx} to {batch_end_idx} (out of {offset + total_rows_to_process_after_offset} total to process).")
                
                driver = None # Initialize WebDriver to None for the current batch.
                try:
                    # Setup Selenium WebDriver with LinkedIn login for this batch.
                    # kill_chrome_processes() # Potentially useful if issues with lingering processes occur.
                    driver = setup_driver_linkedin_singin()
                    time.sleep(3) # Allow time for driver setup and login.

                    # Iterate through each row in the current batch that needs processing.
                    # 'idx' here is the original DataFrame index.
                    for idx in current_batch_df_slice[unprocessed_mask].index:
                        # Check for stop signal before processing each row.
                        if check_stop_signal(step_id):
                            logging.info(f"Stop signal detected during row processing at index {idx + 1}. Terminating.")
                            write_progress(idx, offset + total_rows_to_process_after_offset, job_id, step_id=step_id, stop_call=True)
                            df.to_csv(output_csv, index=False) # Save progress.
                            stopped = True
                            break # Exit the inner loop (row processing).

                        # Check if the Selenium session has expired (e.g., redirected to login page).
                        if driver.current_url.startswith("https://www.linkedin.com/login"):
                            logging.warning(f"Session expired for row {idx + 1}. Reinitializing WebDriver and re-logging in.")
                            driver, _ = restart_driver_and_tor(driver, None, use_tor=False, linkedin=True)
                            time.sleep(3) # Allow time for re-login.

                        first_name = df.at[idx, 'First Name'] if 'First Name' in df.columns else f"Row_{idx+1}"
                        company_url_original = df.at[idx, linkedin_column]

                        # Process only if company URL is valid and seems like a LinkedIn company URL.
                        if pd.notna(company_url_original) and "/company/" in company_url_original:
                            logging.info(f"Processing row {idx + 1}/{len(df)}: URL {company_url_original}")
                            
                            # Standardize company URL: remove trailing slashes, ensure it ends with /about.
                            company_url_for_about = company_url_original.rstrip('/').replace('/about', '')
                            company_url_for_about = f"{company_url_for_about}/about"
                            logging.info(f"Modified URL for scraping (row {idx + 1}): {company_url_for_about}")

                            # Call the core extraction function.
                            extraction_result, driver = extract_company_info(first_name, company_url_for_about, idx, driver)
                            
                            # Update DataFrame with extracted data.
                            df.at[idx, 'Website'] = extraction_result['Website']
                            df.at[idx, 'About_Text'] = extraction_result['About_Text']
                            # Mark as processed if a website was found. This logic might be refined
                            # to mark as processed even if website is None but About_Text was found,
                            # or simply after any attempt. Currently, it's based on website success.
                            df.at[idx, 'Processed_About_Website'] = extraction_result['Website'] != "None"
                        else:
                            logging.warning(f"Skipping invalid or missing LinkedIn company URL in row {idx + 1}: {company_url_original}")
                            df.at[idx, 'Processed_About_Website'] = False # Mark as not processable if URL is bad.
                        
                        # Save progress to the output CSV file after processing each row.
                        df.to_csv(output_csv, index=False)
                        logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
                        # Report progress for this row.
                        write_progress(idx + 1, total_rows_to_process_after_offset + offset, job_id, step_id=step_id)

                        time.sleep(random.uniform(1, 2)) # Small random delay between requests.
                except Exception as e:
                    logging.error(f"Error processing batch from index {batch_start_idx} to {batch_end_idx}: {e}", exc_info=True)
                finally:
                    # Ensure WebDriver for the batch is closed.
                    if driver is not None:
                        try:
                            driver.quit()
                            logging.info(f"WebDriver closed successfully for batch {batch_start_idx}-{batch_end_idx}.")
                        except Exception as e:
                            logging.error(f"Error closing WebDriver for batch {batch_start_idx}-{batch_end_idx}: {e}")
                        # kill_chrome_processes() # Optional: ensure all related browser processes are terminated.
                        time.sleep(2)  # Brief pause to ensure processes can terminate fully.
                
                if stopped: # If stop signal was received during row processing, break from batch loop too.
                    break

        finally: # This 'finally' is for the main try-catch block of the function.
            # This block executes regardless of exceptions in the processing loop,
            # but not if WebDriver is managed solely within the batch loop's try-finally.
            # The driver here would be the one from the last batch if not properly closed there.
            # However, current logic closes driver per batch. This might be redundant or a safeguard.
            if driver is not None: # Check if driver instance might still exist (e.g., if error before batch finally)
                try:
                    driver.quit()
                    time.sleep(2)
                    logging.info("WebDriver closed successfully in main finally block (safeguard).")
                except Exception as e:
                    logging.error(f"Error closing WebDriver in main finally block: {e}")
                # kill_chrome_processes()

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

        # Optionally delete rows where 'Website' is "None" after all processing is done (and not stopped early).
        if delete_no_website and not stopped:
            initial_row_count = len(df)
            df = df[df['Website'] != "None"] # Filter DataFrame
            deleted_rows_count = initial_row_count - len(df)
            if deleted_rows_count > 0:
                logging.info(f"Deleted {deleted_rows_count} rows where Website was 'None'.")
                df.to_csv(output_csv, index=False) # Save the filtered DataFrame.
                logging.info(f"Saved final DataFrame after deleting rows to {output_csv}")
            else:
                logging.info("No rows with Website 'None' found to delete, or `delete_no_website` was false.")

        logging.info(f"Final row count in DataFrame after processing: {len(df)}")
        return df

    except FileNotFoundError:
        logging.error(f"Input CSV file '{input_csv}' not found.")
        print(f"Error: Input CSV file '{input_csv}' not found.") # Also print to console for direct script execution.
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred in process_csv_and_extract_info: {e}", exc_info=True)
        print(f"Error processing CSV: {e}") # Also print to console.
        return None
    # Removed the redundant 'finally' block for progress writing here as it's covered by the 'stopped' logic.


# Example usage: This block executes if the script is run directly.
if __name__ == "__main__":
    # Configure paths for input and output CSV files using Config object.
    # Assumes Config.DATA_CSV_PATH points to a base directory for CSV data.
    input_file_name = "Test_3.csv" # Example input file name
    input_dir_path = os.path.join(Config.DATA_CSV_PATH, "updated_name") # Subdirectory for input
    output_dir_path = os.path.join(Config.DATA_CSV_PATH, "domain_about") # Subdirectory for output

    # Construct full input and output file paths.
    # Input file is expected to be prefixed, e.g., "Updated_Name_Filtered_Test_3.csv".
    # Output file will be similarly prefixed, e.g., "DomainAbout_Updated_Name_Filtered_Test_3.csv".
    full_input_csv_path = os.path.join(input_dir_path, f"Updated_Name_Filtered_{input_file_name}")
    full_output_csv_path = os.path.join(output_dir_path, f"DomainAbout_Updated_Name_Filtered_{input_file_name}")

    # Ensure output directory exists
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
        logging.info(f"Created output directory: {output_dir_path}")

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Call the main processing function with example parameters.
    process_csv_and_extract_info(
        input_csv=full_input_csv_path,
        output_csv=full_output_csv_path,
        max_rows=2000,       # Process up to 2000 rows from the CSV.
        batch_size=50,       # Process in batches of 50 rows.
        delete_no_website=True, # Remove rows if no website is found.
        offset=0,            # Start processing from the beginning of the CSV (no offset).
        job_id=str(uuid.uuid4()) # Generate a unique ID for this job run for tracking.
        # step_id is not specified here, will default to 'step5' in the function.
    )