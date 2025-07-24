# My Project

## Project Description

This project is a web application that helps users to generate icebreakers for sales outreach. The application is divided into a frontend and a backend. The frontend is a single-page application that allows users to upload a CSV file with a list of leads. The backend is a Flask application that processes the CSV file and generates icebreakers for each lead.

## How to configure & run the project

### Prerequisites

*   Python 3.9+
*   Pip

### Installation

1.  Clone the repository:

    ```bash
    git clone https://your-repository-url.git
    ```

2.  Install the dependencies:

    ```bash
    pip install -r requirements.txt
    ```

### Running the application

```bash
python backend/app.py
```

## Description of API endpoints

### `POST /api/upload`

Handles file uploads. Expects a file in the 'file' part of a multipart/form-data request. Saves the uploaded file to the directory specified by `Config.DATA_CSV_PATH`.

### `POST /api/steps/<int:step>`

Main endpoint to trigger various data processing steps. The behavior depends on the 'step' number provided in the URL. Steps 1, 2, 3 are synchronous. Steps 5, 6, 7, 8 are asynchronous, run in background threads, and their status can be tracked.

### `POST /api/stop/<int:step>`

Stops a running asynchronous job for steps 5, 6, 7, or 8. It creates a 'stop_stepX.txt' signal file that the background script should check. It also updates the job's status in the `jobs_stepX.json` and progress files.

### `GET /api/progress/<int:step>`

Retrieves the progress of an asynchronous job (steps 5, 6, 7). Requires a 'job_id' query parameter for specific job progress. Reads progress from a `progress_stepX_jobY.json` file.

### `GET /api/jobs/<int:step>`

Lists all recorded jobs and their statuses for a specific asynchronous step (5, 6, or 7). Reads data from the corresponding `jobs_stepX.json` file.

### `GET /api/files/<path:folder>`

Lists all CSV files in a specified folder within the data directory. The 'folder' path parameter can be a subfolder name or 'csv' for the root data CSV path.

### `GET /api/logs`

Lists all log files (ending with '.log') from the configured log directory.

## Description of each function i have created

### `backend/app.py`

-   **`index()`**: Renders the `index.html` template.

### `backend/config.py`

-   **`init_dirs()`**: Creates the necessary directories for the application to run.
-   **`verify_drivers()`**: Verifies that the driver executables exist.

### `backend/routes/api.py`

-   **`update_job_status(step, job_id, status)`**: Updates the status of a job in the `jobs_stepX.json` file.
-   **`upload_file()`**: Handles file uploads.
-   **`run_step(step)`**: Main endpoint to trigger various data processing steps.
-   **`stop_step(step)`**: Stops a running asynchronous job.
-   **`get_progress(step)`**: Retrieves the progress of an asynchronous job.
-   **`get_jobs(step)`**: Lists all recorded jobs and their statuses for a specific asynchronous step.
-   **`list_files(folder)`**: Lists all CSV files in a specified folder.
-   **`get_logs()`**: Lists all log files.

### `backend/scripts/openai/correctname_finder.py`

-   **`find_the_correct_name(lead_name, temperature=0.7, org_id=None, max_retries=3, initial_delay=1)`**: Formats a lead name by removing emojis, fixing capitalization, and stripping titles/credentials using OpenAI's Chat Completion API.
-   **`process_row(idx, name, temperature, org_id, max_retries)`**: Helper function to process a single row's Full Name.
-   **`process_csv(input_csv, output_csv=None, input_path=os.path.join(Config.DATA_CSV_PATH, "filtered_url"), output_path=os.path.join(Config.DATA_CSV_PATH, "updated_name"), batch_size=100, temperature=0.7, max_retries=3, n_threads=10)`**: Reads a CSV file, processes each row's Full Name through `find_the_correct_name` in batches using threads, and saves the updated CSV with formatted names and a `Processed_Name` column.

### `backend/scripts/openai/icebreaker_generator.py`

-   **`generate_icebreaker(cleaned_text, openAI_client, system_message, user_message_role, user_message_content, temperature=0.7)`**: Generates a personalized icebreaker using OpenAI's Chat Completion API based on provided system and user messages.
-   **`process_csv_and_generate_icebreaker(input_csv, output_csv, max_rows=2000, batch_size=50, agent_prompt='default_agent', delete_no_icebreaker=False, offset=0, job_id=None, step_id='step8')`**: Processes a CSV file containing LinkedIn profile data, generates personalized icebreakers using OpenAI, and saves the enriched data to an output CSV.

### `backend/scripts/sales_navigator_scrape/email_finder.py`

-   **`find_email(full_name, company_name, driver, tor_process=None, max_retries=2, retry_delay=2)`**: Finds an email on Skrapp.io Email Finder for a given name and company with up to 2 retries.
-   **`process_csv_and_find_emails(input_csv, output_csv, max_rows=2000, batch_size=50, tor_restart_interval=30, offset=0, delete_no_email=True, job_id=None, step_id='step6')`**: Processes a CSV file, finds emails using Skrapp.io, and updates the CSV with Email and Status columns.

### `backend/scripts/sales_navigator_scrape/extract_company_about_website.py`

-   **`extract_company_info(first_name, company_url, index, driver, max_retries=3, retry_delay=3)`**: Extracts the "About" section text and website domain from a given LinkedIn company URL.
-   **`process_csv_and_extract_info(input_csv, output_csv, max_rows=2000, batch_size=50, delete_no_website=True, offset=0, job_id=None, step_id='step5')`**: Processes a CSV file containing LinkedIn company URLs, extracts "About" text and website domains, and saves the enriched data to an output CSV.

### `backend/scripts/sales_navigator_scrape/navigators_scrape_companyID.py`

-   **`extract_company_info(lead_text)`**: Extract Company Id, Company Url, and Company Name from a lead text block.
-   **`clean_summary(text)`**: Clean HTML tags and extra text from summary text.
-   **`parse_lead_block(lead_text)`**: Parse a single lead block and extract relevant fields.
-   **`parse_sales_navigator(input_file, output_file, output_path=Config.DATA_CSV_PATH)`**: Parse LinkedIn Sales Navigator data from a text file and save to a CSV.

### `backend/scripts/sales_navigator_scrape/remove_empty_companyurl.py`

-   **`remove_empty_company_rows(input_csv, output_csv=None, input_path=Config.DATA_CSV_PATH, output_path=os.path.join(Config.DATA_CSV_PATH, "filtered_url"))`**: Removes rows from a CSV file where Company Id or Company Url is empty.

### `backend/scripts/sales_navigator_scrape/update_company_urls.py`

-   **`get_company_url(company_id_url, driver)`**: Fetches the redirected LinkedIn company URL (name-based) from an ID-based URL.
-   **`process_csv_and_update_urls(input_csv, output_csv, max_rows=1000, batch_size=10)`**: Processes a CSV file, updates 'Regular Company Url' with name-based URLs, and saves progress in real-time.

### `backend/scripts/sales_navigator_scrape/update_company_urls_with_school_fix.py`

-   **`check_stop_signal()`**: Checks if a stop signal file exists for Step 4.
-   **`write_progress(current_row, total_rows)`**: Writes progress to a JSON file for Step 4.
-   **`get_company_url(company_id_url, driver)`**: Fetches the redirected LinkedIn company URL (name-based) from an ID-based URL.
-   **`process_csv_and_update_urls(input_csv, output_csv, max_rows=2000, batch_size=10)`**: Processes a CSV file, updates 'Regular Company Url' with name-based URLs, deletes school-related rows, and saves progress in real-time.

### `backend/scripts/sales_navigator_scrape/verify_emails.py`

-   **`verify_email_neverbounce()`**: Placeholder function for verifying an email using NeverBounce.
-   **`verify_email_scrapp(email, driver, tor_process=None, max_retries=2, retry_delay=2)`**: Verifies an email on Skrapp.io Email Verifier with up to 2 retries, checking only the verification message.
-   **`process_csv_and_verify_emails(input_csv, output_csv, max_rows=2000, batch_size=50, tor_restart_interval=30, offset=0, delete_invalid=True, job_id=None, step_id='step7')`**: Processes a CSV file to verify email addresses contained within it using the Skrapp.io service.

### `backend/scripts/selenium/driver_setup_for_scrape.py`

-   **`restart_driver_and_tor(driver, tor_process, use_tor=False, linkedin=False, chromedriver_path=Config.CHROMEDRIVER_PATH, tor_path=Config.TOR_EXECUTABLE, headless=False)`**: Restarts both the WebDriver and Tor process (if applicable) with proper cleanup and reinitialization.
-   **`setup_driver(chromedriver_path=Config.CHROMEDRIVER_PATH, browser="chrome", headless=False)`**: Sets up a WebDriver (Chrome or Firefox) with anti-detection measures to appear less like a bot.
-   **`setup_driver_linkedin_singin(chromedriver_path=Config.CHROMEDRIVER_PATH, browser="chrome", headless=False)`**: Sets up a WebDriver with LinkedIn Chrome profile for authentication.
-   **`start_tor(tor_path=Config.TOR_EXECUTABLE)`**: Starts the Tor process.
-   **`stop_tor(tor_process)`**: Stops the Tor process and its children.
-   **`restart_tor(tor_process, tor_path=Config.TOR_EXECUTABLE)`**: Restarts the Tor process.
-   **`setup_chrome_with_tor(chromedriver_path=Config.CHROMEDRIVER_PATH, headless=False)`**: Setup Chrome WebDriver routed through Tor SOCKS5 proxy (127.0.0.1:9050).
-   **`setup_firefox_with_tor(geckodriver_path=Config.GECKODRIVER_PATH, headless=False)`**: Setup Firefox WebDriver routed through Tor SOCKS5 proxy (127.0.0.1:9050).
-   **`kill_chrome_processes()`**: Kill all Chrome processes that might be locking the user data directory.

## Description of how to collaborate as an open source project

We welcome contributions to this project! Please follow these guidelines:

-   Fork the repository.
-   Create a new branch for your feature or bug fix.
-   Make your changes and commit them with a descriptive message.
-   Push your changes to your fork.
-   Create a pull request.
