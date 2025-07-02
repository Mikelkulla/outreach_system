import sys
import os
from dotenv import load_dotenv
from openai import OpenAI
import pandas as pd
import logging
from datetime import datetime
import time
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed
from backend.config import Config

def find_the_correct_name(lead_name, temperature=0.7, org_id=None, max_retries=3, initial_delay=1):
    """
    Formats a lead name by removing emojis, fixing capitalization, and stripping titles/credentials using OpenAI's Chat Completion API.
    Implements retry logic for rate limit errors with exponential backoff.
    
    Parameters:
        lead_name (str): The lead name to format (e.g., 'Josh Bartlome 💪🔍').
        temperature (float): Sampling temperature for OpenAI API (default: 0.7).
        org_id (str, optional): OpenAI organization ID, if required.
        max_retries (int): Maximum number of retry attempts for rate limit errors (default: 3).
        initial_delay (float): Initial delay in seconds for exponential backoff (default: 1).
    
    Returns:
        str: Formatted name, or None if an error occurs after retries.
    """
    try:
        # Load OpenAI API key and optional organization ID from .env file
        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key not found in .env file.")

        # Initialize OpenAI client
        client = OpenAI(api_key=api_key, organization=org_id)

        # Check if lead_name is valid
        if not lead_name or not isinstance(lead_name, str):
            logging.warning(f"Invalid or empty lead name: {lead_name}")
            return None

        # Truncate lead_name to fit within token limits
        max_text_length = 256  # Approx. 1500 chars to stay within 256 tokens with prompt
        lead_name = lead_name[:max_text_length]

        # Define system and user messages
        system_message = {
            "role": "system",
            "content": "You are an expert, intelligent writing assistant."
        }
        user_message = {
            "role": "user",
            "content": f"""
            Format the following lead name by removing emojis, fixing capitalization, and stripping titles/credentials. Output only the formatted name.

            Examples:
            Input: Josh Bartlome' 💪🔍
            Output: Josh Bartlome

            Input: Sia (Athanasia) Dimaggio, NYS Real Estate Broker, Owner,CRS,ABR
            Output: Sia Dimaggio

            Input: Akash B.
            Output: Akash B

            Input: Cynthia Tant, PhD, WCR
            Output: Cynthia Tant

            Input: Tamairo Moutry-CEO/Real Estate Broker, WI, FL, GA, and Real Estate Investor
            Output: Tamairo Moutry

            Input: https://linkedin.com/in/brucebradyatl
            Output: Bruce Brady
            
            Lead Name to format:
            {lead_name}
            """
        }

        # Retry loop for rate limit errors
        for attempt in range(max_retries + 1):
            try:
                # Call OpenAI Chat Completion API
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[system_message, user_message],
                    max_tokens=128,
                    temperature=temperature
                )

                # Extract the formatted name
                formatted_name = response.choices[0].message.content.strip()
                logging.info(f"Successfully formatted '{lead_name}' to '{formatted_name}'")
                return formatted_name

            except openai.RateLimitError as e:
                if attempt < max_retries:
                    delay = initial_delay * (2 ** attempt)  # Exponential backoff: 1s, 2s, 4s, etc.
                    logging.warning(f"Rate limit error for '{lead_name}', attempt {attempt + 1}/{max_retries}. Waiting {delay} seconds: {e}")
                    time.sleep(delay)
                    continue
                else:
                    logging.error(f"Max retries reached for '{lead_name}': {e}")
                    return None

            except Exception as e:
                logging.error(f"Error formatting name '{lead_name}': {e}")
                return None

    except Exception as e:
        logging.error(f"Error initializing API for '{lead_name}': {e}")
        return None

def process_row(idx, name, temperature, org_id, max_retries):
    """
    Helper function to process a single row's Full Name.
    
    Parameters:
        idx (int): Index of the row in the DataFrame.
        name (str): The Full Name to format.
        temperature (float): Sampling temperature for OpenAI API.
        org_id (str): OpenAI organization ID.
        max_retries (int): Maximum number of retry attempts for rate limit errors.
    
    Returns:
        tuple: (idx, formatted_name, processed_status)
    """
    if pd.notnull(name):
        formatted_name = find_the_correct_name(
            name,
            temperature=temperature,
            org_id=org_id,
            max_retries=max_retries
        )
        return idx, formatted_name, formatted_name is not None
    return idx, None, False

def process_csv(input_csv, output_csv=None, input_path=os.path.join(Config.DATA_CSV_PATH, "filtered_url"), output_path=os.path.join(Config.DATA_CSV_PATH, "updated_name"), batch_size=100, temperature=0.7, max_retries=3, n_threads=10):
    """
    Reads a CSV file, processes each row's Full Name through find_the_correct_name in batches using threads,
    and saves the updated CSV with formatted names and a Processed_Name column.
    
    Parameters:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to save the updated CSV file.
        input_path (str): Directory of the input CSV.
        output_path (str): Directory to save the output CSV.
        batch_size (int): Number of rows to process per batch.
        temperature (float): Sampling temperature for OpenAI API.
        max_retries (int): Maximum number of retry attempts for rate limit errors.
        n_threads (int): Number of threads to use for parallel processing (default: 10).
    
    Returns:
        pd.DataFrame or None: The processed DataFrame, or None if an error occurs.
    """

    try:
        if output_csv is None:
            output_csv = f"Updated_Name_{input_csv}"

        # Construct full input and output paths
        input_file = os.path.join(input_path, input_csv)
        output_file = os.path.join(output_path, output_csv)

        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Load environment variables
        load_dotenv()
        org_id = os.getenv("OPENAI_ORG_ID")

        # Check if output file exists and use it as input if available
        if os.path.exists(output_file):
            logging.info(f"Output file '{output_file}' exists, using it as input")
            input_file = output_file
        else:
            logging.info(f"No output file found, using input file '{input_file}'")

        # Read the CSV file
        logging.info(f"Reading CSV: {input_file}")
        df = pd.read_csv(input_file, dtype=str, keep_default_na=False)

        # Check if Full Name column exists
        if 'Full Name' not in df.columns:
            raise ValueError("CSV file must contain a 'Full Name' column.")

        # Initialize Processed_Name column only for rows where it's missing
        if 'Processed_Name' not in df.columns:
            logging.info("Processed_Name column not found, creating new column with False values")
            df['Processed_Name'] = False
        else:
            # Convert Processed_Name to boolean, preserving existing True/False values
            df['Processed_Name'] = df['Processed_Name'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)
            logging.info(f"Found Processed_Name column with {df['Processed_Name'].sum()} processed rows")

        # Process rows in batches
        total_rows = len(df)
        logging.info(f"Total rows to process: {total_rows}")
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]
            
            # Skip already processed rows
            unprocessed_mask = ~batch['Processed_Name']
            if not unprocessed_mask.any():
                logging.info(f"Batch {start_idx}-{end_idx} already processed, skipping.")
                continue

            logging.info(f"Processing batch {start_idx}-{end_idx} of {total_rows} rows with {n_threads} threads")
            
            # Process unprocessed Full Names in the batch using threads
            with ThreadPoolExecutor(max_workers=n_threads) as executor:
                # Submit tasks for unprocessed rows
                future_to_idx = {
                    executor.submit(
                        process_row,
                        idx,
                        df.at[idx, 'Full Name'],
                        temperature,
                        org_id,
                        max_retries
                    ): idx
                    for idx in batch[unprocessed_mask].index
                }

                # Collect results
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        idx, formatted_name, processed = future.result()
                        if processed:
                            df.at[idx, 'Full Name'] = formatted_name
                            df.at[idx, 'Processed_Name'] = True
                        else:
                            df.at[idx, 'Processed_Name'] = False
                    except Exception as e:
                        logging.error(f"Error processing row {idx}: {e}")
                        df.at[idx, 'Processed_Name'] = False

            # Save progress after each batch
            df.to_csv(output_file, index=False)
            logging.info(f"Saved batch {start_idx}-{end_idx} to {output_file}")

        print(f"Updated CSV saved to {output_file}")
        return df

    except FileNotFoundError:
        logging.error(f"Input CSV file '{input_file}' not found.")
        print(f"Error: Input CSV file '{input_file}' not found.")
        return None
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        print(f"Error processing CSV: {e}")
        return None