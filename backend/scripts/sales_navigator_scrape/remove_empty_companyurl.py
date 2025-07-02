import pandas as pd
import logging
import os
from backend.config import Config
from config.utils import load_csv


def remove_empty_company_rows(input_csv, output_csv=None, input_path=Config.DATA_CSV_PATH, output_path=os.path.join(Config.DATA_CSV_PATH, "filtered_url")):
    """
    Removes rows from a CSV file where Company Id or Company Url is empty.
    
    Parameters:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to save the filtered CSV file (default: 'Filtered_' + input_csv).
        input_path (str): Directory of the input CSV.
        output_path (str): Directory to save the filtered CSV.
    
    Returns:
        pd.DataFrame or None: The filtered DataFrame, or None if an error occurs.
    """

    try:
        if output_csv is None:
            output_csv = f"Filtered_{input_csv}"

        # Construct full input and output paths
        input_file = os.path.join(input_path, input_csv)
        output_file = os.path.join(output_path, output_csv)

        # Use load_csv function
        df, resolved_input_file = load_csv(
            input_csv=input_file,
            output_csv=output_file,
            required_columns=['Company Id', 'Company Url']
        )
        if df is None:
            return None

        # Log initial row count
        initial_rows = len(df)
        logging.info(f"Initial row count: {initial_rows}")

        # Filter rows where both Company Id and Company Url are non-empty
        filtered_df = df[df['Company Id'].str.strip() != '']
        filtered_df = filtered_df[filtered_df['Company Url'].str.strip() != '']
        
        # Log filtered row count
        filtered_rows = len(filtered_df)
        logging.info(f"Filtered row count: {filtered_rows} (removed {initial_rows - filtered_rows} rows)")

        # Save the filtered DataFrame to a new CSV
        os.makedirs(output_path, exist_ok=True)
        filtered_df.to_csv(output_file, index=False)
        logging.info(f"Filtered CSV saved to {output_file}")
        print(f"Filtered CSV saved to {output_file}")

        return filtered_df

    except FileNotFoundError:
        logging.error(f"Input CSV file '{input_file}' not found.")
        print(f"Error: Input CSV file '{input_file}' not found.")
        return None
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        print(f"Error processing CSV: {e}")
        return None