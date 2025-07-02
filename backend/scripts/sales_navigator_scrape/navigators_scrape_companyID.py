import os
import re
import pandas as pd
from datetime import datetime
import logging
from backend.config import Config
from config.logging import setup_logging
# Define the fields to extract
fields = [
    "Email", "Full Name", "First Name", "Last Name", "Company Name", "Title",
    "Company Id", "Company Url", "Regular Company Url", "Summary", "Title Description",
    "Industry", "Company Location", "Location", "Duration In Role", "Duration In Company",
    "Past Experience Company Name", "Past Experience Company Url", "Past Experience Company Title",
    "Past Experience Date", "Past Experience Duration", "Connection Degree", "Profile Image Url",
    "Shared Connections Count", "Name", "Vmid", "Linked In Profile Url", "Is Premium",
    "Is Open Link", "Query", "Timestamp", "Default Profile Url"
]

def extract_company_info(lead_text):
    """
    Extract Company Id, Company Url, and Company Name from a lead text block.
    
    Args:
        lead_text (str): The text block containing lead information.
        
    Returns:
        tuple: (company_id, company_url, company_name)
    """
    match = re.search(
        r'<a\s+[^>]*data-anonymize\s*=\s*"company-name"[^>]*href\s*=\s*"\/sales\/company\/(\d+)[^"]*"[^>]*>(.*?)<\/a>',
        lead_text, re.IGNORECASE | re.DOTALL
    )
    if match:
        company_id = match.group(1).strip()
        company_name = re.sub(r'\s+', ' ', match.group(2).strip())  # Clean whitespace
        company_url = f"https://www.linkedin.com/company/{company_id}"
        logging.info(f"Extracted company info: ID={company_id}, Name={company_name}, URL={company_url}")
        return company_id, company_url, company_name
    logging.warning("No company info found in lead text")
    return "", "", ""

def clean_summary(text):
    """
    Clean HTML tags and extra text from summary text.
    
    Args:
        text (str): The raw summary text.
        
    Returns:
        str: Cleaned summary text.
    """
    if not text:
        logging.debug("No summary text provided")
        return ""
    # Extract text between <span style="display: inline;"> and </span> if present
    span_match = re.search(r'<span style="display: inline;">(.*?)<\/span>', text, re.DOTALL)
    if span_match:
        text = span_match.group(1)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove "…see more" or similar
    text = re.sub(r'…see more', '', text, flags=re.IGNORECASE)
    # Clean up extra whitespace
    text = ' '.join(text.strip().split())
    logging.debug(f"Cleaned summary: {text[:50]}...")  # Log first 50 chars
    return text

def parse_lead_block(lead_text):
    """
    Parse a single lead block and extract relevant fields.
    
    Args:
        lead_text (str): The text block for a single lead.
        
    Returns:
        dict or None: Extracted lead data as a dictionary, or None if invalid.
    """
    lead_data = {field: "" for field in fields}
    
    # Extract Full Name
    name_match = re.search(r'<span data-anonymize="person-name">([^<]+)</span>', lead_text)
    if name_match:
        lead_data["Full Name"] = name_match.group(1).strip()
        lead_data["Name"] = lead_data["Full Name"]
        # Split Full Name into First Name and Last Name
        name_parts = lead_data["Full Name"].split()
        if len(name_parts) >= 2:
            lead_data["First Name"] = name_parts[0]
            lead_data["Last Name"] = " ".join(name_parts[1:])
        elif len(name_parts) == 1:
            lead_data["First Name"] = name_parts[0]
        logging.info(f"Processing lead: {lead_data['Full Name'] or 'Unknown'}")
    
    # Extract Connection Degree
    connection_match = re.search(r'(\d+\w{2}\s+degree connection)', lead_text)
    if connection_match:
        lead_data["Connection Degree"] = connection_match.group(1)
        logging.debug(f"Connection Degree: {lead_data['Connection Degree']}")
    
    # Extract Title
    title_match = re.search(r'<span data-anonymize="title">([^<]+)</span>', lead_text)
    if title_match:
        lead_data["Title"] = title_match.group(1).strip()
        logging.debug(f"Title: {lead_data['Title']}")
    
    # Extract Company Name (from text, not href)
    company_match = re.search(r'·\s*([A-Za-z\s\d\&\/\-\.]+?)(?=\n)', lead_text)
    if company_match:
        lead_data["Company Name"] = company_match.group(1).strip()
        logging.debug(f"Company Name (text): {lead_data['Company Name']}")
    
    # Extract Company Id and Company Url
    company_id, company_url, company_name = extract_company_info(lead_text)
    lead_data["Company Id"] = company_id
    lead_data["Company Url"] = company_url
    lead_data["Regular Company Url"] = company_url
    if company_name:  # Prefer company name from href if available
        lead_data["Company Name"] = company_name
    
    # Skip if Company Id or Company Url is missing
    if not company_id or not company_url:
        logging.warning(f"Skipping lead '{lead_data['Full Name'] or 'Unknown'}': No company profile link found.")
        logging.debug(f"No href found in block:\n{lead_text[:200]}...")
        return None
    
    # Extract Location
    location_match = re.search(r'([A-Za-z\s,]+?,\s*[A-Za-z\s]+,\s*[A-Za-z\s]+|Greater [A-Za-z\s]+Metropolitan Area)', lead_text)
    if location_match:
        lead_data["Location"] = location_match.group(1).strip()
        lead_data["Company Location"] = lead_data["Location"]
        logging.debug(f"Location: {lead_data['Location']}")
    
    # Extract Duration In Role
    role_duration_match = re.search(r'(\d+\s+years?(?:\s+\d+\s+months?)?)\s+in role', lead_text)
    if role_duration_match:
        lead_data["Duration In Role"] = role_duration_match.group(1).strip()
        logging.debug(f"Duration In Role: {lead_data['Duration In Role']}")
    
    # Extract Duration In Company
    company_duration_match = re.search(r'(\d+\s+years?(?:\s+\d+\s+months?)?)\s+in company', lead_text)
    if company_duration_match:
        lead_data["Duration In Company"] = company_duration_match.group(1).strip()
        logging.debug(f"Duration In Company: {lead_data['Duration In Company']}")
    
    # Extract Summary
    summary_match = re.search(r'About:\s*([\s\S]+?)(?=\n\s*\n|\Z)', lead_text)
    if summary_match:
        lead_data["Summary"] = clean_summary(summary_match.group(1).strip())
    
    # Extract Industry
    industry_match = re.search(r'(Hospitality|Hotels and Motels|Travel Arrangements)', lead_text)
    if industry_match:
        lead_data["Industry"] = industry_match.group(1)
        logging.debug(f"Industry: {lead_data['Industry']}")
    
    # Set Timestamp
    lead_data["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.debug(f"Timestamp: {lead_data['Timestamp']}")
    
    return lead_data

def parse_sales_navigator(input_file, output_file, output_path=Config.DATA_CSV_PATH):
    """
    Parse LinkedIn Sales Navigator data from a text file and save to a CSV.
    
    Args:
        input_file (str): Path to the input text file.
        output_file (str): Name of the output CSV file.
        output_path (str): Directory path for the output CSV file.
        
    Returns:
        pd.DataFrame or None: The resulting DataFrame, or None if an error occurs.
    """

    logging.info(f"Starting parse_sales_navigator with input_file={input_file}, output_file={output_file}, output_path={output_path}")
    
    try:
        # Read the input file
        logging.info(f"Reading input file: {input_file}")
        with open(input_file, "r", encoding="utf-8") as file:
            input_text = file.read()
    except FileNotFoundError:
        logging.error(f"Input file '{input_file}' not found")
        print(f"Error: Input file '{input_file}' not found.")
        return None
    except Exception as e:
        logging.error(f"Error reading input file '{input_file}': {e}")
        print(f"Error reading input file '{input_file}': {e}")
        return None

    # Initialize list to store lead data
    leads = []
    logging.info("Splitting input text into lead blocks")
    # Split the input text into individual lead entries
    lead_blocks = re.split(r'(?=<span data-anonymize="person-name">)', input_text)
    lead_blocks = [block for block in lead_blocks if '<span data-anonymize="person-name">' in block]
    logging.info(f"Found {len(lead_blocks)} lead blocks")

    # Parse each lead block
    for i, lead_text in enumerate(lead_blocks, 1):
        logging.info(f"Parsing lead block {i}/{len(lead_blocks)}")
        lead_data = parse_lead_block(lead_text)
        if lead_data:
            leads.append(lead_data)
            logging.info(f"Successfully parsed lead: {lead_data['Full Name'] or 'Unknown'}")
        else:
            logging.warning(f"Skipped lead block {i} due to missing company info")

    # Convert to DataFrame
    logging.info(f"Converting {len(leads)} leads to DataFrame")
    new_df = pd.DataFrame(leads, columns=fields)

    # Construct full output path
    full_output_path = os.path.join(output_path, output_file)
    logging.info(f"Output path: {full_output_path}")

    # Handle existing CSV and deduplication
    if os.path.exists(full_output_path):
        logging.info(f"Existing CSV found at {full_output_path}, loading for deduplication")
        try:
            existing_df = pd.read_csv(full_output_path, dtype=str, keep_default_na=False)
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=["Full Name"], keep="last").reset_index(drop=True)
            logging.info(f"Deduplicated {len(existing_df)} existing rows with {len(new_df)} new rows, resulting in {len(combined_df)} rows")
        except Exception as e:
            logging.error(f"Error reading existing CSV '{full_output_path}': {e}")
            print(f"Error reading existing CSV '{full_output_path}': {e}")
            return None
    else:
        logging.info("No existing CSV found, using new data")
        combined_df = new_df

    # Save to CSV
    try:
        logging.info(f"Saving DataFrame to {full_output_path}")
        os.makedirs(output_path, exist_ok=True)
        combined_df.to_csv(full_output_path, index=False)
        logging.info(f"Leads processed and appended to {full_output_path}")
        print(f"Leads processed and appended to {full_output_path}")
        return combined_df
    except Exception as e:
        logging.error(f"Error saving CSV to '{full_output_path}': {e}")
        print(f"Error saving CSV to '{full_output_path}': {e}")
        return None

if __name__ == "__main__":
    input_file = "Sales_Navigator.txt"
    output_file = "Test.csv"
    output_path = "data/csv/"
    parse_sales_navigator(input_file, output_file, output_path)