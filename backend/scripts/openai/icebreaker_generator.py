import json
import logging
import random
import os
import time
from dotenv import load_dotenv
from openai import OpenAI
import pandas as pd
from backend.config import Config
from config.job_functions import check_stop_signal, write_progress
from config.utils import load_csv

def generate_icebreaker(cleaned_text, openAI_client, system_message, user_message_role, user_message_content, temperature=0.7):
    """
    Generates a personalized icebreaker using OpenAI's Chat Completion API based on provided system and user messages.

    Parameters:
        cleaned_text (str): Cleaned text from LinkedIn company or individual profile.
        openAI_client: Initialized OpenAI client instance.
        system_message (dict): System message with role and content for the OpenAI API.
        user_message_role (str): Role for the user message (e.g., 'user').
        user_message_content (str): Content of the user message template, to be formatted with cleaned_text.
        temperature (float): Sampling temperature for OpenAI API (default: 0.7).

    Returns:
        str: Personalized icebreaker text, or None if an error occurs.
    """
    try:
        # Truncate cleaned text to fit within input token limits (approx. 4000 chars for ~1000 tokens, leaving room for prompt)
        max_text_length = 4000
        cleaned_text = cleaned_text[:max_text_length]

        user_message = {
            "role": user_message_role,
            "content": f"{user_message_content}\nCompany Profile About Text:\n{cleaned_text}"
        }

        # Call OpenAI Chat Completion API
        response = openAI_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[system_message, user_message],
            max_tokens=100,  # Suitable for concise 1-2 sentence icebreakers
            temperature=temperature
        )

        # Extract the generated icebreaker
        icebreaker = response.choices[0].message.content.strip()
        logging.info(icebreaker)
        return icebreaker

    except Exception as e:
        logging.error(f"Error generating icebreaker: {e}", exc_info=True)
        return None

# def process_csv_and_generate_icebreaker(input_csv, output_csv, max_rows=2000, batch_size=50, agent_prompt='default_agent', delete_no_icebreaker=False, offset=0, job_id=None, step_id='step8'):
#     """
#     Processes a CSV file containing LinkedIn profile data, generates personalized icebreakers using OpenAI, and saves the enriched data to an output CSV.

#     This function manages the overall workflow: loading data, iterating through rows in batches, invoking `generate_icebreaker` for each row, saving progress, and optionally cleaning up rows where no icebreaker was generated.

#     Parameters:
#     -----------
#     input_csv (str): Path to the input CSV file. Must contain columns 'Summary' and 'About_Text'.
#     output_csv (str): Path where the updated CSV file will be saved.
#     max_rows (int): Maximum number of rows from the input CSV to process.
#     batch_size (int): Number of rows to process before logging and saving progress.
#     agent_prompt (str): Name of the agent prompt to use (e.g., 'hospitality', 'default_agent').
#     delete_no_icebreaker (bool): If True, rows where 'Icebreaker' remains "None" after processing will be removed from the final output CSV.
#     offset (int): Number of rows to skip from the beginning of the input CSV.
#     job_id (str): A unique identifier for the current processing job (for external tracking).
#     step_id (str): Identifier for the current step in a larger job pipeline (for external tracking).

#     Returns:
#     --------
#     pd.DataFrame or None: The updated DataFrame with generated icebreakers, or None if a critical error occurs during setup (e.g., file not found).
#     """
#     try:
#         # Load agent prompts from JSON file
#         with open(os.path.join(Config.AGENT_PROMPTS_PATH), 'r', encoding='utf-8') as f:
#             agents = json.load(f)

#         logging.info(f"Agents: {agents}")
#         # Define system and user messages from .json file
#         system_message = agents.get(agent_prompt).get("system_message")
#         logging.info(f"System Message: {system_message}")

#         user_message_role = agents.get(agent_prompt).get("user_message").get("role")
#         logging.info(f"User Message Role: {user_message_role}")

#         user_message_content = agents.get(agent_prompt).get("user_message").get("content")
#         logging.info(f"User Message Content: {user_message_content}")

#         # Load the input CSV file using a utility function.
#         # Needs Summary and About_Text columns to exists
#         required_columns = ['Summary', 'About_Text']
#         df, resolved_input_csv = load_csv(
#             input_csv=input_csv,
#             output_csv=output_csv,
#             required_columns=required_columns
#         )
#         if df is None: # load_csv returns None on failure (e.g., file not found)
#             return None

#         # Load OpenAI API key and optional organization ID from .env file
#         load_dotenv()
#         api_key = os.getenv("OPENAI_API_KEY")
#         org_id = os.getenv("OPENAI_ORG_ID")
#         openAI_client = OpenAI(api_key=api_key, organization=org_id)

#         if not api_key:
#             raise ValueError("OpenAI API key not found in .env file.")

#         # Initialize new columns in the DataFrame if they don't already exist.
#         if 'Icebreaker' not in df.columns:
#             df['Icebreaker'] = "None"  # To store generated icebreakers
#         # 'Processed_Icebreaker' tracks if a row has been attempted.
#         # This helps in resuming interrupted jobs and skipping already processed rows.
#         if 'Processed_Icebreaker' not in df.columns:
#             df['Processed_Icebreaker'] = False
#         else:
#             # Ensure 'Processed_Icebreaker' is boolean, handling string representations if loaded from CSV.
#             df['Processed_Icebreaker'] = df['Processed_Icebreaker'].map(
#                 {'True': True, 'False': False, True: True, False: False}
#             ).fillna(False)
#             logging.info(f"Found 'Processed_Icebreaker' column with {df['Processed_Icebreaker'].sum()} rows already marked as processed.")

#         # Initialize progress reporting for external monitoring.
#         # Reports total rows to be processed (considering max_rows and actual df length).
#         write_progress(0, min(len(df), max_rows), job_id, step_id=step_id)

#         # Apply offset: skip the first 'offset' number of rows.
#         if offset < 0:
#             logging.error("Offset cannot be negative.")
#             raise ValueError("Offset cannot be negative")
#         if offset >= len(df):
#             logging.info(f"Offset {offset} is greater than or equal to DataFrame length {len(df)}. No rows to process.")
#             return df # Return the original DataFrame as no processing is needed.

#         # Calculate the total number of rows to actually process, considering offset and max_rows.
#         total_rows_to_process_after_offset = min(len(df) - offset, max_rows)
#         if total_rows_to_process_after_offset <= 0:
#             logging.info(f"No rows to process after applying offset {offset} and max_rows {max_rows}.")
#             return df

#         logging.info(f"Total rows to process (after offset {offset}, up to max_rows {max_rows}): {total_rows_to_process_after_offset}")

#         stopped = False # Flag to indicate if processing was stopped by an external signal.
#         try:
#             # Process rows in batches for better resource management and logging.
#             # The outer loop iterates from 'offset' up to 'offset + total_rows_to_process_after_offset'.
#             for batch_start_idx in range(offset, offset + total_rows_to_process_after_offset, batch_size):
#                 # Check for an external stop signal before starting a new batch.
#                 if check_stop_signal(step_id):
#                     logging.info("Stop signal detected. Terminating processing.")
#                     # Report current progress before stopping.
#                     write_progress(batch_start_idx, offset + total_rows_to_process_after_offset, job_id, step_id=step_id, stop_call=True)
#                     df.to_csv(output_csv, index=False)
#                     stopped = True
#                     break # Exit the batch processing loop.

#                 batch_end_idx = min(batch_start_idx + batch_size, offset + total_rows_to_process_after_offset)
#                 current_batch_df_slice = df.iloc[batch_start_idx:batch_end_idx]
                
#                 # Identify rows within the current batch that haven't been processed yet.
#                 unprocessed_mask = ~current_batch_df_slice['Processed_Icebreaker']
#                 if not unprocessed_mask.any():
#                     logging.info(f"Batch from index {batch_start_idx} to {batch_end_idx} already processed. Skipping.")
#                     write_progress(batch_end_idx, offset + total_rows_to_process_after_offset, job_id, step_id=step_id)
#                     continue # Move to the next batch.

#                 logging.info(f"Processing batch: rows from index {batch_start_idx} to {batch_end_idx} (out of {offset + total_rows_to_process_after_offset} total to process).")
                
#                 try:
#                     # Iterate through each row in the current batch that needs processing.
#                     # 'idx' here is the original DataFrame index.
#                     for idx in current_batch_df_slice[unprocessed_mask].index:
#                         # Check for stop signal before processing each row.
#                         if check_stop_signal(step_id):
#                             logging.info(f"Stop signal detected during row processing at index {idx + 1}. Terminating.")
#                             write_progress(idx, offset + total_rows_to_process_after_offset, job_id, step_id=step_id, stop_call=True)
#                             df.to_csv(output_csv, index=False) # Save progress.
#                             stopped = True
#                             break # Exit the inner loop (row processing).

#                         about_text = df.at[idx, 'About_Text'] if 'About_Text' in df.columns else None
#                         summary = df.at[idx, 'Summary'] if 'Summary' in df.columns else None
#                         first_name = df.at[idx, 'First Name'] if 'First Name' in df.columns else f"Row_{idx+1}"

#                         # Select text for icebreaker (About_Text, Summary, or empty string for generic icebreaker)
#                         if pd.notna(about_text):
#                             logging.info(f"Processing with **About Text**, row {idx + 1}/{len(df)}: Name - {first_name}")
#                             icebreaker_text = about_text
#                         elif pd.notna(summary):
#                             logging.info(f"Processing with **Summary**, row {idx + 1}/{len(df)}: Name - {first_name}")
#                             icebreaker_text = summary
#                         else:
#                             logging.info(f"No valid About_Text or Summary for row {idx + 1}: {first_name}. Generating generic icebreaker.")
#                             icebreaker_text = ""  # Empty string to trigger generic icebreaker

#                         # Call the icebreaker generator function.
#                         generated_icebreaker = generate_icebreaker(icebreaker_text, openAI_client, system_message, user_message_role, user_message_content)

#                         # Update DataFrame with generated icebreaker.
#                         df.at[idx, 'Icebreaker'] = generated_icebreaker if generated_icebreaker else "None"

#                         # Mark as processed if icebreaker generated succesfully.
#                         df.at[idx, 'Processed_Icebreaker'] = generated_icebreaker != "None"

#                         # Save progress to the output CSV file after processing each row.
#                         df.to_csv(output_csv, index=False)
#                         logging.info(f"Saved progress for row {idx + 1} to {output_csv}")
#                         # Report progress for this row.
#                         write_progress(idx + 1, total_rows_to_process_after_offset + offset, job_id, step_id=step_id)
#                         time.sleep(random.uniform(0.1, 1))

#                 except Exception as e:
#                     logging.error(f"Error processing batch from index {batch_start_idx} to {batch_end_idx}: {e}", exc_info=True)
                
#                 if stopped: # If stop signal was received during row processing, break from batch loop too.
#                     break

#         finally: # This 'finally' is for the main try-catch block of the function.
#             # Report final progress status (completed or stopped).
#             if not stopped:
#                 # Determine the final processed row count for progress reporting.
#                 # If completed, it's total_rows_to_process_after_offset.
#                 # If stopped, it's based on the last processed index.
#                 final_status = "stopped" if check_stop_signal(step_id) else "completed"
#                 # Calculate the number of rows processed from the perspective of the 'offset' start.
#                 # If df is empty or offset is beyond df length, df.index[-1] would error.
#                 last_processed_row_absolute_index = df.index[-1] + 1 if not df.empty and offset < len(df) else offset
#                 # Effective rows processed in this run.
#                 effective_processed_count = max(0, last_processed_row_absolute_index)

#                 final_row_for_progress = (total_rows_to_process_after_offset + offset) if final_status == "completed" else effective_processed_count

#                 logging.info(f"Final Status: {final_status}. Reporting progress for {final_row_for_progress}/{total_rows_to_process_after_offset + offset} effective rows.")
#                 write_progress(final_row_for_progress, total_rows_to_process_after_offset + offset, job_id, step_id=step_id, stop_call=(final_status == "stopped"))

#             # Optionally delete rows where 'Icebreaker' is "None"
#             if delete_no_icebreaker and not stopped:
#                 initial_row_count = len(df)
#                 df = df[df['Icebreaker'] != "None"]
#                 deleted_rows_count = initial_row_count - len(df)
#                 if deleted_rows_count > 0:
#                     logging.info(f"Deleted {deleted_rows_count} rows where Icebreaker was 'None'.")
#                     df.to_csv(output_csv, index=False)
#                     logging.info(f"Saved final DataFrame after deleting rows to {output_csv}")
#                 else:
#                     logging.info("No rows with Icebreaker 'None' found to delete.")

#         logging.info(f"Final row count in DataFrame after processing: {len(df)}")
#         return df

#     except ValueError as v_error:
#         logging.error(f"Value Error: {v_error}")
#         return None
#     except FileNotFoundError:
#         logging.error(f"Input CSV file '{input_csv}' not found.")
#         return None
#     except json.JSONDecodeError as e:
#         logging.error(f"Invalid JSON in agent_prompts.json: {e}")
#         print(f"Error: Invalid JSON in agent_prompts.json: {e}")
#         return None
#     except UnicodeDecodeError as e:
#         logging.error(f"Unable to decode agent_prompts.json. Ensure it is saved with UTF-8 encoding: {e}")
#         print(f"Error: Unable to decode agent_prompts.json. Ensure it is saved with UTF-8 encoding: {e}")
#         return None
#     except Exception as e:
#         logging.error(f"An unexpected error occurred in process_csv_and_generate_icebreaker: {e}", exc_info=True)
#         return None

# Example usage with sample about text
if __name__ == "__main__":
    # Example cleaned text from a LinkedIn company profile
    sample_text = "We are a leading hotel agency focused on exceptional guest experiences."
    # Optional: Specify organization ID if required
    org_id = os.getenv("OPENAI_ORG_ID")
    # Initialize OpenAI client
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    
    openAI_client = OpenAI(api_key=api_key, organization=org_id)
    with open("backend\\scripts\\openai\\agent_prompts.json", 'r', encoding='utf-8') as f:
        agents = json.load(f)
    
    system_message = agents.get("hospitality").get("system_message")
    user_message_role = agents.get("hospitality").get("user_message").get("role")
    user_message_content = agents.get("hospitality").get("user_message").get("content")
    user_message = {
        "role": user_message_role,
        "content": f"""{user_message_content}
        Company Profile About Text:
        {sample_text}
        """
    }
    print(system_message)
    print(user_message_role)
    print(user_message_content)
    print(user_message)
    # print(agents.get("hospitality").get("system_message").get("content"))
    # selected_agent = next((agent[agent_prompt] for agent in agents if agent_prompt in agent), None)
    # if not selected_agent:
    #     raise ValueError(f"Agent '{agent_prompt}' not found in agents.json")
    
    # print("# Role\nAct as an outreach specialist for an AI technology company, specializing in crafting engaging icebreakers for email outreach to professionals across various industries to promote AI agents for customer support, reservations, operations, and business process automation.\n\n# Task\nCreate compelling and personalized icebreakers for email templates to initiate contact with decision-makers across industries in North America. These icebreakers should be based on text extracted from LinkedIn company profiles or individual profiles of business professionals. Your goal is to make the initial email feel personalized, engaging, and relevant, encouraging a positive response to discuss AI solutions for customer support, reservations, operations, or business process automation.\n\n## Specifics\n- Generate icebreakers that are directly related to the content found on the LinkedIn company or individual profile provided, focusing on their work, industry, or role.\n- Keep the icebreakers concise (1-2 sentences), engaging, and relevant to the recipient’s industry and the use of AI for customer support, reservations, operations, or business process automation.\n- Personalize the icebreaker to the recipient using details such as their role, company achievements, or recent activities mentioned on LinkedIn.\n- The icebreaker should seamlessly integrate into the email template following the greeting \"Hi {{name}}, {{icebreaker}}\".\n- Avoid generic statements that could apply to any company or individual; focus on specifics like recent company milestones, technology initiatives, or operational improvements.\n- If the LinkedIn profile text is vague or lacks detail, craft an icebreaker that shows genuine interest in learning more about their work, goals, or challenges in customer support, reservations, operations, or process automation.\n- Subtly tie the icebreaker to potential benefits of AI agents (e.g., enhancing customer support, streamlining reservations, improving operational efficiency, or automating business processes) without being overtly salesy.\n- Target decision-makers such as CEOs, Founders, Co-Founders, Owners, Directors, Managers, or IT leaders in companies across various industries.\n\n## Target Audience\n- **Geography**: North America\n- **Industry**: All industries\n- **Company Size**: 1-10, 11-50, 51-200, 201-500, 501-1000\n- **Function**: Operations, Sales, Marketing, Information Technology, Customer Success, General Management\n- **Seniority Level**: Owner, C-Level, Director, Manager\n- **Titles**: Chief Executive Officer, Director of Operations, Marketing Director, Senior Director of Marketing, Co-Founder, Founder, Owner, General Manager, IT Director, Chief Technology Officer, Chief Information Officer, VP of Operations\n- **Keywords**: AI, chatbot, customer support, reservations, operations, business process automation, efficiency, technology solutions\n\n## Tools\nYou do not have external tools to assist you. Your primary resource will be the LinkedIn company or individual profile text provided by the user, or inferred details based on the target audience’s roles, industries, and functions.\n\n**Usage Context**\nThe icebreaker will be inserted into this email template:\n\n> **Hi {{name}}, {{icebreaker}}**\n**Do not include Hi {{name}} in your response, i just need the icebreaker, raw text. No \"*\" or quotes (\"\") in the end or beginning. **\n\n**Guidelines**\n## Examples\nQ: LinkedIn company profile mentions a company recently expanded its operations to a new city.\nA: Congratulations on your recent expansion to a new city! How are you planning to streamline operations or customer support in this new market?\n\nQ: LinkedIn individual profile of a Director highlights their focus on improving team productivity.\nA: I noticed your impressive work on boosting team productivity! What strategies are you exploring to further automate processes or enhance customer support?\n\nQ: LinkedIn company profile mentions adopting new technology to improve business operations.\nA: Your recent adoption of new tech for operations caught my attention! What challenges are you tackling to streamline processes or improve customer interactions?\n\nQ: LinkedIn profile is vague but indicates the person is a Manager at a mid-sized company.\nA: As a Manager at a dynamic company, I bet you’re always looking for ways to optimize operations or customer support. What’s the latest project you’re excited about?\n\n## Notes\n- Your responses should feel like they’re coming from a real person who has taken the time to understand the recipient’s role, achievements, or company initiatives.\n- Maintain a professional yet approachable tone, avoiding overly formal language to foster a human and friendly interaction.\n- Always aim to make the recipient feel like the email was crafted specifically for them, using LinkedIn profile details to personalize the icebreaker.\n- Avoid technical AI jargon (e.g., “machine learning,” “NLP”) unless the profile explicitly mentions tech adoption; instead, use terms like “customer support,” “reservations,” “operations,” or “process automation” to align with business priorities.\n- The primary goal is to break the ice and encourage dialogue about how AI agents can address their specific needs in customer support, reservations, operations, or business process automation, not to pitch the product directly in the icebreaker.\n- If the profile mentions specific challenges (e.g., operational bottlenecks, customer service issues), tailor the icebreaker to subtly address how AI could help without being pushy.\n- If the company profile text is empty, just generate a general icebreaker that can fit to all kind of people.\n- If you don't understand anything, or any error happen just answer with just a space (i dont want the email to have wierd things) and never mention anything else (e.g. i couldn't provide ..., please rovide text... etc)")
    # # Generate icebreaker
    # icebreaker = generate_icebreaker(sample_text, openAI_client, temperature=0.7)
    # if icebreaker:
    #     print("Generated Icebreaker:")
    #     print(icebreaker)
    # else:
    #     print("Failed to generate icebreaker.")