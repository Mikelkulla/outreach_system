from flask import Blueprint, jsonify, request
import os
import json
import threading
import uuid
from backend.config import Config
from backend.scripts.sales_navigator_scrape.navigators_scrape_companyID import parse_sales_navigator
from backend.scripts.sales_navigator_scrape.remove_empty_companyurl import remove_empty_company_rows
from backend.scripts.openai.correctname_finder import process_csv
from backend.scripts.sales_navigator_scrape.extract_company_about_website import process_csv_and_extract_info
from backend.scripts.sales_navigator_scrape.email_finder import process_csv_and_find_emails
from backend.scripts.sales_navigator_scrape.verify_emails import process_csv_and_verify_emails
from backend.scripts.openai.icebreaker_generator import process_csv_and_generate_icebreaker

api_bp = Blueprint("api", __name__)

def update_job_status(step, job_id, status):
    """
    Updates the status of a job in the jobs_stepX.json file.
    
    Parameters:
        step (int): Step number (5, 6, 7, or 8).
        job_id (str): UUID of the job.
        status (str): New status ('running', 'completed', or 'stopped').
    """
    # Construct the path to the JSON file that stores job information for this step.
    jobs_file = os.path.join(Config.TEMP_PATH, f"jobs_step{step}.json")
    try:
        # Ensure the temporary directory for job files exists.
        os.makedirs(Config.TEMP_PATH, exist_ok=True)

        # Load existing jobs if the file exists, otherwise start with an empty list.
        if os.path.exists(jobs_file):
            with open(jobs_file, "r") as f:
                jobs = json.load(f)
        else:
            jobs = []

        # Find the job by its ID and update its status.
        for job in jobs:
            if job["job_id"] == job_id:
                job["status"] = status
                job_found = True
                break
      
        # If a new job (not yet in the list) is being marked (e.g. as 'failed' immediately),
        # it might not be found. This path is less common as jobs are usually added first.
        # However, to be robust, one might consider adding it if not found and status is 'failed' or 'completed'.
        # For now, it only updates existing jobs.

        # Write the updated list of jobs back to the JSON file.
        with open(jobs_file, "w") as f:
            json.dump(jobs, f, indent=2)
    except Exception as e:
        # Log any errors encountered during the status update.
        print(f"Error updating job status for step {step}, job {job_id}: {e}")

@api_bp.route("/upload", methods=["POST"])
def upload_file():
    """
    Handles file uploads. Expects a file in the 'file' part of a multipart/form-data request.
    Saves the uploaded file to the directory specified by Config.DATA_CSV_PATH.
    """
    # Check if a file was included in the request.
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]

    # Check if a filename was provided (i.e., a file was actually selected).
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    # Construct the full path to save the file.
    file_path = os.path.join(Config.DATA_CSV_PATH, file.filename)
    # Save the file to the designated path.
    file.save(file_path)

    return jsonify({"message": f"File {file.filename} uploaded successfully", "path": file_path}), 200

@api_bp.route("/steps/<int:step>", methods=["POST"])
def run_step(step):
    """
    Main endpoint to trigger various data processing steps.
    The behavior depends on the 'step' number provided in the URL.
    Steps 1, 2, 3 are synchronous.
    Steps 5, 6, 7, 8 are asynchronous, run in background threads, and their status can be tracked.
    """
    try:
        # Step 1: Parse Sales Navigator HTML content to extract company IDs.
        if step == 1:
            # Expect JSON with HTML content and output filename
            data = request.get_json()
            if not data or "html_content" not in data or "output_file" not in data:
                return jsonify({"error": "Missing html_content or output_file"}), 400
            
            # Temporarily save the HTML content from the request to a file.
            temp_file = os.path.join(Config.TEMP_PATH, "Sales_Navigator.txt")
            with open(temp_file, "w", encoding="utf-8") as f:
                f.write(data["html_content"])
            
            # Run the script
            output_path = Config.DATA_CSV_PATH # Base path for output CSVs
            # Call the script to parse the HTML and generate a CSV.
            result_df = parse_sales_navigator(temp_file, data["output_file"], output_path)
            
            if result_df is None: # Script indicates failure if it returns None
                return jsonify({"error": "Script execution failed for Step 1", "status": "failed"}), 500
            
            return jsonify({
                "message": f"Step 1 completed. Output saved to {os.path.join(output_path, data['output_file'])}",
                "status": "success",
                "rows_processed": len(result_df)
            }), 200
        
        # Step 2: Remove rows with empty company URLs from a CSV.
        elif step == 2:
            # Expect JSON with input CSV filename
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv for Step 2"}), 400
            
            input_csv = data["input_csv"]
            # Define output filename and call the processing script.
            output_csv = f"Filtered_{input_csv}"
            result_df = remove_empty_company_rows(input_csv, output_csv)
            
            if result_df is None:
                return jsonify({"error": "Step 2 execution failed", "status": "failed"}), 500
            
            # Output is saved in a 'filtered_url' subdirectory.            
            return jsonify({
                "message": f"Step 2 completed. Filtered CSV saved to {os.path.join(Config.DATA_CSV_PATH, 'filtered_url', output_csv)}",
                "status": "success",
                "rows_processed": len(result_df)
            }), 200
        
        # Step 3: Process CSV to correct/update names (likely using OpenAI).
        elif step == 3:
            # Expect JSON with input CSV filename
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv for Step 3"}), 400
            
            input_csv = data["input_csv"]
            # Define output filename and call the processing script.
            output_csv = f"Updated_Name_{input_csv}"
            result_df = process_csv(input_csv, output_csv) # Script from openai.correctname_finder
            
            if result_df is None:
                return jsonify({"error": "Step 3 execution failed", "status": "failed"}), 500
            
            # Output is saved in an 'updated_name' subdirectory.
            return jsonify({
                "message": f"Step 3 completed. Updated CSV saved to {os.path.join(Config.DATA_CSV_PATH, 'updated_name', output_csv)}",
                "status": "success",
                "rows_processed": len(result_df)
            }), 200

        # Step 4: (Currently Commented Out) Intended for updating company URLs.        
        # elif step == 4:
        #     # Expect JSON with input CSV filename
        #     data = request.get_json()
        #     if not data or "input_csv" not in data:
        #         return jsonify({"error": "Missing input_csv"}), 400
        #     
        #     input_csv = data["input_csv"]
        #     max_rows = data.get("max_rows", 2000)
        #     batch_size = data.get("batch_size", 10)
        #     output_csv = f"Updated_URL_{input_csv}"
        #     input_path = os.path.join(Config.UPDATED_NAME_PATH, input_csv)
        #     output_path = os.path.join(Config.UPDATED_URL_PATH, output_csv)
        #     # Remove stop signal if it exists
        #     stop_file = os.path.join(Config.TEMP_PATH, "stop_step4.txt")
        #     if os.path.exists(stop_file):
        #         os.remove(stop_file)
        #     result_df = process_csv_and_update_urls(input_path, output_path, max_rows=max_rows, batch_size=batch_size)
        #     if result_df is None:
        #         return jsonify({"error": "Step 4 execution failed", "status": "failed"}), 500
        #     
        #     return jsonify({
        #         "message": f"Step 4 completed. Updated CSV saved to {output_path}",
        #         "status": "success",
        #         "rows_processed": len(result_df)
        #     }), 200
        
        # Step 5: Extract company domain and about information (Asynchronous).
        elif step == 5:
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv for Step 5"}), 400

            # Extract parameters from the request.
            input_csv = data["input_csv"]
            max_rows = data.get("max_rows", 2000)                           # Default max rows to process
            batch_size = data.get("batch_size", 100)                        # Default batch size for processing
            delete_no_website = data.get("delete_no_website", False)         # Option to delete rows without websites
            offset = data.get("offset", 0)                                  # Row offset to start processing from

            output_csv = f"DomainAbout_{input_csv}"
            # Construct full paths for input and output files.
            input_path = os.path.join(Config.UPDATED_NAME_PATH, input_csv)      # Input from 'updated_name' folder
            output_path = os.path.join(Config.DOMAIN_ABOUT_PATH, output_csv)    # Output to 'domain_about' folder

            job_id = str(uuid.uuid4()) # Generate a unique ID for this job.

            # Remove any pre-existing stop signal file for this step.
            stop_file = os.path.join(Config.TEMP_PATH, "stop_step5.txt")
            if os.path.exists(stop_file):
                os.remove(stop_file)

            # Record job metadata in jobs_step5.json.
            jobs_file = os.path.join(Config.TEMP_PATH, "jobs_step5.json")
            os.makedirs(Config.TEMP_PATH, exist_ok=True)
            jobs = []
            if os.path.exists(jobs_file):
                with open(jobs_file, "r") as f:
                    jobs = json.load(f)
            jobs.append({
                "job_id": job_id,
                "input_csv": input_csv,
                "output_csv": output_csv,
                "status": "running" # Initial status
            })
            with open(jobs_file, "w") as f:
                json.dump(jobs, f, indent=2)

            # Define the function to be executed in a separate thread.            
            def run_step5_async():
                try:
                    result_df = process_csv_and_extract_info(
                        input_csv=input_path,
                        output_csv=output_path,
                        max_rows=max_rows,
                        batch_size=batch_size,
                        delete_no_website=delete_no_website,
                        offset=offset,
                        job_id=job_id # Pass job_id for progress tracking/logging by the script
                    )
                    # If script indicates failure, update job status and log error.
                    if result_df is None:
                        update_job_status(5, job_id, "failed")
                        # Optionally, write more detailed error to a specific file.
                        with open(os.path.join(Config.TEMP_PATH, f"step5_error_{job_id}.txt"), "w") as f:
                            f.write(f"Step 5 execution failed, Job Id: {job_id}")
                except Exception as e:
                    update_job_status(5, job_id, "failed")
                    print(f"Exception in Step 5 background job {job_id}: {e}")

            # Start the background thread.            
            threading.Thread(target=run_step5_async, daemon=True).start()
            # Return an immediate response indicating the job has started.
            return jsonify({
                "message": f"Step 5 (Extract Domain/About) started. Output will be saved to {output_path}",
                "status": "started",
                "job_id": job_id
            }), 200
        
        # Step 6: Find emails for companies (Asynchronous).
        elif step == 6:
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv for Step 6"}), 400
            
            # Extract parameters.
            input_csv = data["input_csv"]
            max_rows = data.get("max_rows", 2000)
            batch_size = data.get("batch_size", 50)
            delete_no_email = data.get("delete_no_email", False)
            offset = data.get("offset", 0)
            tor_restart_interval = data.get("tor_restart_interval", 30) # Parameter for Tor network usage

            output_csv = f"Emails_{input_csv}"
            # Input from 'domain_about' folder, output to 'emails' folder.
            input_path = os.path.join(Config.DOMAIN_ABOUT_PATH, input_csv)
            output_path = os.path.join(Config.EMAILS_PATH, output_csv)
            job_id = str(uuid.uuid4())

            stop_file = os.path.join(Config.TEMP_PATH, "stop_step6.txt")
            if os.path.exists(stop_file):
                os.remove(stop_file)

            # Record job metadata in jobs_step6.json.
            jobs_file = os.path.join(Config.TEMP_PATH, "jobs_step6.json")
            os.makedirs(Config.TEMP_PATH, exist_ok=True)
            jobs = []
            if os.path.exists(jobs_file):
                with open(jobs_file, "r") as f:
                    jobs = json.load(f)
            jobs.append({
                "job_id": job_id,
                "input_csv": input_csv,
                "output_csv": output_csv,
                "status": "running"
            })
            with open(jobs_file, "w") as f:
                json.dump(jobs, f, indent=2)
            
            # Define the threaded function.
            def run_step6_async():
                try:
                    result_df = process_csv_and_find_emails(
                        input_csv=input_path,
                        output_csv=output_path,
                        max_rows=max_rows,
                        batch_size=batch_size,
                        tor_restart_interval=tor_restart_interval,
                        offset=offset,
                        delete_no_email=delete_no_email,
                        job_id=job_id
                    )
                    if result_df is None:
                        update_job_status(6, job_id, "failed")
                        with open(os.path.join(Config.TEMP_PATH, f"step6_error_{job_id}.txt"), "w") as f:
                            f.write(f"Step 6 execution failed, Job Id: {job_id}")
                except Exception as e:
                    update_job_status(6, job_id, "failed")
                    print(f"Exception in Step 6 background job {job_id}: {e}")
            
            threading.Thread(target=run_step6_async, daemon=True).start()
            return jsonify({
                "message": f"Step 6 (Find Emails) started. Output will be saved to {output_path}",
                "status": "started",
                "job_id": job_id
            }), 200
        
        # Step 7: Verify found emails (Asynchronous).
        elif step == 7:
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv for Step 7"}), 400
            
            # Extract parameters.
            input_csv = data["input_csv"]
            max_rows = data.get("max_rows", 2000)
            batch_size = data.get("batch_size", 50)
            delete_invalid = data.get("delete_invalid", False) # Option to delete invalid emails
            offset = data.get("offset", 0)
            tor_restart_interval = data.get("tor_restart_interval", 30)

            output_csv = f"Verified_{input_csv}"
            # Input from 'emails' folder, output to 'verified_emails' folder.
            input_path = os.path.join(Config.EMAILS_PATH, input_csv)
            output_path = os.path.join(Config.VERIFIED_EMAILS_PATH, output_csv)
            job_id = str(uuid.uuid4())

            stop_file = os.path.join(Config.TEMP_PATH, "stop_step7.txt")
            if os.path.exists(stop_file):
                os.remove(stop_file)

            jobs_file = os.path.join(Config.TEMP_PATH, "jobs_step7.json")
            os.makedirs(Config.TEMP_PATH, exist_ok=True)
            jobs = []
            if os.path.exists(jobs_file):
                with open(jobs_file, "r") as f:
                    jobs = json.load(f)
            jobs.append({
                "job_id": job_id,
                "input_csv": input_csv,
                "output_csv": output_csv,
                "status": "running"
            })
            with open(jobs_file, "w") as f:
                json.dump(jobs, f, indent=2)
            
            # Define the threaded function.
            def run_step7_async():
                try:
                    result_df = process_csv_and_verify_emails(
                        input_csv=input_path,
                        output_csv=output_path,
                        max_rows=max_rows,
                        batch_size=batch_size,
                        tor_restart_interval=tor_restart_interval,
                        offset=offset,
                        delete_invalid=delete_invalid,
                        job_id=job_id,
                        step_id='step7'
                    )
                    if result_df is None:
                        update_job_status(7, job_id, "failed")
                        with open(os.path.join(Config.TEMP_PATH, f"step7_error_{job_id}.txt"), "w") as f:
                            f.write(f"Step 7 execution failed, Job Id: {job_id}")
                except Exception as e:
                    update_job_status(7, job_id, "failed")
                    print(f"Error in Step 7 job {job_id}: {e}")
            
            threading.Thread(target=run_step7_async, daemon=True).start()
            return jsonify({
                "message": f"Step 7 (Verify Emails) started. Output will be saved to {output_path}",
                "status": "started",
                "job_id": job_id
            }), 200

        # Step 8: Generate personalized icebreakers (Asynchronous).
        elif step == 8:
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv for Step 8"}), 400

            # Extract parameters.
            input_csv = data["input_csv"]
            max_rows = data.get("max_rows", 2000)
            batch_size = data.get("batch_size", 50)
            agent_prompt = data.get("agent_prompt", "default_agent")
            delete_no_icebreaker = data.get("delete_no_icebreaker", False)
            offset = data.get("offset", 0)

            output_csv = f"Icebreaker_{input_csv}"
            # Input from 'verified_emails' folder, output to 'icebreakers' folder.
            input_path = os.path.join(Config.VERIFIED_EMAILS_PATH, input_csv)
            output_path = os.path.join(Config.ICEBREAKERS_PATH, output_csv)
            job_id = str(uuid.uuid4())

            stop_file = os.path.join(Config.TEMP_PATH, "stop_step8.txt")
            if os.path.exists(stop_file):
                os.remove(stop_file)

            # Record job metadata in jobs_step8.json.
            jobs_file = os.path.join(Config.TEMP_PATH, "jobs_step8.json")
            os.makedirs(Config.TEMP_PATH, exist_ok=True)
            jobs = []
            if os.path.exists(jobs_file):
                with open(jobs_file, "r") as f:
                    jobs = json.load(f)
            jobs.append({
                "job_id": job_id,
                "input_csv": input_csv,
                "output_csv": output_csv,
                "status": "running"
            })
            with open(jobs_file, "w") as f:
                json.dump(jobs, f, indent=2)
            
            # Define the threaded function.
            def run_step8_async():
                try:
                    result_df = process_csv_and_generate_icebreaker(
                        input_csv=input_path,
                        output_csv=output_path,
                        max_rows=max_rows,
                        batch_size=batch_size,
                        agent_prompt=agent_prompt,
                        delete_no_icebreaker=delete_no_icebreaker,
                        offset=offset,
                        job_id=job_id,
                        step_id='step8'
                    )
                    if result_df is None:
                        update_job_status(8, job_id, "failed")
                        with open(os.path.join(Config.TEMP_PATH, f"step8_error_{job_id}.txt"), "w") as f:
                            f.write(f"Step 8 execution failed, Job Id: {job_id}")

                except Exception as e:
                    update_job_status(8, job_id, "failed")
                    print(f"Error in Step 8 job {job_id}: {e}")
            
            threading.Thread(target=run_step8_async, daemon=True).start()
            return jsonify({
                "message": f"Step 8 (Generate Icebreakers) started. Output will be saved to {output_path}",
                "status": "started",
                "job_id": job_id
            }), 200
        
        else:
            # Handle invalid step numbers.
            return jsonify({"error": "Invalid step number provided"}), 400
    except Exception as e:
        # Catch-all for any other unexpected errors during step processing.
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@api_bp.route("/stop/<int:step>", methods=["POST"])
def stop_step(step):
    """
    Stops a running asynchronous job for steps 5, 6, 7, or 8.
    It creates a 'stop_stepX.txt' signal file that the background script should check.
    It also updates the job's status in the jobs_stepX.json and progress files.
    """
    # Define the stop signal file and the jobs metadata file.    
    stop_file = os.path.join(Config.TEMP_PATH, f"stop_step{step}.txt")
    jobs_file = os.path.join(Config.TEMP_PATH, f"jobs_step{step}.json")

    # Check if the step number is valid for stoppable jobs.
    if step not in [5, 6, 7, 8]:
        return jsonify({"error": f"Step {step} cannot be stopped or is not a valid stoppable step."}), 400
        
    try:
        # Create the stop signal file. The background script is expected to detect this.
        with open(stop_file, "w") as f:
            f.write("stop")
        
        # Update the status of the 'running' job to 'stopped' in the jobs JSON file.
        job_stopped_id = None
        if os.path.exists(jobs_file):
            with open(jobs_file, "r") as f:
                jobs = json.load(f)

            for job in jobs:
                if job["status"] == "running": # Find the currently running job for this step
                    job["status"] = "stopped"
                    job_stopped_id = job["job_id"]
                    # Update progress file to reflect stopped status
                    progress_file = os.path.join(Config.TEMP_PATH, f"progress_step{step}_{job_stopped_id}.json")
                    if os.path.exists(progress_file):
                        try:
                            with open(progress_file, "r") as pf:
                                progress = json.load(pf)
                            progress["status"] = "stopped" # Update status in progress data
                            with open(progress_file, "w") as pf:
                                json.dump(progress, pf, indent=2)
                        except Exception as e:
                            print(f"Warning: Could not update progress file {progress_file} for job {job_stopped_id}: {e}")
                    else:
                        print(f"Warning: Progress file {progress_file} not found for job {job_stopped_id} during stop operation.")
            with open(jobs_file, "w") as f:
                json.dump(jobs, f, indent=2)

        if job_stopped_id:
            return jsonify({"message": f"Stop signal sent for Step {step}, Job ID {job_stopped_id}. Status updated to 'stopped'."}), 200
        else:
            return jsonify({"message": f"Stop signal sent for Step {step}. No actively running job found to mark as 'stopped'."}), 200
            
    except Exception as e:
        return jsonify({"error": f"Error stopping step {step}: {str(e)}"}), 500

@api_bp.route("/progress/<int:step>", methods=["GET"])
def get_progress(step):
    """
    Retrieves the progress of an asynchronous job (steps 5, 6, 7).
    Requires a 'job_id' query parameter for specific job progress.
    Reads progress from a 'progress_stepX_jobY.json' file.
    """
    job_id = request.args.get("job_id") # Get job_id from query parameters.

    if step not in [5, 6, 7, 8]:
        return jsonify({
            "step": step, 
            "job_id": job_id or "N/A", 
            "progress": "Progress tracking not implemented for this step.", 
            "current_row": 0, 
            "total_rows": 0, 
            "status": "not_implemented"
        }), 400

    if not job_id:
        # If no specific job_id is provided, try to find the latest or currently running job.
        # This part might need more sophisticated logic if multiple jobs can exist per step.
        # For now, it implies a primary progress file or requires job_id for specific tracking.
        # A simple fallback if job_id is missing for steps that usually require it.
        jobs_file = os.path.join(Config.TEMP_PATH, f"jobs_step{step}.json")
        if os.path.exists(jobs_file):
            with open(jobs_file, "r") as f:
                jobs_data = json.load(f)
            # Attempt to find the last 'running' or most recent job if no ID is given
            running_jobs = [j for j in jobs_data if j.get("status") == "running"]
            if running_jobs:
                job_id = running_jobs[-1].get("job_id") # Get the last running job's ID
            elif jobs_data:
                 job_id = jobs_data[-1].get("job_id") # Get the last known job's ID

        if not job_id: # If still no job_id, return appropriate message
            return jsonify({
                "step": step,
                "job_id": "Unknown",
                "progress": "No active or specific job ID provided for progress tracking.",
                "current_row": 0,
                "total_rows": 0,
                "status": "no_job_id_provided"
            }), 400

    # Construct path to the specific job's progress file.
    progress_file = os.path.join(Config.TEMP_PATH, f"progress_step{step}_{job_id}.json")  

    try:
        if os.path.exists(progress_file):
            with open(progress_file, "r") as f:
                progress = json.load(f)

            # Format progress message based on status.
            progress_message = f"Processing row {progress.get('current_row', 0)}/{progress.get('total_rows', 0)}" \
                               if progress.get("status") == "running" else progress.get("status", "N/A").capitalize()    
            return jsonify({
                "step": step,
                "job_id": progress.get("job_id", job_id), # Use job_id from file if available
                "progress": progress_message,
                "current_row": progress.get("current_row", 0),
                "total_rows": progress.get("total_rows", 0),
                "status": progress.get("status", "unknown")
            })
        else:
            # If progress file doesn't exist, the job might not have started or reported progress.
            return jsonify({
                "step": step,
                "job_id": job_id,
                "progress": "Not started or progress file not found.",
                "current_row": 0,
                "total_rows": 0,
                "status": "not_started"
            })
    except Exception as e:
        return jsonify({"error": f"Error retrieving progress for step {step}, job {job_id}: {str(e)}"}), 500

@api_bp.route("/jobs/<int:step>", methods=["GET"])
def get_jobs(step):
    """
    Lists all recorded jobs and their statuses for a specific asynchronous step (5, 6, or 7).
    Reads data from the corresponding 'jobs_stepX.json' file.
    """
    if step not in [5, 6, 7, 8]:
        return jsonify({"step": step, "jobs": [], "message": "Job tracking only available for steps 5, 6, 7, 8."}), 400

    jobs_file = os.path.join(Config.TEMP_PATH, f"jobs_step{step}.json")
    try:
        if os.path.exists(jobs_file):
            with open(jobs_file, "r") as f:
                jobs_data = json.load(f)
            return jsonify({"step": step, "jobs": jobs_data})
        else:
            # If no jobs file exists, it means no jobs have been run for this step yet.
            return jsonify({"step": step, "jobs": []})
    except Exception as e:
        return jsonify({"error": f"Error retrieving jobs for step {step}: {str(e)}"}), 500

@api_bp.route("/files/<path:folder>", methods=["GET"])
def list_files(folder):
    """
    Lists all CSV files in a specified folder within the data directory.
    The 'folder' path parameter can be a subfolder name or 'csv' for the root data CSV path.
    """
    try:
        # Determine the target folder path based on the 'folder' argument.
        if folder == "csv": # Special case: "csv" refers to the root data CSV directory.
            folder_path = Config.DATA_CSV_PATH
        else: # Otherwise, it's a subfolder within DATA_CSV_PATH.
            folder_path = os.path.join(Config.DATA_CSV_PATH, folder)

        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            return jsonify({"error":  "Folder not found or is not a directory", "path_checked": folder_path}), 404
        
        # List files in the directory, filtering for those ending with '.csv'.
        csv_files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f)) and f.endswith(".csv")]
        return jsonify({"files": csv_files}), 200
    except Exception as e:
        return jsonify({"error": f"Error listing files in folder '{folder}': {str(e)}"}), 500

@api_bp.route("/logs", methods=["GET"])
def get_logs():
    """
    Lists all log files (ending with '.log') from the configured log directory.
    """
    log_dir = Config.LOG_PATH

    try:
        if not os.path.exists(log_dir) or not os.path.isdir(log_dir):
            return jsonify({"error": "Log directory not found or is not a directory", "log_path_checked": log_dir}), 404

        # List files in the log directory, filtering for those ending with '.log'.
        log_files = [f for f in os.listdir(log_dir) if os.path.isfile(os.path.join(log_dir, f)) and f.endswith(".log")]
        return jsonify({"logs": log_files, "log_directory": log_dir})
    except Exception as e:
        return jsonify({"error": f"Error listing log files: {str(e)}"}), 500