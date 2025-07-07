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

api_bp = Blueprint("api", __name__)

def update_job_status(step, job_id, status):
    """
    Updates the status of a job in the jobs_stepX.json file.
    
    Parameters:
        step (int): Step number (5, 6, or 7).
        job_id (str): UUID of the job.
        status (str): New status ('running', 'completed', or 'stopped').
    """
    jobs_file = os.path.join(Config.TEMP_PATH, f"jobs_step{step}.json")
    try:
        os.makedirs(Config.TEMP_PATH, exist_ok=True)
        if os.path.exists(jobs_file):
            with open(jobs_file, "r") as f:
                jobs = json.load(f)
        else:
            jobs = []
        for job in jobs:
            if job["job_id"] == job_id:
                job["status"] = status
                break
        with open(jobs_file, "w") as f:
            json.dump(jobs, f, indent=2)
    except Exception as e:
        print(f"Error updating job status for step {step}, job {job_id}: {e}")

@api_bp.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400
    file_path = os.path.join(Config.DATA_CSV_PATH, file.filename)
    file.save(file_path)
    return jsonify({"message": f"File {file.filename} uploaded successfully", "path": file_path}), 200

@api_bp.route("/steps/<int:step>", methods=["POST"])
def run_step(step):
    try:
        if step == 1:
            # Expect JSON with HTML content and output filename
            data = request.get_json()
            if not data or "html_content" not in data or "output_file" not in data:
                return jsonify({"error": "Missing html_content or output_file"}), 400
            
            # Save HTML content to temp file
            temp_file = os.path.join(Config.TEMP_PATH, "Sales_Navigator.txt")
            with open(temp_file, "w", encoding="utf-8") as f:
                f.write(data["html_content"])
            
            # Run the script
            output_path = Config.DATA_CSV_PATH
            result_df = parse_sales_navigator(temp_file, data["output_file"], output_path)
            
            if result_df is None:
                return jsonify({"error": "Script execution failed", "status": "failed"}), 500
            
            return jsonify({
                "message": f"Step 1 completed. Output saved to {os.path.join(output_path, data['output_file'])}",
                "status": "success",
                "rows_processed": len(result_df)
            }), 200
        
        elif step == 2:
            # Expect JSON with input CSV filename
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv"}), 400
            
            input_csv = data["input_csv"]
            output_csv = f"Filtered_{input_csv}"
            result_df = remove_empty_company_rows(input_csv, output_csv)
            
            if result_df is None:
                return jsonify({"error": "Step 2 execution failed", "status": "failed"}), 500
            
            return jsonify({
                "message": f"Step 2 completed. Filtered CSV saved to {os.path.join(Config.DATA_CSV_PATH, 'filtered_url', output_csv)}",
                "status": "success",
                "rows_processed": len(result_df)
            }), 200
        
        elif step == 3:
            # Expect JSON with input CSV filename
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv"}), 400
            
            input_csv = data["input_csv"]
            output_csv = f"Updated_Name_{input_csv}"
            result_df = process_csv(input_csv, output_csv)
            
            if result_df is None:
                return jsonify({"error": "Step 3 execution failed", "status": "failed"}), 500
            
            return jsonify({
                "message": f"Step 3 completed. Updated CSV saved to {os.path.join(Config.DATA_CSV_PATH, 'updated_name', output_csv)}",
                "status": "success",
                "rows_processed": len(result_df)
            }), 200
        
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
        
        elif step == 5:
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv"}), 400
            input_csv = data["input_csv"]
            max_rows = data.get("max_rows", 2000)
            batch_size = data.get("batch_size", 100)
            delete_no_website = data.get("delete_no_website", True)
            offset = data.get("offset", 0)
            output_csv = f"DomainAbout_{input_csv}"
            input_path = os.path.join(Config.UPDATED_NAME_PATH, input_csv)
            output_path = os.path.join(Config.DOMAIN_ABOUT_PATH, output_csv)
            job_id = str(uuid.uuid4())
            # Remove stop signal if it exists
            stop_file = os.path.join(Config.TEMP_PATH, "stop_step5.txt")
            if os.path.exists(stop_file):
                os.remove(stop_file)
            # Save job metadata
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
                "status": "running"
            })
            with open(jobs_file, "w") as f:
                json.dump(jobs, f, indent=2)
            
            def run_step5():
                try:
                    result_df = process_csv_and_extract_info(
                        input_csv=input_path,
                        output_csv=output_path,
                        max_rows=max_rows,
                        batch_size=batch_size,
                        delete_no_website=delete_no_website,
                        offset=offset,
                        job_id=job_id
                    )
                    if result_df is None:
                        update_job_status(5, job_id, "failed")
                        with open(os.path.join(Config.TEMP_PATH, "step5_error.txt"), "w") as f:
                            f.write(f"Step 5 execution failed, Job Id: {job_id}")
                except Exception as e:
                    update_job_status(5, job_id, "failed")
                    print(f"Error in Step 5 job {job_id}: {e}")
            
            threading.Thread(target=run_step5, daemon=True).start()
            return jsonify({
                "message": f"Step 5 started. Output will be saved to {output_path}",
                "status": "started",
                "job_id": job_id
            }), 200
        
        elif step == 6:
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv"}), 400
            input_csv = data["input_csv"]
            max_rows = data.get("max_rows", 2000)
            batch_size = data.get("batch_size", 50)
            delete_no_email = data.get("delete_no_email", True)
            offset = data.get("offset", 0)
            tor_restart_interval = data.get("tor_restart_interval", 30)
            output_csv = f"Emails_{input_csv}"
            input_path = os.path.join(Config.DOMAIN_ABOUT_PATH, input_csv)
            output_path = os.path.join(Config.EMAILS_PATH, output_csv)
            job_id = str(uuid.uuid4())
            stop_file = os.path.join(Config.TEMP_PATH, "stop_step6.txt")
            if os.path.exists(stop_file):
                os.remove(stop_file)
            # Save job metadata
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
            
            def run_step6():
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
                        with open(os.path.join(Config.TEMP_PATH, "step6_error.txt"), "w") as f:
                            f.write(f"Step 6 execution failed, Job Id: {job_id}")
                except Exception as e:
                    update_job_status(6, job_id, "failed")
                    print(f"Error in Step 6 job {job_id}: {e}")
            
            threading.Thread(target=run_step6, daemon=True).start()
            return jsonify({
                "message": f"Step 6 started. Output will be saved to {output_path}",
                "status": "started",
                "job_id": job_id
            }), 200
        
        elif step == 7:
            data = request.get_json()
            if not data or "input_csv" not in data:
                return jsonify({"error": "Missing input_csv"}), 400
            input_csv = data["input_csv"]
            max_rows = data.get("max_rows", 2000)
            batch_size = data.get("batch_size", 50)
            delete_invalid = data.get("delete_invalid", True)
            offset = data.get("offset", 0)
            tor_restart_interval = data.get("tor_restart_interval", 30)
            output_csv = f"Verified_{input_csv}"
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
            
            def run_step7():
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
                        with open(os.path.join(Config.TEMP_PATH, "step7_error.txt"), "w") as f:
                            f.write(f"Step 7 execution failed, Job Id: {job_id}")
                except Exception as e:
                    update_job_status(7, job_id, "failed")
                    print(f"Error in Step 7 job {job_id}: {e}")
            
            threading.Thread(target=run_step7, daemon=True).start()
            return jsonify({
                "message": f"Step 7 started. Output will be saved to {output_path}",
                "status": "started",
                "job_id": job_id
            }), 200
        
        else:
            return jsonify({"error": "Invalid step"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/stop/<int:step>", methods=["POST"])
def stop_step(step):
    stop_file = os.path.join(Config.TEMP_PATH, f"stop_step{step}.txt")
    jobs_file = os.path.join(Config.TEMP_PATH, f"jobs_step{step}.json")
    try:
        # Write stop signal
        with open(stop_file, "w") as f:
            f.write("stop")
        
        # Update job status
        if os.path.exists(jobs_file):
            with open(jobs_file, "r") as f:
                jobs = json.load(f)
            for job in jobs:
                if job["status"] == "running":
                    job["status"] = "stopped"
                    # Update progress file to reflect stopped status
                    progress_file = os.path.join(Config.TEMP_PATH, f"progress_step{step}_{job['job_id']}.json")
                    if os.path.exists(progress_file):
                        with open(progress_file, "r") as pf:
                            progress = json.load(pf)
                        progress["status"] = "stopped"
                        with open(progress_file, "w") as pf:
                            json.dump(progress, pf)
            with open(jobs_file, "w") as f:
                json.dump(jobs, f)
        
        return jsonify({"message": f"Step {step} stopped"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/progress/<int:step>", methods=["GET"])
def get_progress(step):
    job_id = request.args.get("job_id")
    if step in [5, 6, 7]:
        if job_id:
            progress_file = os.path.join(Config.TEMP_PATH, f"progress_step{step}_{job_id}.json")
        else:
            progress_file = os.path.join(Config.TEMP_PATH, f"progress_step{step}.json")
        try:
            if os.path.exists(progress_file):
                with open(progress_file, "r") as f:
                    progress = json.load(f)
                return jsonify({
                    "step": step,
                    "job_id": progress.get("job_id", ""),
                    "progress": f"Processing row {progress['current_row']}/{progress['total_rows']}" if progress["status"] == "running" else progress["status"].capitalize(),
                    "current_row": progress["current_row"],
                    "total_rows": progress["total_rows"],
                    "status": progress["status"]
                })
            else:
                return jsonify({
                    "step": step,
                    "job_id": job_id or "",
                    "progress": "Not started",
                    "current_row": 0,
                    "total_rows": 0,
                    "status": "not_started"
                })
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"step": step, "job_id": job_id or "", "progress": "Not implemented", "current_row": 0, "total_rows": 0, "status": "not_implemented"})

@api_bp.route("/jobs/<int:step>", methods=["GET"])
def get_jobs(step):
    if step in [5, 6, 7]:
        jobs_file = os.path.join(Config.TEMP_PATH, f"jobs_step{step}.json")
        try:
            if os.path.exists(jobs_file):
                with open(jobs_file, "r") as f:
                    jobs = json.load(f)
                return jsonify({"step": step, "jobs": jobs})
            else:
                return jsonify({"step": step, "jobs": []})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"step": step, "jobs": []})

@api_bp.route("/files/<path:folder>", methods=["GET"])
def list_files(folder):
    try:
        # If folder is "csv", use DATA_CSV_PATH directly; otherwise, use subfolder
        folder_path = Config.DATA_CSV_PATH if folder == "csv" else os.path.join(Config.DATA_CSV_PATH, folder)
        if not os.path.exists(folder_path):
            return jsonify({"error": "Folder not found", "path": folder_path}), 404
        files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
        return jsonify({"files": files}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/logs", methods=["GET"])
def get_logs():
    log_dir = Config.LOG_PATH
    logs = [f for f in os.listdir(log_dir) if f.endswith(".log")]
    return jsonify({"logs": logs})