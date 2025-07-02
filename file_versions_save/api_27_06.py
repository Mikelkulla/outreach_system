from flask import Blueprint, jsonify, request
import os
import json
import threading
from backend.config import Config
from backend.scripts.sales_navigator_scrape.navigators_scrape_companyID import parse_sales_navigator
from backend.scripts.sales_navigator_scrape.remove_empty_companyurl import remove_empty_company_rows
from backend.scripts.openai.correctname_finder import process_csv
# from backend.scripts.sales_navigator_scrape.update_company_urls_with_school_fix import process_csv_and_update_urls
from backend.scripts.sales_navigator_scrape.extract_company_about_website import process_csv_and_extract_info
from backend.scripts.sales_navigator_scrape.email_finder import process_csv_and_find_emails

api_bp = Blueprint("api", __name__)

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
            # Remove stop signal if it exists
            stop_file = os.path.join(Config.TEMP_PATH, "stop_step5.txt")
            if os.path.exists(stop_file):
                os.remove(stop_file)
            result_df = process_csv_and_extract_info(
                input_csv=input_path,
                output_csv=output_path,
                max_rows=max_rows,
                batch_size=batch_size,
                delete_no_website=delete_no_website,
                offset=offset
            )
            if result_df is None:
                return jsonify({"error": "Step 5 execution failed", "status": "failed"}), 500
            return jsonify({
                "message": f"Step 5 completed. Updated CSV saved to {output_path}",
                "status": "success",
                "rows_processed": len(result_df)
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
            stop_file = os.path.join(Config.TEMP_PATH, "stop_step6.txt")
            if os.path.exists(stop_file):
                os.remove(stop_file)
            
            def run_email_finder():
                result_df = process_csv_and_find_emails(
                    input_csv=input_path,
                    output_csv=output_path,
                    max_rows=max_rows,
                    batch_size=batch_size,
                    tor_restart_interval=tor_restart_interval,
                    offset=offset,
                    delete_no_email=delete_no_email
                )
                if result_df is None:
                    with open(os.path.join(Config.TEMP_PATH, "step6_error.txt"), "w") as f:
                        f.write("Step 6 execution failed")
                    result_df = []
                return result_df
            df = run_email_finder()
            # threading.Thread(target=run_email_finder, daemon=True).start()
            return jsonify({
                "message": f"Step 6 started. Output will be saved to {output_path}",
                "status": "started",
                "rows_processed": len(df)
            }), 200
        
        else:
            return jsonify({"error": "Invalid step"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/stop/<int:step>", methods=["POST"])
def stop_step(step):
    try:
        if step == 5:
            stop_file = os.path.join(Config.TEMP_PATH, "stop_step5.txt")
            with open(stop_file, "w") as f:
                f.write("Stop requested")
            return jsonify({"message": "Stop signal sent for Step 5. Process will stop after the current row."}), 200
        elif step == 6:
            stop_file = os.path.join(Config.TEMP_PATH, "stop_step6.txt")
            with open(stop_file, "w") as f:
                f.write("Stop requested")
            return jsonify({"message": "Stop signal sent for Step 6. Process will stop after the current row."}), 200
        else:
            return jsonify({"error": "Invalid step for stop request"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route("/progress/<int:step>", methods=["GET"])
def get_progress(step):
    if step in [5, 6]:
        progress_file = os.path.join(Config.TEMP_PATH, f"progress_step{step}.json")
        try:
            if os.path.exists(progress_file):
                with open(progress_file, "r") as f:
                    progress = json.load(f)
                return jsonify({
                    "step": step,
                    "progress": f"Processing row {progress['current_row']}/{progress['total_rows']}",
                    "current_row": progress['current_row'],
                    "total_rows": progress['total_rows']
                })
            else:
                return jsonify({
                    "step": step,
                    "progress": "Not started",
                    "current_row": 0,
                    "total_rows": 0
                })
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"step": step, "progress": "Not implemented", "current_row": 0, "total_rows": 0})

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