import os
import json
import logging
from backend.config import Config

def write_progress(current_row, total_rows, job_id, step_id, stop_call=False):
    """
    Write processing progress to a JSON file for a specific step and job.

    Args:
        current_row (int): Current row being processed (1-based index).
        total_rows (int): Total number of rows to process.
        job_id (str): Unique identifier for the job (e.g., UUID).
        step_id (str): Identifier for the processing step (e.g., 'step4', 'step5').

    Returns:
        None
    """
    progress_file = os.path.join(Config.TEMP_PATH, f"progress_{step_id}_{job_id}.json")
    status = 'stopped' if stop_call else ("running" if current_row < total_rows else "completed")           
    
    try:
        with open(progress_file, "w") as f:
            json.dump({
                "job_id": job_id,
                "current_row": current_row,
                "total_rows": total_rows,
                "status": status
            }, f, indent=2)
        logging.info(f"Progress updated for job {job_id} ({step_id}): row {current_row}/{total_rows}, status: {status}")
    except Exception as e:
        logging.error(f"Failed to write progress for job {job_id} ({step_id}): {e}")


def check_stop_signal(step_id):
    """
    Check if a stop signal file exists for the specified step.

    Parameters:
        step_id (str): Identifier for the processing step (e.g., 'step7', 'step8').

    Returns:
        bool: True if stop signal file exists, False otherwise.
    """
    stop_file = os.path.join(Config.TEMP_PATH, f"stop_{step_id}.txt")
    return os.path.exists(stop_file)