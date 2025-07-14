document.addEventListener("DOMContentLoaded", () => {
    // Show Step 1 content by default
    document.getElementById("step1-content").classList.remove("hidden");
    checkStepAvailability();
    checkRunningJobs(); // Check for running jobs on page load

    // Step 1 button
    document.getElementById("step1").addEventListener("click", () => {
        document.getElementById("step1-content").classList.remove("hidden");
        document.getElementById("step2-content").classList.add("hidden");
        document.getElementById("step3-content").classList.add("hidden");
        document.getElementById("step4-content").classList.add("hidden");
        document.getElementById("step5-content").classList.add("hidden");
        document.getElementById("step6-content").classList.add("hidden");
        document.getElementById("step7-content").classList.add("hidden");
        document.getElementById("step8-content").classList.add("hidden");
    });

    // Step 2 button
    document.getElementById("step2").addEventListener("click", () => {
        document.getElementById("step1-content").classList.add("hidden");
        document.getElementById("step2-content").classList.remove("hidden");
        document.getElementById("step3-content").classList.add("hidden");
        document.getElementById("step4-content").classList.add("hidden");
        document.getElementById("step5-content").classList.add("hidden");
        document.getElementById("step6-content").classList.add("hidden");
        document.getElementById("step7-content").classList.add("hidden");
        document.getElementById("step8-content").classList.add("hidden");
        populateCsvDropdown("input_csv2", "csv");
    });

    // Step 3 button
    document.getElementById("step3").addEventListener("click", () => {
        document.getElementById("step1-content").classList.add("hidden");
        document.getElementById("step2-content").classList.add("hidden");
        document.getElementById("step3-content").classList.remove("hidden");
        document.getElementById("step4-content").classList.add("hidden");
        document.getElementById("step5-content").classList.add("hidden");
        document.getElementById("step6-content").classList.add("hidden");
        document.getElementById("step7-content").classList.add("hidden");
        document.getElementById("step8-content").classList.add("hidden");
        populateCsvDropdown("input_csv3", "filtered_url");
    });

    // Step 4 button
    document.getElementById("step4").addEventListener("click", () => {
        document.getElementById("step1-content").classList.add("hidden");
        document.getElementById("step2-content").classList.add("hidden");
        document.getElementById("step3-content").classList.add("hidden");
        document.getElementById("step4-content").classList.remove("hidden");
        document.getElementById("step5-content").classList.add("hidden");
        document.getElementById("step6-content").classList.add("hidden");
        // populateCsvDropdown("input_csv4", "updated_name");
        document.getElementById("step7-content").classList.add("hidden");
        document.getElementById("step8-content").classList.add("hidden");
    });

    // Step 5 button
    document.getElementById("step5").addEventListener("click", () => {
        document.getElementById("step1-content").classList.add("hidden");
        document.getElementById("step2-content").classList.add("hidden");
        document.getElementById("step3-content").classList.add("hidden");
        document.getElementById("step4-content").classList.add("hidden");
        document.getElementById("step5-content").classList.remove("hidden");
        document.getElementById("step6-content").classList.add("hidden");
        document.getElementById("step7-content").classList.add("hidden");
        document.getElementById("step8-content").classList.add("hidden");
        populateCsvDropdown("input_csv5", "updated_name");
        populateJobDropdown("job_select_step5", 5);
        checkRunningJobs(); // Check running jobs when navigating to Step 5
    });

    // Step 6 button
    document.getElementById("step6").addEventListener("click", () => {
        document.getElementById("step1-content").classList.add("hidden");
        document.getElementById("step2-content").classList.add("hidden");
        document.getElementById("step3-content").classList.add("hidden");
        document.getElementById("step4-content").classList.add("hidden");
        document.getElementById("step5-content").classList.add("hidden");
        document.getElementById("step6-content").classList.remove("hidden");
        document.getElementById("step7-content").classList.add("hidden");
        document.getElementById("step8-content").classList.add("hidden");
        populateCsvDropdown("input_csv6", "domain_about");
        populateJobDropdown("job_select_step6", 6);
        checkRunningJobs(); // Check running jobs when navigating to Step 6
    });

    // Step 7 button
    document.getElementById("step7").addEventListener("click", () => {
        document.getElementById("step1-content").classList.add("hidden");
        document.getElementById("step2-content").classList.add("hidden");
        document.getElementById("step3-content").classList.add("hidden");
        document.getElementById("step4-content").classList.add("hidden");
        document.getElementById("step5-content").classList.add("hidden");
        document.getElementById("step6-content").classList.add("hidden");
        document.getElementById("step7-content").classList.remove("hidden");
        document.getElementById("step8-content").classList.add("hidden");
        populateCsvDropdown("input_csv7", "emails");
        populateJobDropdown("job_select_step7", 7);
        checkRunningJobs();
    });

    // Step 8 button
    document.getElementById("step8").addEventListener("click", () => {
        document.getElementById("step1-content").classList.add("hidden");
        document.getElementById("step2-content").classList.add("hidden");
        document.getElementById("step3-content").classList.add("hidden");
        document.getElementById("step4-content").classList.add("hidden");
        document.getElementById("step5-content").classList.add("hidden");
        document.getElementById("step6-content").classList.add("hidden");
        document.getElementById("step7-content").classList.add("hidden");
        document.getElementById("step8-content").classList.remove("hidden");
        populateCsvDropdown("input_csv8", "verified");
        populateJobDropdown("job_select_step8", 8);
        checkRunningJobs();
    });

    // Run Step 1
    document.getElementById("run_step1").addEventListener("click", async () => {
        const htmlInput = document.getElementById("html_input").value;
        const outputFile = document.getElementById("output_file").value.trim();
        const statusDiv = document.getElementById("status1");

        if (!htmlInput) {
            statusDiv.textContent = "Error: Please paste HTML content.";
            return;
        }
        if (!outputFile) {
            statusDiv.textContent = "Error: Please enter an output CSV filename.";
            return;
        }

        statusDiv.textContent = "Processing...";

        try {
            const response = await fetch("/api/steps/1", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ html_content: htmlInput, output_file: outputFile })
            });
            const result = await response.json();

            if (response.ok) {
                statusDiv.textContent = result.message + ` (${result.rows_processed} rows processed)`;
                checkStepAvailability();
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
        }
    });

    // Run Step 2
    document.getElementById("run_step2").addEventListener("click", async () => {
        const inputCsv = document.getElementById("input_csv2").value;
        const statusDiv = document.getElementById("status2");

        if (!inputCsv) {
            statusDiv.textContent = "Error: Please select an input CSV.";
            return;
        }

        statusDiv.textContent = "Processing...";

        try {
            const response = await fetch("/api/steps/2", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ input_csv: inputCsv })
            });
            const result = await response.json();

            if (response.ok) {
                statusDiv.textContent = result.message + ` (${result.rows_processed} rows processed)`;
                checkStepAvailability();
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
        }
    });

    // Run Step 3
    document.getElementById("run_step3").addEventListener("click", async () => {
        const inputCsv = document.getElementById("input_csv3").value;
        const statusDiv = document.getElementById("status3");

        if (!inputCsv) {
            statusDiv.textContent = "Error: Please select an input CSV.";
            return;
        }

        statusDiv.textContent = "Processing...";

        try {
            const response = await fetch("/api/steps/3", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ input_csv: inputCsv })
            });
            const result = await response.json();

            if (response.ok) {
                statusDiv.textContent = result.message + ` (${result.rows_processed} rows processed)`;
                checkStepAvailability();
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
        }
    });

    // // Run Step 4
    // let progressInterval4 = null;

    // document.getElementById("run_step4").addEventListener("click", async () => {
    //     const inputCsv = document.getElementById("input_csv4").value;
    //     const maxRows = parseInt(document.getElementById("max_rows_step4").value);
    //     const batchSize = parseInt(document.getElementById("batch_size_step4").value);
    //     const statusDiv = document.getElementById("status4");
    //     const runButton = document.getElementById("run_step4");
    //     const stopButton = document.getElementById("stop_step4");
    //     if (!inputCsv) {
    //         statusDiv.textContent = "Error: Please select an input CSV.";
    //         return;
    //     }
    //     if (isNaN(maxRows) || maxRows < 1) {
    //         statusDiv.textContent = "Error: Maximum rows must be a positive number.";
    //         return;
    //     }
    //     if (isNaN(batchSize) || batchSize < 1) {
    //         statusDiv.textContent = "Error: Batch size must be a positive number.";
    //         return;
    //     }
    //     statusDiv.textContent = "Processing...";
    //     runButton.disabled = true;
    //     runButton.classList.add("bg-gray-600", "cursor-not-allowed");
    //     runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
    //     stopButton.disabled = false;
    //     stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
    //     stopButton.classList.add("bg-red-600", "hover:bg-red-700");
    //     
    //     // Start polling for progress
    //     progressInterval4 = setInterval(async () => {
    //         try {
    //             const response = await fetch("/api/progress/4");
    //             const result = await response.json();
    //             if (result.progress && result.progress !== "Not started") {
    //                 statusDiv.textContent = result.progress;
    //             }
    //             // Stop polling if process is done
    //             if (result.current_row >= result.total_rows && result.current_row > 0) {
    //                 clearInterval(progressInterval4);
    //                 progressInterval4 = null;
    //             }
    //         } catch (error) {
    //             console.error("Progress polling error:", error);
    //         }
    //     }, 10000); // Poll every 10 seconds
    // 
    //     try {
    //         const response = await fetch("/api/steps/4", {
    //             method: "POST",
    //             headers: { "Content-Type": "application/json" },
    //             body: JSON.stringify({ input_csv: inputCsv, max_rows: maxRows, batch_size: batchSize })
    //         });
    //         const result = await response.json();
    // 
    //         if (response.ok) {
    //             statusDiv.textContent = result.message + ` (${result.rows_processed} rows processed)`;
    //         } else {
    //             statusDiv.textContent = `Error: ${result.error}`;
    //         }
    //     } catch (error) {
    //         statusDiv.textContent = `Error: ${error.message}`;
    //     } finally {
    //         runButton.disabled = false;
    //         runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
    //         runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
    //         stopButton.disabled = true;
    //         stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
    //         stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
    //         if (progressInterval4) {
    //             clearInterval(progressInterval4);
    //             progressInterval4 = null;
    //         }
    //     }
    // });
    // 
    // document.getElementById("stop_step4").addEventListener("click", async () => {
    //     const statusDiv = document.getElementById("status4");
    //     statusDiv.textContent = "Sending stop signal...";
    //     try {
    //         const response = await fetch("/api/stop/4", {
    //             method: "POST",
    //             headers: { "Content-Type": "application/json" }
    //         });
    //         const result = await response.json();
    //         if (response.ok) {
    //             statusDiv.textContent = result.message;
    //             if (progressInterval4) {
    //                 clearInterval(progressInterval4);
    //                 progressInterval4 = null;
    //             }
    //         } else {
    //             statusDiv.textContent = `Error: ${result.error}`;
    //         }
    //     } catch (error) {
    //         statusDiv.textContent = `Error: ${error.message}`;
    //     }
    // });

    let progressInterval5 = null;
    document.getElementById("run_step5").addEventListener("click", async () => {
        const inputCsv = document.getElementById("input_csv5").value;
        const maxRows = parseInt(document.getElementById("max_rows_step5").value);
        const batchSize = parseInt(document.getElementById("batch_size_step5").value);
        const deleteNoWebsite = document.getElementById("delete_no_website").checked;
        const offset = parseInt(document.getElementById("offset").value);
        // const torRestartInterval = parseInt(document.getElementById("tor_restart_interval").value);
        const statusDiv = document.getElementById("status5");
        const runButton = document.getElementById("run_step5");
        const stopButton = document.getElementById("stop_step5");
        if (!inputCsv) {
            statusDiv.textContent = "Error: Please select an input CSV.";
            return;
        }
        if (isNaN(maxRows) || maxRows < 1) {
            statusDiv.textContent = "Error: Maximum rows must be a positive number.";
            return;
        }
        if (isNaN(batchSize) || batchSize < 1) {
            statusDiv.textContent = "Error: Batch size must be a positive number.";
            return;
        }
        if (isNaN(offset) || offset < 0) {
            statusDiv.textContent = "Error: Offset must be a non-negative number.";
            return;
        }
        // if (isNaN(torRestartInterval) || torRestartInterval < 1) {
        //     statusDiv.textContent = "Error: Tor restart interval must be a positive number.";
        //     return;
        // }
        statusDiv.textContent = "Processing...";
        runButton.disabled = true;
        runButton.classList.add("bg-gray-600", "cursor-not-allowed");
        runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
        stopButton.disabled = false;
        stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
        stopButton.classList.add("bg-red-600", "hover:bg-red-700");
        
        try {
            const response = await fetch("/api/steps/5", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    input_csv: inputCsv,
                    max_rows: maxRows,
                    batch_size: batchSize,
                    delete_no_website: deleteNoWebsite,
                    offset: offset
                    // tor_restart_interval: torRestartInterval
                })
            });
            const result = await response.json();
            if (response.ok) {
                statusDiv.textContent = result.message;
                const jobId = result.job_id;
                // Start polling for the new job
                if (progressInterval5) {
                    clearInterval(progressInterval5);
                }
                progressInterval5 = setInterval(async () => {
                    try {
                        const response = await fetch(`/api/progress/5?job_id=${jobId}`);
                        const result = await response.json();
                        if (result.progress) {
                            statusDiv.textContent = result.progress;
                        }
                        // Update button states based on job status
                        if (result.status === "running") {
                            runButton.disabled = true;
                            runButton.classList.add("bg-gray-600", "cursor-not-allowed");
                            runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
                            stopButton.disabled = false;
                            stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                            stopButton.classList.add("bg-red-600", "hover:bg-red-700");
                        } else if (result.status === "completed" || result.status === "stopped") {
                            clearInterval(progressInterval5);
                            progressInterval5 = null;
                            statusDiv.textContent = `Job ${result.status} (${result.current_row}/${result.total_rows} rows processed)`;
                            runButton.disabled = false;
                            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                            stopButton.disabled = true;
                            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                            checkStepAvailability();
                            populateJobDropdown("job_select_step5", 5);
                        }
                    } catch (error) {
                        console.error("Progress polling error for Step 5:", error);
                        statusDiv.textContent = `Error: ${error.message}`;
                    }
                }, 5000);
                // Refresh job dropdown
                populateJobDropdown("job_select_step5", 5);
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
                runButton.disabled = false;
                runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                stopButton.disabled = true;
                stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
            runButton.disabled = false;
            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
            stopButton.disabled = true;
            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
        }
    });

    document.getElementById("stop_step5").addEventListener("click", async () => {
        const statusDiv = document.getElementById("status5");
        const runButton = document.getElementById("run_step5");
        const stopButton = document.getElementById("stop_step5");
        statusDiv.textContent = "Sending stop signal...";
        try {
            const response = await fetch("/api/stop/5", {
                method: "POST",
                headers: { "Content-Type": "application/json" }
            });
            const result = await response.json();
            if (response.ok) {
                statusDiv.textContent = result.message;
                if (progressInterval5) {
                    clearInterval(progressInterval5);
                    progressInterval5 = null;
                }
                // Update button states
                runButton.disabled = false;
                runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                stopButton.disabled = true;
                stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                // Refresh job dropdown
                populateJobDropdown("job_select_step5", 5);
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
        }
    });

    let progressInterval6 = null;
    document.getElementById("run_step6").addEventListener("click", async () => {
        const inputCsv = document.getElementById("input_csv6").value;
        const maxRows = parseInt(document.getElementById("max_rows_step6").value);
        const batchSize = parseInt(document.getElementById("batch_size_step6").value);
        const deleteNoEmail = document.getElementById("delete_no_email").checked;
        const offset = parseInt(document.getElementById("offset_step6").value);
        const torRestartInterval = parseInt(document.getElementById("tor_restart_interval_step6").value);
        const statusDiv = document.getElementById("status6");
        const runButton = document.getElementById("run_step6");
        const stopButton = document.getElementById("stop_step6");

        if (!inputCsv) {
            statusDiv.textContent = "Error: Please select an input CSV.";
            return;
        }
        if (isNaN(maxRows) || maxRows < 1) {
            statusDiv.textContent = "Error: Maximum rows must be a positive number.";
            return;
        }
        if (isNaN(batchSize) || batchSize < 1) {
            statusDiv.textContent = "Error: Batch size must be a positive number.";
            return;
        }
        if (isNaN(offset) || offset < 0) {
            statusDiv.textContent = "Error: Offset must be a non-negative number.";
            return;
        }
        if (isNaN(torRestartInterval) || torRestartInterval < 1) {
            statusDiv.textContent = "Error: Tor restart interval must be a positive number.";
            return;
        }

        statusDiv.textContent = "Starting email finder process...";
        runButton.disabled = true;
        runButton.classList.add("bg-gray-600", "cursor-not-allowed");
        runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
        stopButton.disabled = false;
        stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
        stopButton.classList.add("bg-red-600", "hover:bg-red-700");

        try {
            const response = await fetch("/api/steps/6", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    input_csv: inputCsv,
                    max_rows: maxRows,
                    batch_size: batchSize,
                    delete_no_email: deleteNoEmail,
                    offset: offset,
                    tor_restart_interval: torRestartInterval
                })
            });
            const result = await response.json();
            if (response.ok) {
                statusDiv.textContent = result.message;
                const jobId = result.job_id;
                // Start polling for the new job
                if (progressInterval6) {
                    clearInterval(progressInterval6);
                }
                progressInterval6 = setInterval(async () => {
                    try {
                        const response = await fetch(`/api/progress/6?job_id=${jobId}`);
                        const result = await response.json();
                        if (result.progress) {
                            statusDiv.textContent = result.progress;
                        }
                        // Update button states based on job status
                        if (result.status === "running") {
                            runButton.disabled = true;
                            runButton.classList.add("bg-gray-600", "cursor-not-allowed");
                            runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
                            stopButton.disabled = false;
                            stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                            stopButton.classList.add("bg-red-600", "hover:bg-red-700");
                        } else if (result.status === "completed" || result.status === "stopped") {
                            clearInterval(progressInterval6);
                            progressInterval6 = null;
                            statusDiv.textContent = `Job ${result.status} (${result.current_row}/${result.total_rows} rows processed)`;
                            runButton.disabled = false;
                            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                            stopButton.disabled = true;
                            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                            checkStepAvailability();
                            populateJobDropdown("job_select_step6", 6);
                        }
                    } catch (error) {
                        console.error("Progress polling error for Step 6:", error);
                        statusDiv.textContent = `Error: ${error.message}`;
                    }
                }, 5000);
                // Refresh job dropdown
                populateJobDropdown("job_select_step6", 6);
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
                runButton.disabled = false;
                runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                stopButton.disabled = true;
                stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
            runButton.disabled = false;
            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
            stopButton.disabled = true;
            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
        }
    });

    document.getElementById("stop_step6").addEventListener("click", async () => {
        const statusDiv = document.getElementById("status6");
        const runButton = document.getElementById("run_step6");
        const stopButton = document.getElementById("stop_step6");
        statusDiv.textContent = "Sending stop signal...";
        try {
            const response = await fetch("/api/stop/6", {
                method: "POST",
                headers: { "Content-Type": "application/json" }
            });
            const result = await response.json();
            if (response.ok) {
                statusDiv.textContent = result.message;
                if (progressInterval6) {
                    clearInterval(progressInterval6);
                    progressInterval6 = null;
                }
                // Update button states
                runButton.disabled = false;
                runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                stopButton.disabled = true;
                stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                // Refresh job dropdown
                populateJobDropdown("job_select_step6", 6);
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
        }
    });

    let progressInterval7 = null;
    document.getElementById("run_step7").addEventListener("click", async () => {
        const inputCsv = document.getElementById("input_csv7").value;
        const maxRows = parseInt(document.getElementById("max_rows_step7").value);
        const batchSize = parseInt(document.getElementById("batch_size_step7").value);
        const deleteInvalid = document.getElementById("delete_invalid").checked;
        const offset = parseInt(document.getElementById("offset_step7").value);
        const torRestartInterval = parseInt(document.getElementById("tor_restart_interval_step7").value);
        const statusDiv = document.getElementById("status7");
        const runButton = document.getElementById("run_step7");
        const stopButton = document.getElementById("stop_step7");

        if (!inputCsv) {
            statusDiv.textContent = "Error: Please select an input CSV.";
            return;
        }
        if (isNaN(maxRows) || maxRows < 1) {
            statusDiv.textContent = "Error: Maximum rows must be a positive number.";
            return;
        }
        if (isNaN(batchSize) || batchSize < 1) {
            statusDiv.textContent = "Error: Batch size must be a positive number.";
            return;
        }
        if (isNaN(offset) || offset < 0) {
            statusDiv.textContent = "Error: Offset must be a non-negative number.";
            return;
        }
        if (isNaN(torRestartInterval) || torRestartInterval < 1) {
            statusDiv.textContent = "Error: Tor restart interval must be a positive number.";
            return;
        }

        statusDiv.textContent = "Starting email verification process...";
        runButton.disabled = true;
        runButton.classList.add("bg-gray-600", "cursor-not-allowed");
        runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
        stopButton.disabled = false;
        stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
        stopButton.classList.add("bg-red-600", "hover:bg-red-700");

        try {
            const response = await fetch("/api/steps/7", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    input_csv: inputCsv,
                    max_rows: maxRows,
                    batch_size: batchSize,
                    delete_invalid: deleteInvalid,
                    offset: offset,
                    tor_restart_interval: torRestartInterval
                })
            });
            const result = await response.json();
            if (response.ok) {
                statusDiv.textContent = result.message;
                const jobId = result.job_id;
                // Start polling for the new job
                if (progressInterval7) {
                    clearInterval(progressInterval7);
                }
                progressInterval7 = setInterval(async () => {
                    try {
                        const response = await fetch(`/api/progress/7?job_id=${jobId}`);
                        const result = await response.json();
                        if (result.progress) {
                            statusDiv.textContent = result.progress;
                        }
                        // Update button states based on job status
                        if (result.status === "running") {
                            runButton.disabled = true;
                            runButton.classList.add("bg-gray-600", "cursor-not-allowed");
                            runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
                            stopButton.disabled = false;
                            stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                            stopButton.classList.add("bg-red-600", "hover:bg-red-700");
                        } else if (result.status === "completed" || result.status === "stopped") {
                            clearInterval(progressInterval7);
                            progressInterval7 = null;
                            statusDiv.textContent = `Job ${result.status} (${result.current_row}/${result.total_rows} rows processed)`;
                            runButton.disabled = false;
                            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                            stopButton.disabled = true;
                            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                            checkStepAvailability();
                            populateJobDropdown("job_select_step7", 7);
                        }
                    } catch (error) {
                        console.error("Progress polling error for Step 7:", error);
                        statusDiv.textContent = `Error: ${error.message}`;
                    }
                }, 5000);
                // Refresh job dropdown
                populateJobDropdown("job_select_step7", 7);
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
                runButton.disabled = false;
                runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                stopButton.disabled = true;
                stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
            runButton.disabled = false;
            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
            stopButton.disabled = true;
            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
        }
    });

    document.getElementById("stop_step7").addEventListener("click", async () => {
        const statusDiv = document.getElementById("status7");
        const runButton = document.getElementById("run_step7");
        const stopButton = document.getElementById("stop_step7");
        statusDiv.textContent = "Sending stop signal...";
        try {
            const response = await fetch("/api/stop/7", {
                method: "POST",
                headers: { "Content-Type": "application/json" }
            });
            const result = await response.json();
            if (response.ok) {
                statusDiv.textContent = result.message;
                if (progressInterval7) {
                    clearInterval(progressInterval7);
                    progressInterval7 = null;
                }
                // Update button states
                runButton.disabled = false;
                runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                stopButton.disabled = true;
                stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                // Refresh job dropdown
                populateJobDropdown("job_select_step7", 7);
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
        }
    });

    let progressInterval8 = null;
    document.getElementById("run_step8").addEventListener("click", async () => {
        const inputCsv = document.getElementById("input_csv8").value;
        const maxRows = parseInt(document.getElementById("max_rows_step8").value);
        const batchSize = parseInt(document.getElementById("batch_size_step8").value);
        const deleteNoIcebreaker = document.getElementById("delete_no_icebreaker").checked;
        const offset = parseInt(document.getElementById("offset_step8").value);
        const agentPrompt = document.getElementById("agent_prompt").value.trim();
        const statusDiv = document.getElementById("status8");
        const runButton = document.getElementById("run_step8");
        const stopButton = document.getElementById("stop_step8");

        if (!inputCsv) {
            statusDiv.textContent = "Error: Please select an input CSV.";
            return;
        }
        if (isNaN(maxRows) || maxRows < 1) {
            statusDiv.textContent = "Error: Maximum rows must be a positive number.";
            return;
        }
        if (isNaN(batchSize) || batchSize < 1) {
            statusDiv.textContent = "Error: Batch size must be a positive number.";
            return;
        }
        if (isNaN(offset) || offset < 0) {
            statusDiv.textContent = "Error: Offset must be a non-negative number.";
            return;
        }
        if (!agentPrompt) {
            statusDiv.textContent = "Error: Please enter an agent prompt.";
            return;
        }

        statusDiv.textContent = "Starting icebreaker generation process...";
        runButton.disabled = true;
        runButton.classList.add("bg-gray-600", "cursor-not-allowed");
        runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
        stopButton.disabled = false;
        stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
        stopButton.classList.add("bg-red-600", "hover:bg-red-700");

        try {
            const response = await fetch("/api/steps/8", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    input_csv: inputCsv,
                    max_rows: maxRows,
                    batch_size: batchSize,
                    delete_no_icebreaker: deleteNoIcebreaker,
                    offset: offset,
                    agent_prompt: agentPrompt
                })
            });
            const result = await response.json();
            if (response.ok) {
                statusDiv.textContent = result.message;
                const jobId = result.job_id;
                if (progressInterval8) {
                    clearInterval(progressInterval8);
                }
                progressInterval8 = setInterval(async () => {
                    try {
                        const response = await fetch(`/api/progress/8?job_id=${jobId}`);
                        const result = await response.json();
                        if (result.progress) {
                            statusDiv.textContent = result.progress;
                        }
                        if (result.status === "running") {
                            runButton.disabled = true;
                            runButton.classList.add("bg-gray-600", "cursor-not-allowed");
                            runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
                            stopButton.disabled = false;
                            stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                            stopButton.classList.add("bg-red-600", "hover:bg-red-700");
                        } else if (result.status === "completed" || result.status === "stopped") {
                            clearInterval(progressInterval8);
                            progressInterval8 = null;
                            statusDiv.textContent = `Job ${result.status} (${result.current_row}/${result.total_rows} rows processed)`;
                            runButton.disabled = false;
                            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                            stopButton.disabled = true;
                            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                            checkStepAvailability();
                            populateJobDropdown("job_select_step8", 8);
                        }
                    } catch (error) {
                        console.error("Progress polling error for Step 8:", error);
                        statusDiv.textContent = `Error: ${error.message}`;
                    }
                }, 5000);
                populateJobDropdown("job_select_step8", 8);
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
                runButton.disabled = false;
                runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                stopButton.disabled = true;
                stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
            runButton.disabled = false;
            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
            stopButton.disabled = true;
            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
        }
    });

    document.getElementById("stop_step8").addEventListener("click", async () => {
        const statusDiv = document.getElementById("status8");
        const runButton = document.getElementById("run_step8");
        const stopButton = document.getElementById("stop_step8");
        statusDiv.textContent = "Sending stop signal...";
        try {
            const response = await fetch("/api/stop/8", {
                method: "POST",
                headers: { "Content-Type": "application/json" }
            });
            const result = await response.json();
            if (response.ok) {
                statusDiv.textContent = result.message;
                if (progressInterval8) {
                    clearInterval(progressInterval8);
                    progressInterval8 = null;
                }
                runButton.disabled = false;
                runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                stopButton.disabled = true;
                stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                populateJobDropdown("job_select_step8", 8);
            } else {
                statusDiv.textContent = `Error: ${result.error}`;
            }
        } catch (error) {
            statusDiv.textContent = `Error: ${error.message}`;
        }
    });

    // Job selection for Step 5
    document.getElementById("job_select_step5").addEventListener("change", async () => {
        const jobId = document.getElementById("job_select_step5").value;
        const statusDiv = document.getElementById("status5");
        const runButton = document.getElementById("run_step5");
        const stopButton = document.getElementById("stop_step5");
        if (!jobId) {
            statusDiv.textContent = "";
            if (progressInterval5) {
                clearInterval(progressInterval5);
                progressInterval5 = null;
            }
            runButton.disabled = false;
            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
            stopButton.disabled = true;
            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
            return;
        }
        if (progressInterval5) {
            clearInterval(progressInterval5);
        }
        progressInterval5 = setInterval(async () => {
            try {
                const [progressResponse, jobsResponse] = await Promise.all([
                    fetch(`/api/progress/5?job_id=${jobId}`),
                    fetch(`/api/jobs/5`)
                ]);
                const progressResult = await progressResponse.json();
                const jobsResult = await jobsResponse.json();
                const job = jobsResult.jobs.find(j => j.job_id === jobId);
                const jobStatus = job ? job.status : progressResult.status;

                if (progressResult.progress && jobStatus === "running") {
                    statusDiv.textContent = progressResult.progress;
                } else {
                    statusDiv.textContent = `Job ${jobStatus} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                }

                // Update button states based on job status
                if (jobStatus === "running") {
                    runButton.disabled = true;
                    runButton.classList.add("bg-gray-600", "cursor-not-allowed");
                    runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
                    stopButton.disabled = false;
                    stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                    stopButton.classList.add("bg-red-600", "hover:bg-red-700");
                } else if (jobStatus === "completed" || jobStatus === "stopped") {
                    clearInterval(progressInterval5);
                    progressInterval5 = null;
                    statusDiv.textContent = `Job ${jobStatus} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                    runButton.disabled = false;
                    runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                    runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                    stopButton.disabled = true;
                    stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                    stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                    checkStepAvailability();
                    populateJobDropdown("job_select_step5", 5);
                }
            } catch (error) {
                console.error("Progress polling error for Step 5:", error);
                statusDiv.textContent = `Error: ${error.message}`;
            }
        }, 5000);
    });

    // Job selection for Step 6
    document.getElementById("job_select_step6").addEventListener("change", async () => {
        const jobId = document.getElementById("job_select_step6").value;
        const statusDiv = document.getElementById("status6");
        const runButton = document.getElementById("run_step6");
        const stopButton = document.getElementById("stop_step6");
        if (!jobId) {
            statusDiv.textContent = "";
            if (progressInterval6) {
                clearInterval(progressInterval6);
                progressInterval6 = null;
            }
            runButton.disabled = false;
            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
            stopButton.disabled = true;
            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
            return;
        }
        if (progressInterval6) {
            clearInterval(progressInterval6);
        }
        progressInterval6 = setInterval(async () => {
            try {
                const [progressResponse, jobsResponse] = await Promise.all([
                    fetch(`/api/progress/6?job_id=${jobId}`),
                    fetch(`/api/jobs/6`)
                ]);
                const progressResult = await progressResponse.json();
                const jobsResult = await jobsResponse.json();
                const job = jobsResult.jobs.find(j => j.job_id === jobId);
                const jobStatus = job ? job.status : progressResult.status;

                if (progressResult.progress && jobStatus === "running") {
                    statusDiv.textContent = progressResult.progress;
                } else {
                    statusDiv.textContent = `Job ${jobStatus} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                }

                // Update button states based on job status
                if (jobStatus === "running") {
                    runButton.disabled = true;
                    runButton.classList.add("bg-gray-600", "cursor-not-allowed");
                    runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
                    stopButton.disabled = false;
                    stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                    stopButton.classList.add("bg-red-600", "hover:bg-red-700");
                } else if (jobStatus === "completed" || jobStatus === "stopped") {
                    clearInterval(progressInterval6);
                    progressInterval6 = null;
                    statusDiv.textContent = `Job ${jobStatus} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                    runButton.disabled = false;
                    runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                    runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                    stopButton.disabled = true;
                    stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                    stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                    checkStepAvailability();
                    populateJobDropdown("job_select_step6", 6);
                }
            } catch (error) {
                console.error("Progress polling error for Step 6:", error);
                statusDiv.textContent = `Error: ${error.message}`;
            }
        }, 5000);
    });

    // Job selection for Step 7
    document.getElementById("job_select_step7").addEventListener("change", async () => {
        const jobId = document.getElementById("job_select_step7").value;
        const statusDiv = document.getElementById("status7");
        const runButton = document.getElementById("run_step7");
        const stopButton = document.getElementById("stop_step7");
        if (!jobId) {
            statusDiv.textContent = "";
            if (progressInterval7) {
                clearInterval(progressInterval7);
                progressInterval7 = null;
            }
            runButton.disabled = false;
            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
            stopButton.disabled = true;
            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
            return;
        }
        if (progressInterval7) {
            clearInterval(progressInterval7);
        }
        progressInterval7 = setInterval(async () => {
            try {
                const [progressResponse, jobsResponse] = await Promise.all([
                    fetch(`/api/progress/7?job_id=${jobId}`),
                    fetch(`/api/jobs/7`)
                ]);
                const progressResult = await progressResponse.json();
                const jobsResult = await jobsResponse.json();
                const job = jobsResult.jobs.find(j => j.job_id === jobId);
                const jobStatus = job ? job.status : progressResult.status;

                if (progressResult.progress && jobStatus === "running") {
                    statusDiv.textContent = progressResult.progress;
                } else {
                    statusDiv.textContent = `Job ${jobStatus} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                }

                if (jobStatus === "running") {
                    runButton.disabled = true;
                    runButton.classList.add("bg-gray-600", "cursor-not-allowed");
                    runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
                    stopButton.disabled = false;
                    stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                    stopButton.classList.add("bg-red-600", "hover:bg-red-700");
                } else if (jobStatus === "completed" || jobStatus === "stopped") {
                    clearInterval(progressInterval7);
                    progressInterval7 = null;
                    statusDiv.textContent = `Job ${jobStatus} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                    runButton.disabled = false;
                    runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                    runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                    stopButton.disabled = true;
                    stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                    stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                    checkStepAvailability();
                    populateJobDropdown("job_select_step7", 7);
                }
            } catch (error) {
                console.error("Progress polling error for Step 7:", error);
                statusDiv.textContent = `Error: ${error.message}`;
            }
        }, 5000);
    });

    // Job selection for Step 8
    document.getElementById("job_select_step8").addEventListener("change", async () => {
        const jobId = document.getElementById("job_select_step8").value;
        const statusDiv = document.getElementById("status8");
        const runButton = document.getElementById("run_step8");
        const stopButton = document.getElementById("stop_step8");
        if (!jobId) {
            statusDiv.textContent = "";
            if (progressInterval8) {
                clearInterval(progressInterval8);
                progressInterval8 = null;
            }
            runButton.disabled = false;
            runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
            runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
            stopButton.disabled = true;
            stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
            stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
            return;
        }
        if (progressInterval8) {
            clearInterval(progressInterval8);
        }
        progressInterval8 = setInterval(async () => {
            try {
                const [progressResponse, jobsResponse] = await Promise.all([
                    fetch(`/api/progress/8?job_id=${jobId}`),
                    fetch(`/api/jobs/8`)
                ]);
                const progressResult = await progressResponse.json();
                const jobsResult = await jobsResponse.json();
                const job = jobsResult.jobs.find(j => j.job_id === jobId);
                const jobStatus = job ? job.status : progressResult.status;

                if (progressResult.progress && jobStatus === "running") {
                    statusDiv.textContent = progressResult.progress;
                } else {
                    statusDiv.textContent = `Job ${jobStatus} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                }

                if (jobStatus === "running") {
                    runButton.disabled = true;
                    runButton.classList.add("bg-gray-600", "cursor-not-allowed");
                    runButton.classList.remove("bg-blue-600", "hover:bg-blue-700");
                    stopButton.disabled = false;
                    stopButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                    stopButton.classList.add("bg-red-600", "hover:bg-red-700");
                } else if (jobStatus === "completed" || jobStatus === "stopped") {
                    clearInterval(progressInterval8);
                    progressInterval8 = null;
                    statusDiv.textContent = `Job ${jobStatus} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                    runButton.disabled = false;
                    runButton.classList.remove("bg-gray-600", "cursor-not-allowed");
                    runButton.classList.add("bg-blue-600", "hover:bg-blue-700");
                    stopButton.disabled = true;
                    stopButton.classList.add("bg-gray-600", "cursor-not-allowed");
                    stopButton.classList.remove("bg-red-600", "hover:bg-red-700");
                    checkStepAvailability();
                    populateJobDropdown("job_select_step8", 8);
                }
            } catch (error) {
                console.error("Progress polling error for Step 8:", error);
                statusDiv.textContent = `Error: ${error.message}`;
            }
        }, 5000);
    });

    async function populateCsvDropdown(selectId, folder) {
        try {
            const response = await fetch(`/api/files/${folder}`);
            const result = await response.json();
            const select = document.getElementById(selectId);
            select.innerHTML = '<option value="">Select a CSV file</option>';
            if (result.files) {
                result.files.forEach(file => {
                    const option = document.createElement("option");
                    option.value = file;
                    option.textContent = file;
                    select.appendChild(option);
                });
            }
        } catch (error) {
            console.error(`Error populating CSV dropdown for ${folder}:`, error);
        }
    }

    async function populateJobDropdown(selectId, step) {
        try {
            const response = await fetch(`/api/jobs/${step}`);
            const result = await response.json();
            const select = document.getElementById(selectId);
            select.innerHTML = '<option value="">Select a job</option>';
            if (result.jobs) {
                result.jobs.forEach(job => {
                    const option = document.createElement("option");
                    option.value = job.job_id;
                    option.textContent = `${job.input_csv} (${job.status})`;
                    select.appendChild(option);
                });
            }
        } catch (error) {
            console.error(`Error populating job dropdown for step ${step}:`, error);
        }
    }

    async function checkStepAvailability() {
        try {
        // Check Step 2 availability
            let response = await fetch("/api/files/csv");
            let result = await response.json();
            if (result.files && result.files.length > 0) {
                document.getElementById("step2").disabled = false;
                document.getElementById("step2").classList.remove("bg-gray-600", "cursor-not-allowed");
                document.getElementById("step2").classList.add("bg-blue-600", "hover:bg-blue-700");
            }

        // Check Step 3 availability
            response = await fetch("/api/files/filtered_url");
            result = await response.json();
            if (result.files && result.files.length > 0) {
                document.getElementById("step3").disabled = false;
                document.getElementById("step3").classList.remove("bg-gray-600", "cursor-not-allowed");
                document.getElementById("step3").classList.add("bg-blue-600", "hover:bg-blue-700");
            }

        // // Check Step 4 availability
        // response = await fetch("/api/files/updated_name");
        // result = await response.json();
        // if (result.files && result.files.length > 0) {
        //     document.getElementById("step4").disabled = false;
        //     document.getElementById("step4").classList.remove("bg-gray-600", "cursor-not-allowed");
        //     document.getElementById("step4").classList.add("bg-blue-600", "hover:bg-blue-700");
        // }
        // response = await fetch("/api/files/updated_url");
        // result = await response.json();
        // if (result.files && result.files.length > 0) {
        //     document.getElementById("step5").disabled = false;
        //     document.getElementById("step5").classList.remove("bg-gray-600", "cursor-not-allowed");
        //     document.getElementById("step5").classList.add("bg-blue-600", "hover:bg-blue-700");
        // }
        // Check Step 5 availability (skipping Step 4)
            response = await fetch("/api/files/updated_name");
            result = await response.json();
            if (result.files && result.files.length > 0) {
                document.getElementById("step5").disabled = false;
                document.getElementById("step5").classList.remove("bg-gray-600", "cursor-not-allowed");
                document.getElementById("step5").classList.add("bg-blue-600", "hover:bg-blue-700");
            }

        // Check Step 6 availability
        response = await fetch("/api/files/domain_about");
        result = await response.json();
        if (result.files && result.files.length > 0) {
            document.getElementById("step6").disabled = false;
            document.getElementById("step6").classList.remove("bg-gray-600", "cursor-not-allowed");
            document.getElementById("step6").classList.add("bg-blue-600", "hover:bg-blue-700");
        }

        // Check Step 7 availability
        response = await fetch("/api/files/emails");
        result = await response.json();
        if (result.files && result.files.length > 0) {
            document.getElementById("step7").disabled = false;
            document.getElementById("step7").classList.remove("bg-gray-600", "cursor-not-allowed");
            document.getElementById("step7").classList.add("bg-blue-600", "hover:bg-blue-700");
        }

        // Check Step 8 availability
        response = await fetch("/api/files/verified");
        result = await response.json();
        if (result.files && result.files.length > 0) {
            document.getElementById("step8").disabled = false;
            document.getElementById("step8").classList.remove("bg-gray-600", "cursor-not-allowed");
            document.getElementById("step8").classList.add("bg-blue-600", "hover:bg-blue-700");
        }
    } catch (error) {
        console.error("Error checking step availability:", error);
    }
    }

    async function checkRunningJobs() {
        try {
        // Check Step 5 running jobs
            let response = await fetch("/api/jobs/5");
            let result = await response.json();
            const runButton5 = document.getElementById("run_step5");
            const stopButton5 = document.getElementById("stop_step5");
            const statusDiv5 = document.getElementById("status5");
            if (result.jobs && result.jobs.some(job => job.status === "running")) {
                runButton5.disabled = true;
                runButton5.classList.add("bg-gray-600", "cursor-not-allowed");
                runButton5.classList.remove("bg-blue-600", "hover:bg-blue-700");
                stopButton5.disabled = false;
                stopButton5.classList.remove("bg-gray-600", "cursor-not-allowed");
                stopButton5.classList.add("bg-red-600", "hover:bg-red-700");
                const job = result.jobs.find(j => j.status === "running");
                if (job) {
                    statusDiv5.textContent = `Running job ${job.job_id}...`;
                    if (progressInterval5) clearInterval(progressInterval5);
                    progressInterval5 = setInterval(async () => {
                        const progressResponse = await fetch(`/api/progress/5?job_id=${job.job_id}`);
                        const progressResult = await progressResponse.json();
                        if (progressResult.progress) {
                            statusDiv5.textContent = progressResult.progress;
                        }
                        if (progressResult.status !== "running") {
                            clearInterval(progressInterval5);
                            progressInterval5 = null;
                            statusDiv5.textContent = `Job ${progressResult.status} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                            runButton5.disabled = false;
                            runButton5.classList.remove("bg-gray-600", "cursor-not-allowed");
                            runButton5.classList.add("bg-blue-600", "hover:bg-blue-700");
                            stopButton5.disabled = true;
                            stopButton5.classList.add("bg-gray-600", "cursor-not-allowed");
                            stopButton5.classList.remove("bg-red-600", "hover:bg-red-700");
                            checkStepAvailability();
                            populateJobDropdown("job_select_step5", 5);
                        }
                    }, 5000);
                }
            }

            response = await fetch("/api/jobs/6");
            result = await response.json();
            const runButton6 = document.getElementById("run_step6");
            const stopButton6 = document.getElementById("stop_step6");
            const statusDiv6 = document.getElementById("status6");
            if (result.jobs && result.jobs.some(job => job.status === "running")) {
                runButton6.disabled = true;
                runButton6.classList.add("bg-gray-600", "cursor-not-allowed");
                runButton6.classList.remove("bg-blue-600", "hover:bg-blue-700");
                stopButton6.disabled = false;
                stopButton6.classList.remove("bg-gray-600", "cursor-not-allowed");
                stopButton6.classList.add("bg-red-600", "hover:bg-red-700");
                const job = result.jobs.find(j => j.status === "running");
                if (job) {
                    statusDiv6.textContent = `Running job ${job.job_id}...`;
                    if (progressInterval6) clearInterval(progressInterval6);
                    progressInterval6 = setInterval(async () => {
                        const progressResponse = await fetch(`/api/progress/6?job_id=${job.job_id}`);
                        const progressResult = await progressResponse.json();
                        if (progressResult.progress) {
                            statusDiv6.textContent = progressResult.progress;
                        }
                        if (progressResult.status !== "running") {
                            clearInterval(progressInterval6);
                            progressInterval6 = null;
                            statusDiv6.textContent = `Job ${progressResult.status} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                            runButton6.disabled = false;
                            runButton6.classList.remove("bg-gray-600", "cursor-not-allowed");
                            runButton6.classList.add("bg-blue-600", "hover:bg-blue-700");
                            stopButton6.disabled = true;
                            stopButton6.classList.add("bg-gray-600", "cursor-not-allowed");
                            stopButton6.classList.remove("bg-red-600", "hover:bg-red-700");
                            checkStepAvailability();
                            populateJobDropdown("job_select_step6", 6);
                        }
                    }, 5000);
                }
            }

            response = await fetch("/api/jobs/7");
            result = await response.json();
            const runButton7 = document.getElementById("run_step7");
            const stopButton7 = document.getElementById("stop_step7");
            const statusDiv7 = document.getElementById("status7");
            if (result.jobs && result.jobs.some(job => job.status === "running")) {
                runButton7.disabled = true;
                runButton7.classList.add("bg-gray-600", "cursor-not-allowed");
                runButton7.classList.remove("bg-blue-600", "hover:bg-blue-700");
                stopButton7.disabled = false;
                stopButton7.classList.remove("bg-gray-600", "cursor-not-allowed");
                stopButton7.classList.add("bg-red-600", "hover:bg-red-700");
                const job = result.jobs.find(j => j.status === "running");
                if (job) {
                    statusDiv7.textContent = `Running job ${job.job_id}...`;
                    if (progressInterval7) clearInterval(progressInterval7);
                    progressInterval7 = setInterval(async () => {
                        const progressResponse = await fetch(`/api/progress/7?job_id=${job.job_id}`);
                        const progressResult = await progressResponse.json();
                        if (progressResult.progress) {
                            statusDiv7.textContent = progressResult.progress;
                        }
                        if (progressResult.status !== "running") {
                            clearInterval(progressInterval7);
                            progressInterval7 = null;
                            statusDiv7.textContent = `Job ${progressResult.status} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                            runButton7.disabled = false;
                            runButton7.classList.remove("bg-gray-600", "cursor-not-allowed");
                            runButton7.classList.add("bg-blue-600", "hover:bg-blue-700");
                            stopButton7.disabled = true;
                            stopButton7.classList.add("bg-gray-600", "cursor-not-allowed");
                            stopButton7.classList.remove("bg-red-600", "hover:bg-red-700");
                            checkStepAvailability();
                            populateJobDropdown("job_select_step7", 7);
                        }
                    }, 5000);
                }
            }

            response = await fetch("/api/jobs/8");
            result = await response.json();
            const runButton8 = document.getElementById("run_step8");
            const stopButton8 = document.getElementById("stop_step8");
            const statusDiv8 = document.getElementById("status8");
            if (result.jobs && result.jobs.some(job => job.status === "running")) {
                runButton8.disabled = true;
                runButton8.classList.add("bg-gray-600", "cursor-not-allowed");
                runButton8.classList.remove("bg-blue-600", "hover:bg-blue-700");
                stopButton8.disabled = false;
                stopButton8.classList.remove("bg-gray-600", "cursor-not-allowed");
                stopButton8.classList.add("bg-red-600", "hover:bg-red-700");
                const job = result.jobs.find(j => j.status === "running");
                if (job) {
                    statusDiv8.textContent = `Running job ${job.job_id}...`;
                    if (progressInterval8) clearInterval(progressInterval8);
                    progressInterval8 = setInterval(async () => {
                        const progressResponse = await fetch(`/api/progress/8?job_id=${job.job_id}`);
                        const progressResult = await progressResponse.json();
                        if (progressResult.progress) {
                            statusDiv8.textContent = progressResult.progress;
                        }
                        if (progressResult.status !== "running") {
                            clearInterval(progressInterval8);
                            progressInterval8 = null;
                            statusDiv8.textContent = `Job ${progressResult.status} (${progressResult.current_row}/${progressResult.total_rows} rows processed)`;
                            runButton8.disabled = false;
                            runButton8.classList.remove("bg-gray-600", "cursor-not-allowed");
                            runButton8.classList.add("bg-blue-600", "hover:bg-blue-700");
                            stopButton8.disabled = true;
                            stopButton8.classList.add("bg-gray-600", "cursor-not-allowed");
                            stopButton8.classList.remove("bg-red-600", "hover:bg-red-700");
                            checkStepAvailability();
                            populateJobDropdown("job_select_step8", 8);
                        }
                    }, 5000);
                }
            }
        } catch (error) {
            console.error("Error checking running jobs:", error);
        }
    }
});