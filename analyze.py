import re
from datetime import datetime, timedelta
from collections import defaultdict

def parse_log_file(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
        return

    # Data structures
    worker_task_durations = defaultdict(list)
    worker_start_times = {}
    worker_end_times = {}
    worker_last_active = {}
    failed_probes = []
    global_end_time = None

    # Regex patterns
    timestamp_pattern = re.compile(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]")
    worker_task_pattern = re.compile(r"Worker (\d+): Task offset=(\d+) length=(\d+) took ([\d\.]+)s")
    worker_event_pattern = re.compile(r"Worker (\d+) (started|finished)")
    dl_complete_pattern = re.compile(r"Download .+ completed in")
    probe_fail_pattern = re.compile(r"Probe failed: (.+)")
    file_info_pattern = re.compile(r"Probe complete - filename: (.+), size: (\d+)")

    print(f"--- Analyzing {filename} ---\n")

    current_time = None

    for line in lines:
        line = line.strip()
        
        # Extract Timestamp
        ts_match = timestamp_pattern.match(line)
        if ts_match:
            try:
                current_time = datetime.strptime(ts_match.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

        # 1. Global Finish Time
        if dl_complete_pattern.search(line) and current_time:
            global_end_time = current_time

        # 2. Worker Lifecycle (Start/Finish)
        w_event_match = worker_event_pattern.search(line)
        if w_event_match and current_time:
            w_id = int(w_event_match.group(1))
            event_type = w_event_match.group(2)
            if event_type == "started":
                worker_start_times[w_id] = current_time
            elif event_type == "finished":
                worker_end_times[w_id] = current_time
            continue

        # 3. Task Performance
        w_task_match = worker_task_pattern.search(line)
        if w_task_match and current_time:
            worker_id = int(w_task_match.group(1))
            duration = float(w_task_match.group(4))
            worker_task_durations[worker_id].append(duration)
            worker_last_active[worker_id] = current_time
            continue

        # 4. Failures & Info
        if "Probe failed" in line:
            fail_match = probe_fail_pattern.search(line)
            reason = fail_match.group(1) if fail_match else "Unknown"
            failed_probes.append(f"[{ts_match.group(1)}] {reason}")

        if "Probe complete" in line:
            info_match = file_info_pattern.search(line)
            if info_match:
                fname = info_match.group(1)
                fsize_gb = int(info_match.group(2)) / (1024**3)
                print(f"✅ File Found: {fname} ({fsize_gb:.2f} GB)")

    # --- Output Results ---

    # Fallback for global end time
    if not global_end_time:
        global_end_time = current_time

    if failed_probes:
        print("\n❌ Failures Detected:")
        for fail in failed_probes:
            print(f"  {fail}")

    if worker_task_durations:
        print("\n🚀 Worker Thread Detailed Analysis:")
        # Header
        print(f"{'ID':<3} | {'Avg Time':<9} | {'Utilization':<11} | {'Wasted (Wait)':<15}")
        print("-" * 50)
        
        worker_ids = sorted(worker_task_durations.keys())
        worker_averages = {}

        for wid in worker_ids:
            tasks = worker_task_durations[wid]
            count = len(tasks)
            total_work_time = sum(tasks)
            avg_time = total_work_time / count if count > 0 else 0.0
            worker_averages[wid] = avg_time
            
            # Wall Time & Utilization
            start = worker_start_times.get(wid)
            end = worker_end_times.get(wid)
            utilization_str = "N/A"
            
            if start and end:
                wall_seconds = (end - start).total_seconds()
                
                # --- FIX: Handle Precision Errors ---
                # If logs are rounded to seconds, Wall Time might be slightly less than Work Time.
                # We assume if Work Time > Wall Time, the worker was effectively 100% busy.
                if wall_seconds > 0:
                    if total_work_time > wall_seconds:
                        util_pct = 100.0
                    else:
                        util_pct = (total_work_time / wall_seconds) * 100
                    utilization_str = f"{util_pct:.1f}%"

            # Wasted Time (Global End - Last Active)
            last_active = worker_last_active.get(wid)
            wasted_str = "0s"
            if last_active and global_end_time:
                wasted = (global_end_time - last_active).total_seconds()
                wasted_str = f"{wasted:.0f}s"
                if wasted > 5: wasted_str += " ⚠️"
                if wasted == 0: wasted_str = "Straggler 🐢"

            print(f"{wid:<3} | {avg_time:.2f}s     | {utilization_str:<11} | {wasted_str:<15}")

        print("-" * 50)

        # Fastest/Slowest Calculation
        fastest_id = min(worker_averages, key=worker_averages.get)
        slowest_id = max(worker_averages, key=worker_averages.get)
        
        fast_avg = worker_averages[fastest_id]
        slow_avg = worker_averages[slowest_id]
        
        ratio = slow_avg / fast_avg if fast_avg > 0 else 0

        print(f"\n⚡ Fastest Worker: ID {fastest_id} ({fast_avg:.2f}s/task)")
        print(f"🐢 Slowest Worker: ID {slowest_id} ({slow_avg:.2f}s/task)")
        print(f"📊 Ratio: The fastest worker was {ratio:.2f}x faster than the slowest.")
        
    else:
        print("\nℹ️ No worker task data found.")

if __name__ == "__main__":
    parse_log_file("debug.log")