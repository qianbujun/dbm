import os
import shutil
import mimetypes
from datetime import datetime
import time
import json # For loading JSON files
import uuid # For unique temp names if needed, though timestamp is primary
from db_manager import init_db, insert_data_object, FILE_STORAGE_DIR

# Monitor directory
INPUT_MONITOR_DIR = 'input_data'

# Custom MIME type mappings
FILE_TYPE_MAP = {
    '.txt': 'text/plain', '.md': 'text/markdown',
    '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
    '.png': 'image/png', '.gif': 'image/gif', '.bmp': 'image/bmp', '.svg': 'image/svg+xml',
    '.pdf': 'application/pdf',
    '.doc': 'application/msword',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.xls': 'application/vnd.ms-excel',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.ppt': 'application/vnd.ms-powerpoint',
    '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    '.csv': 'text/csv',
    '.json': 'application/json', # Standard type for JSON files
    '.zip': 'application/zip', '.gz': 'application/gzip', '.tar': 'application/x-tar',
}

# Max length for various parts of summary
MAX_SUMMARY_TEXT_SNIPPET = 200  # For text file content snippet
MAX_JSON_SNIPPET_LEN = 150      # For JSON data snippet in summary

# Specific types for JSON structures
JSON_CONTAINER_TYPE = 'application/json_container' # Original JSON files that are lists/dicts of items
JSON_ITEM_TYPE = 'application/json_item'           # Individual items extracted from a container

def get_file_type(filepath: str) -> str:
    """Determines file MIME type using extension map and mimetypes."""
    _, ext = os.path.splitext(filepath)
    ext = ext.lower()
    if ext in FILE_TYPE_MAP:
        return FILE_TYPE_MAP[ext]
    mime_type, _ = mimetypes.guess_type(filepath)
    return mime_type if mime_type else 'application/octet-stream'

def generate_stored_filename(original_filename: str, suffix_part: str = None) -> str:
    """Generates a unique filename for storage, incorporating a timestamp and optional suffix."""
    name_without_ext, ext = os.path.splitext(original_filename)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    # Sanitize name_without_ext to remove problematic characters for filenames if necessary
    # name_without_ext = re.sub(r'[^\w\.-]', '_', name_without_ext)
    if suffix_part:
        return f"{name_without_ext}_{suffix_part}_{timestamp}{ext}"
    return f"{name_without_ext}_{timestamp}{ext}"

def get_file_content_summary(filepath: str, file_type: str, original_filename_for_display: str) -> str:
    """Generates an enhanced content summary for different file types."""
    summary_parts = []
    try:
        file_size_bytes = os.path.getsize(filepath)
        file_size_kb = file_size_bytes / 1024.0
        summary_parts.extend([
            f"File: {original_filename_for_display}",
            f"Type: {file_type}",
            f"Size: {file_size_kb:.2f}KB"
        ])

        if file_type.startswith('text/'):
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content_sample = f.read(MAX_SUMMARY_TEXT_SNIPPET)
                if len(content_sample) == MAX_SUMMARY_TEXT_SNIPPET and file_size_bytes > MAX_SUMMARY_TEXT_SNIPPET:
                    content_sample += "..."
                summary_parts.append(f"Snippet: \"{content_sample}\"")
            except Exception as e:
                summary_parts.append(f"Snippet Error: {e}")

        elif file_type.startswith('image/'):
            try:
                from PIL import Image # Pillow is a soft dependency here
                with Image.open(filepath) as img:
                    width, height = img.size
                    summary_parts.append(f"Dimensions: {width}x{height}")
                    if img.format: summary_parts.append(f"Format: {img.format}")
            except ImportError:
                summary_parts.append("Image Details: Pillow library not installed.")
            except Exception as e:
                summary_parts.append(f"Image Details Error: {e}")
        
        elif file_type in ['application/json', JSON_ITEM_TYPE]: # Single JSON object or extracted item
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                data_str = json.dumps(json_data, ensure_ascii=False)
                snippet = data_str[:MAX_JSON_SNIPPET_LEN]
                if len(data_str) > MAX_JSON_SNIPPET_LEN: snippet += "..."
                
                if isinstance(json_data, list):
                    summary_parts.append(f"JSON Array, Items: {len(json_data)}")
                elif isinstance(json_data, dict):
                    summary_parts.append(f"JSON Object, Keys: {len(json_data.keys())}")
                else:
                    summary_parts.append("JSON Primitive")
                summary_parts.append(f"Data Snippet: {snippet}")
            except Exception as e:
                summary_parts.append(f"JSON Details Error: {e}")
        
        elif file_type == JSON_CONTAINER_TYPE: # Original JSON file that was a list/dict
             try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    json_data_container = json.load(f) # Load to get item count
                if isinstance(json_data_container, list):
                    summary_parts.append(f"Contains: List of {len(json_data_container)} JSON items.")
                elif isinstance(json_data_container, dict):
                     summary_parts.append(f"Contains: JSON object with {len(json_data_container.keys())} top-level entries.")
                else: # Should not happen if it's a "container" from list/dict source
                    summary_parts.append("Contains: Single JSON primitive.")
             except Exception as e:
                summary_parts.append(f"Container Details Error: {e}")
        
        return ". ".join(summary_parts)
    except FileNotFoundError:
        return f"[File Not Found Error for: {original_filename_for_display} at {filepath}]"
    except Exception as e:
        return f"[Error generating summary for {original_filename_for_display}: {e}]"


def ingest_new_data(input_dir: str, target_base_dir: str):
    """Scans input_dir, processes new files, stores them, and records in DB."""
    print(f"Scanning for new data in: {input_dir}")
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
        print(f"Created input directory: {input_dir}")
        return

    for original_filename in os.listdir(input_dir):
        source_filepath = os.path.join(input_dir, original_filename)
        if not os.path.isfile(source_filepath):
            continue

        file_type = get_file_type(source_filepath)
        
        # Stored filename for the original (or container) file
        stored_container_filename = generate_stored_filename(original_filename)
        target_container_filepath = os.path.join(target_base_dir, stored_container_filename)

        try:
            shutil.copy(source_filepath, target_container_filepath) # Copy original first

            if file_type == 'application/json':
                try:
                    with open(target_container_filepath, 'r', encoding='utf-8') as f:
                        json_content = json.load(f)

                    if isinstance(json_content, list) and json_content: # Is a non-empty list
                        # This is a list of items. Store the container and then individual items.
                        container_summary = get_file_content_summary(target_container_filepath, JSON_CONTAINER_TYPE, original_filename)
                        container_id = insert_data_object(
                            name=original_filename,
                            file_type=JSON_CONTAINER_TYPE,
                            content_location=target_container_filepath,
                            content_summary=container_summary,
                            tags=["json_container", "unclassified_list"]
                        )

                        if not container_id:
                            print(f"Failed to record JSON container '{original_filename}'. Removing copied container file.")
                            if os.path.exists(target_container_filepath): os.remove(target_container_filepath)
                            continue # Skip to next file in input_dir

                        print(f"Ingested JSON container '{original_filename}' as ID {container_id}. Ingesting items...")
                        
                        items_ingested_count = 0
                        for index, item_data in enumerate(json_content):
                            item_display_name = f"{os.path.splitext(original_filename)[0]}_item_{index}.json"
                            # Suffix for stored item filename, relates to container filename for grouping if needed
                            item_stored_suffix = f"item_{index}" 
                            item_stored_filename = generate_stored_filename(original_filename, suffix_part=item_stored_suffix)
                            item_target_filepath = os.path.join(target_base_dir, item_stored_filename)

                            try:
                                with open(item_target_filepath, 'w', encoding='utf-8') as item_f:
                                    json.dump(item_data, item_f, ensure_ascii=False, indent=2)
                                
                                item_summary = get_file_content_summary(item_target_filepath, JSON_ITEM_TYPE, item_display_name)
                                item_id = insert_data_object(
                                    name=item_display_name,
                                    file_type=JSON_ITEM_TYPE,
                                    content_location=item_target_filepath,
                                    content_summary=item_summary,
                                    tags=["json_item", "unclassified"],
                                    source_original_id=container_id,
                                    source_item_key=str(index)
                                )
                                if item_id: items_ingested_count +=1
                                else:
                                    print(f"  Failed to record JSON item {index} from '{original_filename}'. Removing item file.")
                                    if os.path.exists(item_target_filepath): os.remove(item_target_filepath)
                            except Exception as item_e:
                                print(f"  Error ingesting JSON item {index} from '{original_filename}': {item_e}")
                                if os.path.exists(item_target_filepath): os.remove(item_target_filepath) # Clean item file

                        print(f"  Successfully ingested {items_ingested_count} of {len(json_content)} items from '{original_filename}'.")
                        if items_ingested_count > 0 or container_id: # If container or any item was successful
                             os.remove(source_filepath)
                             print(f"Successfully processed and removed '{original_filename}' (as container and/or items) from input.")
                        # If container failed but items succeeded (not possible with current flow), or vice-versa, logic might need adjustment for source_filepath removal.
                        # Current flow: if container_id is None, items are not processed.

                    else: # JSON file is not a list OR is an empty list, ingest as a single file.
                        summary = get_file_content_summary(target_container_filepath, file_type, original_filename)
                        data_id = insert_data_object(
                            name=original_filename, file_type=file_type, content_location=target_container_filepath,
                            content_summary=summary, tags=["json_object", "unclassified"]
                        )
                        if data_id:
                            os.remove(source_filepath)
                            print(f"Successfully ingested single JSON file '{original_filename}' and removed from input.")
                        else:
                            print(f"Failed to record single JSON file '{original_filename}'. Removing copied file.")
                            if os.path.exists(target_container_filepath): os.remove(target_container_filepath)
                
                except json.JSONDecodeError: # Invalid JSON
                    summary = get_file_content_summary(target_container_filepath, 'application/octet-stream', f"{original_filename} (invalid JSON)")
                    data_id = insert_data_object(
                        name=original_filename, file_type='application/octet-stream', # Mark as generic binary
                        content_location=target_container_filepath, content_summary=summary, tags=["invalid_json", "unclassified"]
                    )
                    if data_id:
                        os.remove(source_filepath)
                        print(f"Ingested '{original_filename}' (as invalid JSON) and removed from input.")
                    else:
                        print(f"Failed to record invalid JSON '{original_filename}'. Removing copied file.")
                        if os.path.exists(target_container_filepath): os.remove(target_container_filepath)
                except Exception as e_json_proc:
                    print(f"Error during JSON-specific processing for '{original_filename}': {e_json_proc}")
                    if os.path.exists(target_container_filepath): os.remove(target_container_filepath) # Clean up copy

            else: # Not a JSON file, ingest as a single file
                summary = get_file_content_summary(target_container_filepath, file_type, original_filename)
                data_id = insert_data_object(
                    name=original_filename, file_type=file_type, content_location=target_container_filepath,
                    content_summary=summary, tags=["unclassified"]
                )
                if data_id:
                    os.remove(source_filepath)
                    print(f"Successfully ingested '{original_filename}' and removed from input.")
                else:
                    print(f"Failed to record '{original_filename}'. Removing copied file.")
                    if os.path.exists(target_container_filepath): os.remove(target_container_filepath)

        except Exception as e:
            print(f"FATAL error processing file '{original_filename}': {e}")
            import traceback; traceback.print_exc()
            # Ensure copied file is removed if processing fails before DB insert attempt for it
            if os.path.exists(target_container_filepath) and not get_data_object_by_id(target_container_filepath): # Crude check if it was recorded
                 # This check isn't perfect; if insert_data_object fails, target_container_filepath might remain.
                 # Better: only remove source_filepath on full success. If shutil.copy fails, target_container_filepath won't exist.
                 # If DB ops fail, target_container_filepath might be orphaned. Add cleanup.
                 pass


def run_ingestor_service(interval_seconds: int = 10):
    """Continuously runs the data ingestion process."""
    print(f"Starting data ingestor service. Monitoring '{INPUT_MONITOR_DIR}' every {interval_seconds} seconds.")
    # init_db() might be called by other main services, but good for standalone
    init_db() 
    while True:
        ingest_new_data(INPUT_MONITOR_DIR, FILE_STORAGE_DIR)
        time.sleep(interval_seconds)

if __name__ == '__main__':
    print("Running data_ingestor.py standalone for testing...")
    init_db() 

    if not os.path.exists(INPUT_MONITOR_DIR):
        os.makedirs(INPUT_MONITOR_DIR)
        print(f"Created input directory: {INPUT_MONITOR_DIR}")

    # Create sample files
    sample_files_info = {
        "test_list.json": [{"id": 1, "val": "apple"}, {"id": 2, "val": "banana"}],
        "test_object.json": {"product_code": "X100", "price": 99.99},
        "empty_list.json": [],
        "sample_notes.txt": "This is a text file with some notes for testing."
    }
    for fname, content in sample_files_info.items():
        fpath = os.path.join(INPUT_MONITOR_DIR, fname)
        with open(fpath, 'w', encoding='utf-8') as f:
            if fname.endswith(".json"): json.dump(content, f, indent=2)
            else: f.write(content)
        print(f"Created sample file: {fpath}")
    
    print(f"Starting ingestor for a short test period (approx 15s)...")
    
    end_time = time.time() + 15 
    while time.time() < end_time:
        ingest_new_data(INPUT_MONITOR_DIR, FILE_STORAGE_DIR)
        time.sleep(3)
        if not os.listdir(INPUT_MONITOR_DIR): # Stop if input is empty
            print("Input directory is empty. Ingestion likely completed for samples.")
            break
    
    print("Data ingestor standalone test run finished.")