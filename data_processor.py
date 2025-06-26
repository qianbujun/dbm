# data_processor.py
import time
import base64
from db_manager import get_data_objects, update_data_object, FILE_STORAGE_DIR, init_db, get_data_object_by_id # Already imported above
import mimetypes
from PIL import Image # pip install Pillow
import re
import random # random.sample might not be needed with pre-split items
from openai import OpenAI
import os
from datetime import datetime

# --- Configuration ---
DASHSCOPE_API_KEY_ENV = "DASHSCOPE_API_KEY"
# IMPORTANT: Replace with your actual key if not using env var, or ensure env var is set.
DASHSCOPE_API_KEY_FALLBACK = os.getenv(DASHSCOPE_API_KEY_ENV,'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx') 

client = OpenAI(
    api_key=DASHSCOPE_API_KEY_FALLBACK, # Uses fallback if env var is not set
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

# Model names (ensure these are valid for DashScope's OpenAI-compatible mode)
QWEN_TEXT_MODEL = "qwen3-32b" 
QWEN_VL_MODEL = "qwen2.5-vl-32b-instruct"

# Quality scoring weights
LLM_QUALITY_WEIGHT = 0.65 # Give slightly more to LLM for nuanced JSON/text
SIMPLE_QUALITY_WEIGHT = 0.35
assert abs(LLM_QUALITY_WEIGHT + SIMPLE_QUALITY_WEIGHT - 1.0) < 1e-9

# Max content length for LLM processing (chars, approximate)
MAX_LLM_TEXT_INPUT_CHARS = 12000 # Characters for text processing
MAX_LLM_JSON_INPUT_CHARS = 12000 # Characters for JSON string representation

# File types from ingestor
JSON_CONTAINER_TYPE = 'application/json_container'
JSON_ITEM_TYPE = 'application/json_item'


# --- Helper Functions ---
def get_image_base64_uri(image_path: str) -> str | None:
    try:
        with open(image_path, "rb") as f:
            encoded_string = base64.b64encode(f.read()).decode('utf-8')
        mime_type, _ = mimetypes.guess_type(image_path)
        return f"data:{mime_type or 'image/jpeg'};base64,{encoded_string}"
    except Exception as e:
        print(f"Error encoding image {image_path} to base64: {e}")
        return None

def extract_tags_from_filename(filename: str) -> list:
    tags = []
    if not filename: return tags
    name_part, ext_part = os.path.splitext(filename)
    if ext_part: tags.append(ext_part.lstrip('.').lower())
    
    cleaned_name = re.sub(r'[-_.\s]+', ' ', name_part).strip()
    words = [word.lower() for word in cleaned_name.split(' ') if word.strip()]
    
    meaningful_words = []
    for word in words:
        if word.isdigit():
            if len(word) == 4 and 1900 <= int(word) <= datetime.now().year + 5:
                meaningful_words.append(word)
        elif len(word) > 2:
            meaningful_words.append(word)
    tags.extend(meaningful_words)
    return list(set(tags))

def safe_truncate(text: str, max_length: int) -> str:
    return text[:max_length-3] + "..." if len(text) > max_length else text

# --- LLM Interaction Functions ---
def _call_qwen_chat_completion(model: str, messages: list, temperature: float = 0.2, max_tokens: int = 200) -> str | None:
    """Helper to call Qwen chat completion and extract content."""
    try:
        response = client.chat.completions.create(
            model=model, messages=messages, temperature=temperature, max_tokens=max_tokens,
            extra_body={"enable_thinking": False} 
        )
        content = response.choices[0].message.content.strip()
        return content
    except Exception as e:
        print(f"Error calling Qwen model {model} (messages: {str(messages)[:200]}...): {e}")
        return None

def qwen_classify_text(text_content: str, filename: str) -> list:
    print(f"LLM classifying text: '{filename}'")
    content_for_llm = safe_truncate(text_content, MAX_LLM_TEXT_INPUT_CHARS)
    prompt = (
        f"Analyze text from file '{filename}'. Extract 5-10 relevant keywords/tags. "
        f"Focus on specific nouns, concepts, topics. Output a comma-separated list of tags ONLY. "
        f"Content: \"{content_for_llm}\""
    )
    tags_str = _call_qwen_chat_completion(QWEN_TEXT_MODEL, [{'role': 'user', 'content': prompt}], 0.1, 100)
    return [t.strip() for t in tags_str.split(',')] if tags_str else ["text_llm_failed"]

def qwen_classify_json(json_data_str: str, filename: str) -> list:
    print(f"LLM classifying JSON: '{filename}'")
    content_for_llm = safe_truncate(json_data_str, MAX_LLM_JSON_INPUT_CHARS)
    prompt = (
        f"Analyze JSON from file '{filename}'. Extract 5-10 tags describing its domain, purpose, or key fields. "
        f"Output a comma-separated list of tags ONLY. JSON: {content_for_llm}"
    )
    tags_str = _call_qwen_chat_completion(QWEN_TEXT_MODEL, [{'role': 'user', 'content': prompt}], 0.1, 100)
    return [t.strip() for t in tags_str.split(',')] if tags_str else ["json_llm_failed"]

def qwen_classify_image(image_path: str, filename: str) -> list: # Simplified return
    print(f"LLM classifying image: '{filename}'")
    image_uri = get_image_base64_uri(image_path)
    if not image_uri: return ["image_base64_error"]
    prompt = (
        f"Analyze image (filename: '{filename}'). Provide 5-10 keywords for content, objects, scene, style. "
        f"Output a comma-separated list of tags ONLY."
    )
    messages = [{'role': 'user', 'content': [{'type': 'image_url', 'image_url': {'url': image_uri}}, {'type': 'text', 'text': prompt}]}]
    tags_str = _call_qwen_chat_completion(QWEN_VL_MODEL, messages, 0.2, 150)
    return [t.strip() for t in tags_str.split(',')] if tags_str else ["image_llm_failed"]

def _qwen_score_content(content_for_llm: str, filename: str, content_type_description: str, model: str) -> float:
    print(f"LLM scoring {content_type_description} quality: '{filename}'")
    prompt = (
        f"Evaluate the quality of the {content_type_description} from file '{filename}'. Consider clarity, completeness, "
        f"coherence, structure (if applicable), and information value. Score 0-100 (100=best). Output ONLY the numeric score. "
        f"{content_type_description.capitalize()}: \"{content_for_llm}\""
    )
    score_str = _call_qwen_chat_completion(model, [{'role': 'user', 'content': prompt}], 0.0, 10)
    if score_str:
        match = re.search(r'\b(\d+)\b', score_str) # Match whole number
        if match: return max(0.0, min(1.0, int(match.group(0)) / 100.0))
    print(f"Could not parse LLM quality score for '{filename}': Response '{score_str}'")
    return 0.5 # Default

def qwen_score_text_quality(text_content: str, filename: str) -> float:
    return _qwen_score_content(safe_truncate(text_content, MAX_LLM_TEXT_INPUT_CHARS), filename, "text content", QWEN_TEXT_MODEL)

def qwen_score_json_quality(json_data_str: str, filename: str) -> float:
    return _qwen_score_content(safe_truncate(json_data_str, MAX_LLM_JSON_INPUT_CHARS), filename, "JSON data", QWEN_TEXT_MODEL)

def qwen_score_image_quality(image_path: str, filename: str) -> float:
    print(f"LLM scoring image quality: '{filename}'")
    image_uri = get_image_base64_uri(image_path)
    if not image_uri: return 0.1
    prompt = (
        f"Evaluate quality of image (filename: '{filename}'). Consider sharpness, lighting, composition, visual info. "
        f"Score 0-100 (100=best). Output ONLY numeric score."
    )
    messages = [{'role': 'user', 'content': [{'type': 'image_url', 'image_url': {'url': image_uri}}, {'type': 'text', 'text': prompt}]}]
    score_str = _call_qwen_chat_completion(QWEN_VL_MODEL, messages, 0.0, 10)
    if score_str:
        match = re.search(r'\b(\d+)\b', score_str)
        if match: return max(0.0, min(1.0, int(match.group(0)) / 100.0))
    print(f"Could not parse LLM image quality score for '{filename}': Response '{score_str}'")
    return 0.5

# --- Simple Heuristic Scoring Functions ---
def simple_score_text_quality(text_content: str, file_size_bytes: int) -> float:
    score, text_len = 0.3, len(text_content)
    if text_len == 0: return 0.05
    if text_len > 2000: score += 0.4
    elif text_len > 100: score += 0.15
    if file_size_bytes > 10240: score += 0.1
    if '\ufffd' in text_content: score -= 0.3
    return max(0.05, min(1.0, score))

def simple_score_json_quality(json_data: any, file_size_bytes: int) -> float:
    score = 0.3 # Base for valid JSON
    num_items, num_keys_total, max_depth = 0, 0, 0

    def get_stats(data, depth=0):
        nonlocal num_items, num_keys_total, max_depth
        max_depth = max(max_depth, depth)
        if isinstance(data, dict):
            num_keys_total += len(data)
            if not data: score_modifier = -0.1
            for v in data.values(): get_stats(v, depth + 1)
        elif isinstance(data, list):
            num_items += len(data)
            if not data: score_modifier = -0.1
            for i in data: get_stats(i, depth + 1)
    
    get_stats(json_data)
    score_modifier = 0 # Temp var for adjustments within this scope
    if num_items > 50: score_modifier += 0.3
    elif num_items > 5: score_modifier += 0.15
    if num_keys_total > 30: score_modifier += 0.2
    elif num_keys_total > 5: score_modifier += 0.1
    if max_depth > 4: score_modifier += 0.1
    
    # Tabular data bonus (list of dicts with consistent keys)
    if isinstance(json_data, list) and len(json_data) > 1 and all(isinstance(i, dict) for i in json_data):
        keys_sample = [set(item.keys()) for item in json_data[:5]] # Sample first 5
        if len(keys_sample) > 0 and all(k_set == keys_sample[0] for k_set in keys_sample) and len(keys_sample[0]) > 1:
            score_modifier += 0.25; print("Tabular JSON bonus applied.")

    if file_size_bytes > 51200: score_modifier += 0.1
    elif file_size_bytes < 100 and (num_items <=1 and num_keys_total <=2): score_modifier -=0.15
    
    return max(0.05, min(1.0, score + score_modifier))

def simple_score_image_quality(image_path: str, file_size_bytes: int) -> float:
    try:
        with Image.open(image_path) as img: width, height = img.size
    except: width, height = 0,0
    score, pixels = 0.2, width * height
    if pixels >= 1e6: score += 0.3
    elif pixels >= 2.5e5: score += 0.15
    if file_size_bytes >= 1e6: score += 0.3
    elif file_size_bytes < 1e4: score -= 0.2
    return max(0.05, min(1.0, score))

def simple_score_other_quality(obj_type: str, file_size_bytes: int, filename: str) -> float:
    score = 0.2
    if file_size_bytes > 1e6: score += 0.2
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.pdf', '.docx', '.xlsx', '.zip', '.csv']: score += 0.2
    if obj_type == JSON_CONTAINER_TYPE and file_size_bytes > 1e5: score += 0.25 # Large container
    return max(0.05, min(1.0, score))

# --- Main Processing Logic ---
def process_data_objects():
    """
    Fetches 'new' data objects from the database and processes them.
    This function's role is to analyze existing files. It does NOT create new files.
    If a file corresponding to a database entry is not found on disk, this
    function will mark the entry with an 'error' status and move on to the next item.
    """
    print("Fetching 'new' data objects...")
    data_objects = get_data_objects(status="new", limit=15) # Process in batches

    if not data_objects:
        print("No new data objects to process.")
        return

    for obj in data_objects:
        obj_id, obj_name, obj_type, obj_loc = obj['id'], obj['name'], obj['type'], obj['content_location']
        print(f"\nProcessing ID: {obj_id}, Name: '{obj_name}', Type: {obj_type}")
        update_data_object(obj_id, status="processing")

        tags_final = obj.get('tags', []) # Keep existing tags if any (e.g. from ingestor)
        if "unclassified" in tags_final: tags_final.remove("unclassified")

        try:
            # CRITICAL CHECK: The first step is to ensure the file exists.
            # If os.path.exists returns False, a FileNotFoundError is raised
            # and caught by the specific exception handler below.
            if not os.path.exists(obj_loc):
                raise FileNotFoundError(f"File referenced in DB not found at path: {obj_loc}")

            # If the file exists, proceed with analysis...
            file_size = os.path.getsize(obj_loc)
            tags_filename = extract_tags_from_filename(obj_name)
            tags_llm, score_llm, score_simple = [], 0.5, 0.5

            if obj_type.startswith('text/'):
                with open(obj_loc, 'r', encoding='utf-8', errors='ignore') as f: content = f.read()
                tags_llm = qwen_classify_text(content, obj_name)
                score_llm = qwen_score_text_quality(content, obj_name)
                score_simple = simple_score_text_quality(content, file_size)
            elif obj_type.startswith('image/'):
                tags_llm = qwen_classify_image(obj_loc, obj_name)
                score_llm = qwen_score_image_quality(obj_loc, obj_name)
                score_simple = simple_score_image_quality(obj_loc, file_size)
            elif obj_type == 'application/json' or obj_type == JSON_ITEM_TYPE:
                with open(obj_loc, 'r', encoding='utf-8') as f: json_obj = json.load(f)
                json_str = json.dumps(json_obj, ensure_ascii=False)
                tags_llm = qwen_classify_json(json_str, obj_name)
                score_llm = qwen_score_json_quality(json_str, obj_name)
                score_simple = simple_score_json_quality(json_obj, file_size)
                tags_final.append('json_item' if obj_type == JSON_ITEM_TYPE else 'json')
            elif obj_type == JSON_CONTAINER_TYPE:
                tags_llm = ["json_container", "list_data"] # Generic for containers
                obj_summary = obj.get('content', "")
                # Try to parse item count from summary for a slightly better LLM score context
                match_items = re.search(r'Items: (\d+)', obj_summary, re.IGNORECASE) or \
                              re.search(r'List of (\d+) JSON items', obj_summary, re.IGNORECASE)
                item_count_info = f" (contains {match_items.group(1)} items)" if match_items else ""
                score_llm = qwen_score_text_quality(f"JSON container file: {obj_name}{item_count_info}. Summary: {safe_truncate(obj_summary, 500)}", obj_name)
                score_simple = simple_score_other_quality(obj_type, file_size, obj_name)
                tags_final.append('json_container')
            else: # Other types
                summary_for_llm = obj.get('content', obj_name)
                tags_llm = qwen_classify_text(summary_for_llm, obj_name)
                score_llm = qwen_score_text_quality(summary_for_llm, obj_name)
                score_simple = simple_score_other_quality(obj_type, file_size, obj_name)
                if '/' in obj_type: tags_filename.append(obj_type.split('/')[-1])

            all_tags = list(set(
                [t.lower().strip() for t_list in [tags_final, tags_filename, tags_llm] for t in t_list if t and isinstance(t, str) and t.strip()]
            ))
            updated_tags = sorted(all_tags if all_tags else ["unclassified"])
            
            final_score = round((score_llm * LLM_QUALITY_WEIGHT) + (score_simple * SIMPLE_QUALITY_WEIGHT), 3)

            update_data_object(obj_id, tags=updated_tags, quality_score=final_score, status="classified")
            print(f"  Successfully processed ID: {obj_id}. Tags: {updated_tags}, Score: {final_score:.2f} (L:{score_llm:.2f},S:{score_simple:.2f})")

        except FileNotFoundError as e:
            # This block is executed if the file does not exist.
            # The database record is updated to 'error' status, and no file is created.
            print(f"  ERROR (File Not Found): {e}")
            print(f"  Marking object ID {obj_id} as 'error' in the database and skipping.")
            update_data_object(obj_id, status="error", tags=(tags_final + ["file_not_found_error"]))
            continue # Explicitly move to the next object in the loop
        except json.JSONDecodeError as e:
            print(f"  ERROR (Invalid JSON): {e} for {obj_id} at {obj_loc}.")
            print(f"  Marking object ID {obj_id} as 'error' in the database and skipping.")
            update_data_object(obj_id, status="error", tags=(tags_final + ["json_decode_error"]))
            continue # Explicitly move to the next object
        except Exception as e:
            print(f"  UNEXPECTED ERROR processing {obj_id}: {e}")
            import traceback; traceback.print_exc()
            err_tag = re.sub(r'[^a-zA-Z0-9_]', '', type(e).__name__)[:20]
            print(f"  Marking object ID {obj_id} as 'error' in the database and skipping.")
            update_data_object(obj_id, status="error", tags=(tags_final + ["processing_error", err_tag]))
            continue # Explicitly move to the next object

def run_processor_service(interval_seconds: int = 10):
    """Continuously runs the data processing service."""
    print(f"Starting data processor service. API Key in use: '{str(client.api_key)[:5]}...'. Processing 'new' items every {interval_seconds}s.")
    init_db()
    while True:
        try:
            process_data_objects()
        except Exception as e:
            print(f"CRITICAL ERROR in processor service main loop: {e}")
            import traceback; traceback.print_exc()
            time.sleep(interval_seconds * 3) # Longer sleep on critical outer loop error
        time.sleep(interval_seconds)

# This check ensures the processor service only runs when this script is executed directly.
if __name__ == '__main__':
    # The first part of the __main__ block (lines 202-286) is for testing the database functions.
    # The following lines start the actual data processing service.

    print("\n--- Starting Data Processor Service ---")
    print("Ensure data_ingestor.py has run or there are 'new' items in DB.")
    run_processor_service(interval_seconds=7)