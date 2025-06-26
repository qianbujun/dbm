# db_manager.py
import sqlite3
import json
from datetime import datetime
import os
import uuid # Added for UUIDs

DATABASE_FILE = 'data.db'
FILE_STORAGE_DIR = 'data_files'

def init_db():
    """
    Initializes the database and file storage directory.
    Creates them if they don't exist.
    Applies schema changes for UUIDs and JSON item tracking.
    """
    if not os.path.exists(FILE_STORAGE_DIR):
        os.makedirs(FILE_STORAGE_DIR)
        print(f"Created file storage directory: {FILE_STORAGE_DIR}")

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # Basic check for old schema (integer ID) - for dev, recommend deleting old DB
    # A full migration script would be needed for production data.
    try:
        cursor.execute("PRAGMA table_info(data_objects);")
        columns_info = {row[1]: row[2] for row in cursor.fetchall()}
        if columns_info and columns_info.get('id') == 'INTEGER':
            print("WARNING: Old database schema (integer ID) detected. "
                  "This script expects a UUID-based ID. "
                  "For development, please delete the old data.db file. "
                  "For production, a proper migration is required.")
    except sqlite3.OperationalError: # Table might not exist yet
        pass

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_objects (
            id TEXT PRIMARY KEY,                       -- Changed to TEXT for UUID
            name TEXT NOT NULL,
            type TEXT NOT NULL,                        -- e.g., 'image/jpeg', 'text/plain', 'application/json_item'
            tags TEXT,                                 -- Stored as JSON string: '["tag1", "tag2"]'
            content_location TEXT NOT NULL,            -- Local file path: 'data_files/image.jpg'
            content TEXT,                              -- Stores the file's enhanced summary/thumbnail
            quality_score REAL DEFAULT 0.0,            -- Float between 0.0 and 1.0
            status TEXT DEFAULT 'new',                 -- 'new', 'processing', 'classified', 'error'
            created_at TEXT NOT NULL,                  -- ISO 8601 format: 'YYYY-MM-DDTHH:MM:SSZ'
            last_updated TEXT NOT NULL,                -- ISO 8601 format
            source_original_id TEXT,                   -- For JSON items: UUID of the original container file's record
            source_item_key TEXT,                      -- For JSON items: index or key within the original container
            FOREIGN KEY (source_original_id) REFERENCES data_objects(id) ON DELETE CASCADE
        );
    ''')
    # Add indexes for commonly queried columns
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_objects_status ON data_objects (status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_objects_type ON data_objects (type);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_objects_created_at ON data_objects (created_at);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_objects_source_original_id ON data_objects (source_original_id);")

    conn.commit()
    conn.close()
    print(f"Initialized database: {DATABASE_FILE}")

def get_db_connection():
    """Gets a database connection with row_factory set to sqlite3.Row."""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def insert_data_object(name: str, file_type: str, content_location: str,
                       content_summary: str = None, tags: list = None,
                       quality_score: float = 0.0, status: str = "new",
                       source_original_id: str = None, source_item_key: str = None) -> str | None:
    """
    Inserts a new data object record into the database.
    Returns the new object's UUID on success, None on failure.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    current_time = datetime.now().isoformat(timespec='seconds') + 'Z'
    new_id = str(uuid.uuid4()) # Generate UUID

    tags_json = json.dumps(list(set(tags))) if tags else json.dumps(["unclassified"])

    try:
        cursor.execute(
            """INSERT INTO data_objects
               (id, name, type, tags, content_location, content, quality_score, status,
                created_at, last_updated, source_original_id, source_item_key)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (new_id, name, file_type, tags_json, content_location, content_summary,
             quality_score, status, current_time, current_time,
             source_original_id, source_item_key)
        )
        conn.commit()
        print(f"Inserted data object: {name} (ID: {new_id})")
        return new_id
    except sqlite3.Error as e:
        print(f"Database error during insertion for '{name}': {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

def get_data_objects(status: str = None, file_type: str = None, limit: int = 100, offset: int = 0) -> list:
    """
    Retrieves data objects, with optional filtering by status and type, and pagination.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM data_objects"
    params = []
    conditions = []

    if status:
        conditions.append("status = ?")
        params.append(status)
    if file_type:
        conditions.append("type = ?")
        params.append(file_type)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += f" ORDER BY created_at ASC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    results = []
    for row in rows:
        obj = dict(row)
        if obj.get('tags'):
            try:
                obj['tags'] = json.loads(obj['tags'])
            except json.JSONDecodeError:
                obj['tags'] = ["tag_error"] # Fallback for corrupted tags
        results.append(obj)
    return results

def get_data_objects_count(status: str = None, file_type: str = None) -> int:
    """
    Retrieves the total count of data objects, with optional filtering.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT COUNT(*) FROM data_objects"
    params = []
    conditions = []

    if status:
        conditions.append("status = ?")
        params.append(status)
    if file_type:
        conditions.append("type = ?")
        params.append(file_type)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cursor.execute(query, params)
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_data_object_by_id(object_id: str) -> dict | None:
    """Retrieves a single data object by its UUID."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM data_objects WHERE id = ?", (object_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        obj = dict(row)
        if obj.get('tags'):
            try:
                obj['tags'] = json.loads(obj['tags'])
            except json.JSONDecodeError:
                 obj['tags'] = ["tag_error"]
        return obj
    return None

def update_data_object(object_id: str, **kwargs) -> bool:
    """
    Updates fields of a data object identified by its UUID.
    Returns True on success, False on failure or if no rows were updated.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    updates = []
    params = []

    kwargs['last_updated'] = datetime.now().isoformat(timespec='seconds') + 'Z'

    for key, value in kwargs.items():
        if key == 'id': continue # ID is immutable
        if key == 'tags' and value is not None:
            updates.append(f"{key} = ?")
            params.append(json.dumps(list(set(value)))) # Ensure unique tags
        else:
            updates.append(f"{key} = ?")
            params.append(value)

    if not updates:
        print(f"No valid fields to update for object ID: {object_id}")
        conn.close()
        return False

    set_clause = ", ".join(updates)
    params.append(object_id)

    try:
        cursor.execute(f"UPDATE data_objects SET {set_clause} WHERE id = ?", params)
        conn.commit()
        if cursor.rowcount == 0:
            print(f"Warning: No rows updated for object ID: {object_id} (it might not exist or values are the same).")
            return False # Or True if "no change needed" is success
        # print(f"Updated data object ID: {object_id}") # Reduce verbosity
        return True
    except sqlite3.Error as e:
        print(f"Database error during update for ID '{object_id}': {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def delete_data_object(object_id: str) -> bool:
    """
    Deletes a data object by its UUID.
    Returns True on success, False on failure.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM data_objects WHERE id = ?", (object_id,))
        conn.commit()
        if cursor.rowcount == 0:
            print(f"Warning: No record deleted for object ID: {object_id} (it might not exist).")
            return False
        print(f"Deleted data object ID: {object_id}")
        return True
    except sqlite3.Error as e:
        print(f"Database error during deletion for ID '{object_id}': {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

if __name__ == '__main__':
    # This block is for demonstrating and testing the db_manager functions.
    # It creates temporary files for this purpose. The main processor does NOT create files.
    print("Running db_manager.py example usage...")
    if os.path.exists(DATABASE_FILE):
        print(f"Removing existing database: {DATABASE_FILE} for test.")
        os.remove(DATABASE_FILE)
    
    init_db()

    # Create a dummy test file
    if not os.path.exists(FILE_STORAGE_DIR):
        os.makedirs(FILE_STORAGE_DIR)
    
    test_doc_path_1 = os.path.join(FILE_STORAGE_DIR, f"example_doc_{uuid.uuid4().hex[:6]}.txt")
    with open(test_doc_path_1, 'w', encoding='utf-8') as f:
        f.write("This is a test document for db_manager.")
    
    summary1 = f"Text document: {os.path.basename(test_doc_path_1)}, Size: {os.path.getsize(test_doc_path_1)} bytes. Content: This is a test..."
    
    obj1_id = insert_data_object(
        name=os.path.basename(test_doc_path_1),
        file_type="text/plain",
        content_location=test_doc_path_1,
        content_summary=summary1,
        tags=["example", "text"],
        status="new"
    )
    if obj1_id:
        print(f"Inserted obj1 with ID: {obj1_id}")
        retrieved_obj1 = get_data_object_by_id(obj1_id)
        print(f"Retrieved obj1: {retrieved_obj1['name'] if retrieved_obj1 else 'Not Found'}")
        update_data_object(obj1_id, status="classified", tags=["example", "text", "processed_test"])
        retrieved_obj1_updated = get_data_object_by_id(obj1_id)
        print(f"Updated obj1 tags: {retrieved_obj1_updated['tags'] if retrieved_obj1_updated else 'Not Found'}")

    # Example of inserting a JSON container and its items
    original_json_filename = f"mylist_{uuid.uuid4().hex[:6]}.json"
    original_json_content = [{"id":1, "data":"itemAlpha"}, {"id":2, "data":"itemBeta"}]
    
    container_timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S%f')
    container_stored_filename = f"{os.path.splitext(original_json_filename)[0]}_{container_timestamp_str}.json"
    container_json_path = os.path.join(FILE_STORAGE_DIR, container_stored_filename)
    with open(container_json_path, 'w', encoding='utf-8') as f:
        json.dump(original_json_content, f)

    container_summary = f"JSON List Container: {original_json_filename}, Items: {len(original_json_content)}, Size: {os.path.getsize(container_json_path)} bytes."
    container_id = insert_data_object(
        name=original_json_filename,
        file_type="application/json_container",
        content_location=container_json_path,
        content_summary=container_summary,
        tags=["json_list_container", "raw_data_test"]
    )

    item_paths_created = []
    if container_id:
        print(f"Inserted JSON container '{original_json_filename}' with ID: {container_id}")
        for i, item_data in enumerate(original_json_content):
            item_filename_base = f"{os.path.splitext(original_json_filename)[0]}_item_{i}"
            item_stored_filename = f"{item_filename_base}_{container_timestamp_str}.json" # Use same timestamp for related items
            item_json_path = os.path.join(FILE_STORAGE_DIR, item_stored_filename)
            item_paths_created.append(item_json_path)
            
            with open(item_json_path, 'w', encoding='utf-8') as f:
                json.dump(item_data, f, indent=2)

            item_summary = f"JSON Item: {item_filename_base}.json (from {original_json_filename}), Size: {os.path.getsize(item_json_path)} bytes. Data: {json.dumps(item_data)[:100]}..."
            item_id = insert_data_object(
                name=f"{item_filename_base}.json",
                file_type="application/json_item",
                content_location=item_json_path,
                content_summary=item_summary,
                tags=["json_item_test"],
                source_original_id=container_id,
                source_item_key=str(i)
            )
            if item_id:
                print(f"  Inserted JSON item {i} ('{item_filename_base}.json') with ID: {item_id}")
    
    all_new_objects = get_data_objects(status="new", limit=5)
    print(f"Found {len(all_new_objects)} 'new' objects. First few: {[o['name'] for o in all_new_objects]}")
    
    # Clean up test files
    if os.path.exists(test_doc_path_1): os.remove(test_doc_path_1)
    if os.path.exists(container_json_path): os.remove(container_json_path)
    for p in item_paths_created:
        if os.path.exists(p): os.remove(p)
    
    print("db_manager.py example run finished.")