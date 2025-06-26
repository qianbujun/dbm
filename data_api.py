from flask import Flask, jsonify, request, send_file, render_template
from db_manager import (get_data_objects, get_data_object_by_id, init_db, 
                        FILE_STORAGE_DIR, insert_data_object, 
                        update_data_object, delete_data_object, get_data_objects_count)
import os
import uuid

app = Flask(__name__)

def is_valid_uuid(uuid_to_test_str: str) -> bool:
    """Checks if a string is a valid UUID."""
    try:
        return str(uuid.UUID(uuid_to_test_str)) == uuid_to_test_str.lower()
    except ValueError:
        return False

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api')
def api_info():
    return jsonify({
        "message": "Welcome to the Data Object API!",
        "version": "1.3.0", # Incremented version
        "endpoints": {
            "GET /api/data": "List objects",
            "GET /api/data/<id>": "Get single object",
            "GET /api/data/<id>/download": "Download file",
            "POST /api/data": "Create new object",
            "PUT /api/data/<id>": "Update object",
            "DELETE /api/data/<id>": "Delete object"
        }
    })

@app.route('/api/data', methods=['GET'])
def list_data_objects_api():
    """Lists data objects, with optional filtering and pagination."""
    status_filter = request.args.get('status')
    type_filter = request.args.get('type')
    
    try:
        limit = int(request.args.get('limit', 30))
        offset = int(request.args.get('offset', 0))
        if not (0 < limit <= 100): limit = 30
        if offset < 0: offset = 0
    except ValueError:
        return jsonify({"error": "Invalid 'limit' or 'offset' parameter. Must be integers."}), 400

    # Fetch paginated data and total count
    data_list = get_data_objects(status=status_filter, file_type=type_filter, limit=limit, offset=offset)
    total_records = get_data_objects_count(status=status_filter, file_type=type_filter)

    safe_data_list = []
    for obj in data_list:
        safe_obj = dict(obj)
        if 'content_location' in safe_obj:
            del safe_obj['content_location']
        safe_data_list.append(safe_obj)

    return jsonify({
        "total_records": total_records,
        "count_in_response": len(safe_data_list),
        "limit_used": limit,
        "offset_used": offset,
        "data": safe_data_list
    })

@app.route('/api/data/<string:object_id_str>', methods=['GET'])
def get_single_data_object_api(object_id_str: str):
    """Retrieves a single data object by its UUID."""
    if not is_valid_uuid(object_id_str):
        return jsonify({"error": "Invalid object ID format. Must be a valid UUID string."}), 400
    
    data_obj = get_data_object_by_id(object_id_str)
    if data_obj:
        response_obj = dict(data_obj)
        if 'content_location' in response_obj:
            del response_obj['content_location']
        return jsonify(response_obj)
    return jsonify({"error": "Data object not found"}), 404

@app.route('/api/data/<string:object_id_str>/download', methods=['GET'])
def download_data_object_content_api(object_id_str: str):
    """Provides download of the raw file content for a data object."""
    if not is_valid_uuid(object_id_str):
        return jsonify({"error": "Invalid object ID format. Must be a valid UUID string."}), 400
        
    data_obj = get_data_object_by_id(object_id_str)
    if not data_obj:
        return jsonify({"error": "Data object not found"}), 404

    file_path_from_db = data_obj.get('content_location')
    if not file_path_from_db:
        return jsonify({"error": "File location not recorded for this object"}), 500

    safe_base_dir = os.path.abspath(FILE_STORAGE_DIR)
    requested_file_abs = os.path.abspath(file_path_from_db)

    if not requested_file_abs.startswith(safe_base_dir):
        app.logger.warning(f"Path traversal attempt: '{requested_file_abs}'")
        return jsonify({"error": "Access denied to file location."}), 403
            
    if os.path.exists(requested_file_abs) and os.path.isfile(requested_file_abs):
        try:
            return send_file(
                requested_file_abs,
                mimetype=data_obj.get('type', 'application/octet-stream'),
                as_attachment=True,
                download_name=data_obj.get('name', os.path.basename(requested_file_abs))
            )
        except Exception as e:
            app.logger.error(f"Error sending file '{requested_file_abs}': {e}")
            return jsonify({"error": "Server error while trying to send file."}), 500
    else:
        app.logger.error(f"File not found for object ID '{object_id_str}': Path '{requested_file_abs}'")
        return jsonify({"error": "File content not found on server storage."}), 404

@app.route('/api/data', methods=['POST'])
def create_data_object():
    """Creates a new data object."""
    data = request.form
    file = request.files.get('file')
    
    # Basic validation
    if not data.get('name') or not data.get('type') or not data.get('status'):
         return jsonify({"error": "Missing required fields: name, type, status"}), 400
    if not file:
        return jsonify({"error": "File part is required"}), 400
    
    filename = f"{uuid.uuid4().hex}_{file.filename}"
    file_path = os.path.join(FILE_STORAGE_DIR, filename)
    
    try:
        file.save(file_path)
        tags = [tag.strip() for tag in data.get('tags', '').split(',') if tag.strip()]
        
        obj_id = insert_data_object(
            name=data.get('name'),
            file_type=data.get('type'),
            content_location=file_path,
            content_summary=data.get('content', ''),
            tags=tags,
            quality_score=float(data.get('quality_score')) if data.get('quality_score') else None,
            status=data.get('status')
        )
        
        if not obj_id:
            os.remove(file_path) # Clean up saved file if DB insert fails
            return jsonify({"error": "Failed to create database record"}), 500
        
        new_obj = get_data_object_by_id(obj_id)
        if 'content_location' in new_obj:
            del new_obj['content_location']
        return jsonify(new_obj), 201
        
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        app.logger.error(f"Error creating data object: {e}")
        return jsonify({"error": "An internal error occurred."}), 500

@app.route('/api/data/<string:object_id_str>', methods=['PUT'])
def update_data_object_api(object_id_str: str):
    """Updates a data object."""
    if not is_valid_uuid(object_id_str):
        return jsonify({"error": "Invalid object ID format."}), 400
        
    data = request.form
    file = request.files.get('file')
    
    updates = {}
    if 'name' in data: updates['name'] = data['name']
    if 'type' in data: updates['type'] = data['type']
    if 'content' in data: updates['content'] = data['content']
    if 'tags' in data: updates['tags'] = [tag.strip() for tag in data['tags'].split(',') if tag.strip()]
    if 'quality_score' in data: 
        try:
            updates['quality_score'] = float(data['quality_score']) if data['quality_score'] else None
        except ValueError:
            return jsonify({"error": "Invalid quality_score format."}), 400
    if 'status' in data: updates['status'] = data['status']
    
    if file:
        existing_obj = get_data_object_by_id(object_id_str)
        if existing_obj and existing_obj.get('content_location'):
            if os.path.exists(existing_obj['content_location']):
                os.remove(existing_obj['content_location'])
        
        filename = f"{uuid.uuid4().hex}_{file.filename}"
        file_path = os.path.join(FILE_STORAGE_DIR, filename)
        file.save(file_path)
        updates['content_location'] = file_path
    
    if not updates:
        return jsonify({"error": "No update data provided"}), 400

    if update_data_object(object_id_str, **updates):
        updated_obj = get_data_object_by_id(object_id_str)
        if 'content_location' in updated_obj:
            del updated_obj['content_location']
        return jsonify(updated_obj)
    
    return jsonify({"error": "Object not found or update failed"}), 404

@app.route('/api/data/<string:object_id_str>', methods=['DELETE'])
def delete_data_object_api(object_id_str: str):
    """Deletes a data object and its associated file."""
    if not is_valid_uuid(object_id_str):
        return jsonify({"error": "Invalid object ID format."}), 400
        
    obj = get_data_object_by_id(object_id_str)
    if not obj:
        return jsonify({"error": "Object not found"}), 404
        
    if obj.get('content_location') and os.path.exists(obj['content_location']):
        os.remove(obj['content_location'])
    
    if delete_data_object(object_id_str):
        return jsonify({"message": "Object deleted successfully"})
    
    return jsonify({"error": "Deletion failed"}), 500

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)