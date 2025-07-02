# data_api.py
from flask import Flask, jsonify, request, send_file, render_template
from db_manager import (get_data_objects, get_data_object_by_id, init_db, 
                        FILE_STORAGE_DIR, insert_data_object, 
                        update_data_object, delete_data_object, get_data_objects_count)
from db_manager import get_tag_graph_data 
import os
import uuid

app = Flask(__name__)

def is_valid_uuid(uuid_to_test_str: str) -> bool:
    """检查字符串是否是有效的UUID。"""
    try:
        return str(uuid.UUID(uuid_to_test_str)) == uuid_to_test_str.lower()
    except ValueError:
        return False

@app.route('/graph')
def graph_page():
    """(新增) 渲染关系图谱页面。"""
    return render_template('graph.html')

@app.route('/api/tags/graph')
def get_tag_graph_api():
    """(新增) 提供标签关系图谱的数据API。"""
    try:
        # 从请求参数获取阈值，提供默认值
        min_freq = int(request.args.get('min_freq', 2))
        min_strength = int(request.args.get('min_strength', 1))
        
        graph_data = get_tag_graph_data(min_frequency=min_freq, min_link_strength=min_strength)
        return jsonify(graph_data)
    except Exception as e:
        app.logger.error(f"生成图谱数据时出错: {e}")
        return jsonify({"error": "生成图谱数据时服务器发生内部错误。"}), 500

# 同时，为了方便访问，我们稍微修改一下主页的视图函数
@app.route('/')
def index():
    """(已更新) 主页，渲染UI界面。"""
    # 我们可以传递一个变量来告诉模板显示图谱链接
    return render_template('index.html', show_graph_link=True)

@app.route('/api')
def api_info():
    """API信息端点。"""
    return jsonify({
        "message": "欢迎使用数据对象 API!",
        "version": "1.4.0", # 版本号增加
        "endpoints": {
            # ... (保持不变)
        }
    })


@app.route('/api/data', methods=['GET'])
def list_data_objects_api():
    """(已更新) 列出数据对象，现在能正确地处理所有过滤条件并返回准确的总数。"""
    # 解析请求参数
    status_filter = request.args.get('status')
    type_filter = request.args.get('type')
    name_like_filter = request.args.get('name_like')
    
    tags_filter_str = request.args.get('tags')
    tags_filter_list = [tag.strip() for tag in tags_filter_str.split(',') if tag.strip()] if tags_filter_str else None
    
    try:
        limit = int(request.args.get('limit', 30))
        offset = int(request.args.get('offset', 0))
        if not (0 < limit <= 100): limit = 30
        if offset < 0: offset = 0
    except ValueError:
        return jsonify({"error": "无效的'limit'或'offset'参数。必须是整数。"}), 400

    # (核心修复) 将所有过滤器同时传递给数据获取函数和计数函数
    common_filters = {
        'status': status_filter,
        'file_type': type_filter,
        'tags': tags_filter_list,
        'name_like': name_like_filter
    }

    # 获取分页后的数据列表
    data_list = get_data_objects(**common_filters, limit=limit, offset=offset)
    
    # 获取应用所有过滤器后的总记录数
    total_records = get_data_objects_count(**common_filters)

    # 准备安全的响应数据（移除敏感路径）
    safe_data_list = [dict(obj) for obj in data_list]
    for obj in safe_data_list:
        if 'content_location' in obj:
            del obj['content_location']

    return jsonify({
        "total_records": total_records,
        "count_in_response": len(safe_data_list),
        "limit_used": limit,
        "offset_used": offset,
        "data": safe_data_list
    })

@app.route('/api/data/<string:object_id_str>', methods=['GET'])
def get_single_data_object_api(object_id_str: str):
    """通过其UUID检索单个数据对象。"""
    if not is_valid_uuid(object_id_str):
        return jsonify({"error": "无效的对象ID格式。必须是有效的UUID字符串。"}), 400
    
    data_obj = get_data_object_by_id(object_id_str)
    if data_obj:
        response_obj = dict(data_obj)
        if 'content_location' in response_obj:
            del response_obj['content_location']
        return jsonify(response_obj)
    return jsonify({"error": "未找到数据对象"}), 404

@app.route('/api/data', methods=['POST'])
def create_data_object():
    """创建一个新的数据对象。"""
    data = request.form
    file = request.files.get('file')
    
    if not data.get('name') or not data.get('type') or not data.get('status'):
         return jsonify({"error": "缺少必填字段: name, type, status"}), 400
    if not file:
        return jsonify({"error": "文件部分是必需的"}), 400
    
    filename = f"{uuid.uuid4().hex}_{file.filename}"
    file_path = os.path.join(FILE_STORAGE_DIR, filename)
    
    try:
        file.save(file_path)
        tags = [tag.strip() for tag in data.get('tags', '').split(',') if tag.strip()]
        
        obj_id = insert_data_object(
            name=data.get('name'),
            file_type=data.get('type'),
            source=data.get('source'),  # 新增: 从表单获取source
            content_location=file_path,
            content_summary=data.get('content', ''),
            tags=tags,
            quality_score=float(data.get('quality_score')) if data.get('quality_score') else 0.0,
            status=data.get('status')
        )
        
        if not obj_id:
            os.remove(file_path)
            return jsonify({"error": "创建数据库记录失败"}), 500
        
        new_obj = get_data_object_by_id(obj_id)
        if 'content_location' in new_obj:
            del new_obj['content_location']
        return jsonify(new_obj), 201
        
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        app.logger.error(f"创建数据对象时出错: {e}")
        return jsonify({"error": "发生内部错误。"}), 500

@app.route('/api/data/<string:object_id_str>', methods=['PUT'])
def update_data_object_api(object_id_str: str):
    """更新一个数据对象。"""
    if not is_valid_uuid(object_id_str):
        return jsonify({"error": "无效的对象ID格式。"}), 400
        
    data = request.form
    file = request.files.get('file')
    
    updates = {}
    if 'name' in data: updates['name'] = data['name']
    if 'type' in data: updates['type'] = data['type']
    if 'source' in data: updates['source'] = data['source'] # 新增
    if 'content' in data: updates['content'] = data['content']
    if 'tags' in data: updates['tags'] = [tag.strip() for tag in data['tags'].split(',') if tag.strip()]
    if 'quality_score' in data: 
        try:
            updates['quality_score'] = float(data['quality_score']) if data.get('quality_score') else None
        except ValueError:
            return jsonify({"error": "无效的quality_score格式。"}), 400
    if 'status' in data: updates['status'] = data['status']
    
    if not updates and not file:
        return jsonify({"error": "没有提供更新数据"}), 400

    if update_data_object(object_id_str, **updates):
        updated_obj = get_data_object_by_id(object_id_str)
        if 'content_location' in updated_obj:
            del updated_obj['content_location']
        return jsonify(updated_obj)
    
    return jsonify({"error": "未找到对象或更新失败"}), 404

@app.route('/api/data/<string:object_id_str>', methods=['DELETE'])
def delete_data_object_api(object_id_str: str):
    """(已更新) 删除一个数据对象及其关联文件，增加了健壮的错误处理。"""
    if not is_valid_uuid(object_id_str):
        return jsonify({"error": "无效的对象ID格式。"}), 400
        
    try:
        # 1. 首先，从数据库获取对象信息
        obj = get_data_object_by_id(object_id_str)
        if not obj:
            return jsonify({"error": "未找到对象"}), 404
            
        # 2. 安全地删除关联的物理文件
        file_path = obj.get('content_location')
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                app.logger.info(f"已删除关联文件: {file_path}")
            except OSError as e:
                # 如果文件删除失败，记录错误，但继续尝试删除数据库记录
                app.logger.error(f"删除文件 '{file_path}' 失败，但将继续删除数据库记录: {e}")

        # 3. 删除数据库记录
        if delete_data_object(object_id_str):
            # 成功路径
            return jsonify({"message": "对象已成功删除"})
        else:
            # 如果db_manager返回False，说明在删除时记录已不存在
            return jsonify({"error": "数据库记录删除失败，可能已被其他进程删除"}), 500

    except Exception as e:
        # 捕获任何其他意外异常
        app.logger.error(f"删除对象ID '{object_id_str}' 时发生意外错误: {e}")
        import traceback
        traceback.print_exc() # 在服务器控制台打印详细的堆栈信息
        return jsonify({"error": "服务器内部错误，删除操作失败"}), 500

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)