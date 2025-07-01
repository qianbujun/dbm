# data_ingestor.py
import os
import shutil
import mimetypes
from datetime import datetime
import time
import json
import uuid
from db_manager import init_db, insert_data_object, FILE_STORAGE_DIR

# 监控目录
INPUT_MONITOR_DIR = 'input_data'

# 自定义MIME类型映射
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
    '.json': 'application/json',
    '.zip': 'application/zip', '.gz': 'application/gzip', '.tar': 'application/x-tar',
}

# 摘要内容的最大长度
MAX_SUMMARY_TEXT_SNIPPET = 200
MAX_JSON_SNIPPET_LEN = 150

# JSON结构的特定类型
JSON_CONTAINER_TYPE = 'application/json_container'
JSON_ITEM_TYPE = 'application/json_item'

def get_file_type(filepath: str) -> str:
    """使用扩展名映射和mimetypes确定文件MIME类型。"""
    _, ext = os.path.splitext(filepath)
    ext = ext.lower()
    if ext in FILE_TYPE_MAP:
        return FILE_TYPE_MAP[ext]
    mime_type, _ = mimetypes.guess_type(filepath)
    return mime_type if mime_type else 'application/octet-stream'

def generate_stored_filename(original_filename: str, suffix_part: str = None) -> str:
    """生成用于存储的唯一文件名，包含时间戳和可选后缀。"""
    name_without_ext, ext = os.path.splitext(original_filename)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    if suffix_part:
        return f"{name_without_ext}_{suffix_part}_{timestamp}{ext}"
    return f"{name_without_ext}_{timestamp}{ext}"

def get_file_content_summary(filepath: str, file_type: str, original_filename_for_display: str) -> str:
    """为不同文件类型生成增强的内容摘要。"""
    # ... (此函数逻辑保持不变)
    summary_parts = []
    try:
        file_size_bytes = os.path.getsize(filepath)
        file_size_kb = file_size_bytes / 1024.0
        summary_parts.extend([
            f"文件: {original_filename_for_display}",
            f"类型: {file_type}",
            f"大小: {file_size_kb:.2f}KB"
        ])

        if file_type.startswith('text/'):
            try:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    content_sample = f.read(MAX_SUMMARY_TEXT_SNIPPET)
                if len(content_sample) == MAX_SUMMARY_TEXT_SNIPPET and file_size_bytes > MAX_SUMMARY_TEXT_SNIPPET:
                    content_sample += "..."
                summary_parts.append(f"片段: \"{content_sample}\"")
            except Exception as e:
                summary_parts.append(f"片段错误: {e}")

        elif file_type.startswith('image/'):
            try:
                from PIL import Image # Pillow 是一个软依赖
                with Image.open(filepath) as img:
                    width, height = img.size
                    summary_parts.append(f"尺寸: {width}x{height}")
                    if img.format: summary_parts.append(f"格式: {img.format}")
            except ImportError:
                summary_parts.append("图像详情: 未安装Pillow库。")
            except Exception as e:
                summary_parts.append(f"图像详情错误: {e}")
        
        elif file_type in ['application/json', JSON_ITEM_TYPE]:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                data_str = json.dumps(json_data, ensure_ascii=False)
                snippet = data_str[:MAX_JSON_SNIPPET_LEN]
                if len(data_str) > MAX_JSON_SNIPPET_LEN: snippet += "..."
                
                if isinstance(json_data, list):
                    summary_parts.append(f"JSON 数组, 元素数: {len(json_data)}")
                elif isinstance(json_data, dict):
                    summary_parts.append(f"JSON 对象, 键数: {len(json_data.keys())}")
                else:
                    summary_parts.append("JSON 原始类型")
                summary_parts.append(f"数据片段: {snippet}")
            except Exception as e:
                summary_parts.append(f"JSON 详情错误: {e}")
        
        elif file_type == JSON_CONTAINER_TYPE:
             try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    json_data_container = json.load(f)
                if isinstance(json_data_container, list):
                    summary_parts.append(f"包含: {len(json_data_container)}个 JSON 项的列表。")
                elif isinstance(json_data_container, dict):
                     summary_parts.append(f"包含: 具有 {len(json_data_container.keys())} 个顶级条目的JSON对象。")
                else:
                    summary_parts.append("包含: 单个JSON原始类型。")
             except Exception as e:
                summary_parts.append(f"容器详情错误: {e}")
        
        return ". ".join(summary_parts)
    except FileNotFoundError:
        return f"[文件未找到错误: {original_filename_for_display} at {filepath}]"
    except Exception as e:
        return f"[为 {original_filename_for_display} 生成摘要时出错: {e}]"


def ingest_new_data(input_dir: str, target_base_dir: str):
    """
    扫描input_dir的子目录，处理新文件，存储它们，并在数据库中记录。
    子目录的名称将被用作数据的'source'。
    """
    print(f"正在扫描新数据于: {input_dir}")
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
        print(f"已创建输入目录: {input_dir}")
        return

    # 遍历input_dir下的每个子目录（这些是source）
    for source_name in os.listdir(input_dir):
        source_dir_path = os.path.join(input_dir, source_name)
        if not os.path.isdir(source_dir_path):
            continue
        
        print(f"--- 正在处理来源: '{source_name}' ---")
        # 遍历源目录中的每个文件
        for original_filename in os.listdir(source_dir_path):
            source_filepath = os.path.join(source_dir_path, original_filename)
            if not os.path.isfile(source_filepath):
                continue
            
            # --- 从这里开始，逻辑与原来类似，但增加了 `source_name` 参数 ---
            file_type = get_file_type(source_filepath)
            stored_container_filename = generate_stored_filename(original_filename)
            target_container_filepath = os.path.join(target_base_dir, stored_container_filename)

            try:
                shutil.copy(source_filepath, target_container_filepath)

                if file_type == 'application/json':
                    try:
                        with open(target_container_filepath, 'r', encoding='utf-8') as f:
                            json_content = json.load(f)

                        if isinstance(json_content, list) and json_content:
                            container_summary = get_file_content_summary(target_container_filepath, JSON_CONTAINER_TYPE, original_filename)
                            container_id = insert_data_object(
                                name=original_filename,
                                file_type=JSON_CONTAINER_TYPE,
                                source=source_name,  # 传递source
                                content_location=target_container_filepath,
                                content_summary=container_summary,
                                tags=["json_container", "unclassified_list"]
                            )

                            if not container_id:
                                print(f"记录JSON容器 '{original_filename}' 失败。正在删除已复制的容器文件。")
                                if os.path.exists(target_container_filepath): os.remove(target_container_filepath)
                                continue

                            print(f"已采集JSON容器 '{original_filename}' (ID: {container_id})。正在采集其内容项...")
                            items_ingested_count = 0
                            for index, item_data in enumerate(json_content):
                                item_display_name = f"{os.path.splitext(original_filename)[0]}_item_{index}.json"
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
                                        source=source_name, # 为每个子项也传递source
                                        content_location=item_target_filepath,
                                        content_summary=item_summary,
                                        tags=["json_item", "unclassified"],
                                        source_original_id=container_id,
                                        source_item_key=str(index)
                                    )
                                    if item_id: items_ingested_count +=1
                                    else:
                                        print(f"  记录来自'{original_filename}'的JSON项 {index} 失败。正在删除项目文件。")
                                        if os.path.exists(item_target_filepath): os.remove(item_target_filepath)
                                except Exception as item_e:
                                    print(f"  采集来自'{original_filename}'的JSON项 {index} 时出错: {item_e}")
                                    if os.path.exists(item_target_filepath): os.remove(item_target_filepath)
                            
                            print(f"  成功从'{original_filename}'采集了 {items_ingested_count}/{len(json_content)} 个项目。")
                            if items_ingested_count > 0 or container_id:
                                 os.remove(source_filepath)
                                 print(f"成功处理并从输入中移除了'{original_filename}'。")
                        else:
                            summary = get_file_content_summary(target_container_filepath, file_type, original_filename)
                            data_id = insert_data_object(
                                name=original_filename, file_type=file_type, source=source_name,
                                content_location=target_container_filepath, content_summary=summary,
                                tags=["json_object", "unclassified"]
                            )
                            if data_id:
                                os.remove(source_filepath)
                                print(f"成功采集了单个JSON文件 '{original_filename}' 并从输入中移除。")
                            else:
                                print(f"记录单个JSON文件 '{original_filename}' 失败。正在删除已复制的文件。")
                                if os.path.exists(target_container_filepath): os.remove(target_container_filepath)
                    
                    except json.JSONDecodeError:
                        summary = get_file_content_summary(target_container_filepath, 'application/octet-stream', f"{original_filename} (无效JSON)")
                        data_id = insert_data_object(
                            name=original_filename, file_type='application/octet-stream', source=source_name,
                            content_location=target_container_filepath, content_summary=summary, tags=["invalid_json", "unclassified"]
                        )
                        if data_id:
                            os.remove(source_filepath)
                            print(f"已采集 '{original_filename}' (作为无效JSON) 并从输入中移除。")
                        else:
                            print(f"记录无效JSON '{original_filename}' 失败。正在删除已复制的文件。")
                            if os.path.exists(target_container_filepath): os.remove(target_container_filepath)
                    except Exception as e_json_proc:
                        print(f"处理 '{original_filename}' 的JSON时出错: {e_json_proc}")
                        if os.path.exists(target_container_filepath): os.remove(target_container_filepath)
                else:
                    summary = get_file_content_summary(target_container_filepath, file_type, original_filename)
                    data_id = insert_data_object(
                        name=original_filename, file_type=file_type, source=source_name,
                        content_location=target_container_filepath, content_summary=summary, tags=["unclassified"]
                    )
                    if data_id:
                        os.remove(source_filepath)
                        print(f"成功采集了 '{original_filename}' 并从输入中移除。")
                    else:
                        print(f"记录 '{original_filename}' 失败。正在删除已复制的文件。")
                        if os.path.exists(target_container_filepath): os.remove(target_container_filepath)

            except Exception as e:
                print(f"处理文件 '{original_filename}' 时发生致命错误: {e}")
                import traceback; traceback.print_exc()

def run_ingestor_service(interval_seconds: int = 10):
    """持续运行数据采集服务。"""
    print(f"启动数据采集服务。每 {interval_seconds} 秒监控一次 '{INPUT_MONITOR_DIR}'。")
    init_db() 
    while True:
        ingest_new_data(INPUT_MONITOR_DIR, FILE_STORAGE_DIR)
        time.sleep(interval_seconds)

if __name__ == '__main__':
    print("独立运行 data_ingestor.py 进行测试...")
    init_db()

    # 创建示例文件和源目录
    sample_source_dir = os.path.join(INPUT_MONITOR_DIR, "sample_source_A")
    if not os.path.exists(sample_source_dir):
        os.makedirs(sample_source_dir)
        print(f"已创建输入源目录: {sample_source_dir}")

    # sample_files_info = {
    #     "test_list.json": [{"id": 1, "val": "苹果"}, {"id": 2, "val": "香蕉"}],
    #     "sample_notes.txt": "这是一份关于市场分析的笔记。"
    # }
    # for fname, content in sample_files_info.items():
    #     fpath = os.path.join(sample_source_dir, fname)
    #     with open(fpath, 'w', encoding='utf-8') as f:
    #         if fname.endswith(".json"): json.dump(content, f, indent=2, ensure_ascii=False)
    #         else: f.write(content)
    #     print(f"已创建示例文件: {fpath}")
    
    print(f"启动采集器进行短暂测试 (约15秒)...")
    ingest_new_data(INPUT_MONITOR_DIR, FILE_STORAGE_DIR) # 立即运行一次
    print("数据采集器独立测试运行完成。")