# data_processor.py
import time
import base64
from db_manager import get_data_objects, update_data_object, FILE_STORAGE_DIR, init_db
import mimetypes
from PIL import Image
import re
import json
from openai import OpenAI
import os
from datetime import datetime

# --- 配置 ---
DASHSCOPE_API_KEY_ENV = "DASHSCOPE_API_KEY"
DASHSCOPE_API_KEY_FALLBACK = os.getenv(DASHSCOPE_API_KEY_ENV,'sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx') 

client = OpenAI(
    api_key=DASHSCOPE_API_KEY_FALLBACK,
    base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
)

# 模型名称
QWEN_TEXT_MODEL = "qwen2-72b-instruct"
QWEN_VL_MODEL = "qwen-vl-plus" 

# 新增：来源质量权重
SOURCE_QUALITY_WEIGHTS = {
    "official_reports": 1.2,
    "internal_data": 1.15,
    "manual_upload": 1.0,
    "web_scraped": 0.85,
    "default": 1.0
}

# LLM处理的最大内容长度 (字符)
MAX_LLM_TEXT_INPUT_CHARS = 10000
MAX_LLM_JSON_INPUT_CHARS = 10000

# 从采集器导入的文件类型
JSON_CONTAINER_TYPE = 'application/json_container'
JSON_ITEM_TYPE = 'application/json_item'

# --- 辅助函数 (无变化) ---
def get_image_base64_uri(image_path: str) -> str | None:
    try:
        with open(image_path, "rb") as f:
            encoded_string = base64.b64encode(f.read()).decode('utf-8')
        mime_type, _ = mimetypes.guess_type(image_path)
        return f"data:{mime_type or 'image/jpeg'};base64,{encoded_string}"
    except Exception as e:
        print(f"将图像 {image_path} 编码为 base64 时出错: {e}")
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

# --- LLM 交互函数 (更新了返回值) ---
def _call_qwen_chat_completion(model: str, messages: list, temperature: float = 0.2, max_tokens: int = 200) -> str | None:
    try:
        response = client.chat.completions.create(
            model=model, messages=messages, temperature=temperature, max_tokens=max_tokens,
            extra_body={"enable_thinking": False} 
        )
        content = response.choices[0].message.content.strip()
        return content
    except Exception as e:
        print(f"调用Qwen模型 {model} 时出错 (消息: {str(messages)[:200]}...): {e}")
        return None

# (分类函数保持不变)
def qwen_classify_text(text_content: str, filename: str) -> list:
    print(f"LLM 正在分类文本: '{filename}'")
    content_for_llm = safe_truncate(text_content, MAX_LLM_TEXT_INPUT_CHARS)
    prompt = (
        f"你是一位数据分类专家。请分析来自文件'{filename}'的文本内容。提取8-15个最相关的中英文关键词或标签。"
        f"标签应包括：1.核心主题(如 '宏观经济') 2.具体实体(如 '中国人民银行') 3.内容类型(如 '研究报告', '新闻稿') 4.更广泛的领域(如 '金融', '科技')。"
        f"请仅输出一个逗号分隔的标签列表，不要有其他任何解释。示例输出: 宏观经济,经济分析,央行,中国人民银行,研究报告,金融,macroeconomics,economic analysis,central bank,research report,finance"
        f"内容: \"{content_for_llm}\""
    )
    tags_str = _call_qwen_chat_completion(QWEN_TEXT_MODEL, [{'role': 'user', 'content': prompt}], 0.1, 150)
    return [t.strip().lower() for t in tags_str.split(',')] if tags_str else ["text_llm_failed"]

def qwen_classify_json(json_data_str: str, filename: str) -> list:
    print(f"LLM 正在分类 JSON: '{filename}'")
    content_for_llm = safe_truncate(json_data_str, MAX_LLM_JSON_INPUT_CHARS)
    prompt = (
        f"你是一位数据分类专家。请分析来自文件'{filename}'的JSON数据。提取8-15个最相关的中英文标签来描述其数据领域、用途或关键字段。"
        f"标签应包括：1.数据领域(如 '股票行情', '用户信息') 2.关键数据指标(如 '收盘价', '用户ID') 3.数据结构类型(如 '时间序列', '对象列表') 4.更广泛的领域(如 '金融', '社交')。"
        f"请仅输出一个逗号分隔的标签列表，不要有其他任何解释。示例输出: 用户数据,用户信息,ID,姓名,user data,user profile,list,social"
        f"JSON: {content_for_llm}"
    )
    tags_str = _call_qwen_chat_completion(QWEN_TEXT_MODEL, [{'role': 'user', 'content': prompt}], 0.1, 150)
    return [t.strip().lower() for t in tags_str.split(',')] if tags_str else ["json_llm_failed"]

def qwen_classify_image(image_path: str, filename: str) -> list:
    print(f"LLM 正在分类图像: '{filename}'")
    image_uri = get_image_base64_uri(image_path)
    if not image_uri: return ["image_base64_error"]
    prompt = (
        f"分析此图像 (文件名: '{filename}')。提供8-15个中英文关键词来描述其内容、物体、场景、风格和颜色。"
        f"请仅输出一个逗号分隔的标签列表，不要有其他任何解释。示例输出: 城市夜景,摩天大楼,灯光,蓝色,科技感,cityscape,skyscraper,night view,blue,tech"
    )
    messages = [{'role': 'user', 'content': [{'type': 'image_url', 'image_url': {'url': image_uri}}, {'type': 'text', 'text': prompt}]}]
    tags_str = _call_qwen_chat_completion(QWEN_VL_MODEL, messages, 0.2, 150)
    return [t.strip().lower() for t in tags_str.split(',')] if tags_str else ["image_llm_failed"]

def _qwen_score_content(content_for_llm: str, filename: str, content_type_description: str, model: str) -> tuple[float, bool]:
    """ (已更新)
    评估内容质量。
    返回一个元组 (score, success)，其中 success 是一个布尔值，指示评分是否成功。
    """
    print(f"LLM 正在评分 {content_type_description} 质量: '{filename}'")
    prompt = (
        f"评估文件'{filename}'中{content_type_description}的质量。考虑清晰度、完整性、连贯性、结构（如果适用）和信息价值。"
        f"给出一个0到100的分数（100为最佳）。请仅输出数字分数，不要有其他任何文字。"
        f"{content_type_description.capitalize()}: \"{content_for_llm}\""
    )
    score_str = _call_qwen_chat_completion(model, [{'role': 'user', 'content': prompt}], 0.0, 10)
    if score_str:
        match = re.search(r'\b(\d+)\b', score_str)
        if match:
            score = max(0.0, min(1.0, int(match.group(0)) / 100.0))
            return score, True  # 成功
    print(f"无法解析'{filename}'的LLM质量分数: 响应 '{score_str}'")
    return 0.5, False  # 失败，返回默认分和失败标记

def qwen_score_text_quality(text_content: str, filename: str) -> tuple[float, bool]:
    return _qwen_score_content(safe_truncate(text_content, MAX_LLM_TEXT_INPUT_CHARS), filename, "文本内容", QWEN_TEXT_MODEL)

def qwen_score_json_quality(json_data_str: str, filename: str) -> tuple[float, bool]:
    return _qwen_score_content(safe_truncate(json_data_str, MAX_LLM_JSON_INPUT_CHARS), filename, "JSON数据", QWEN_TEXT_MODEL)

def qwen_score_image_quality(image_path: str, filename: str) -> tuple[float, bool]:
    print(f"LLM 正在评分图像质量: '{filename}'")
    image_uri = get_image_base64_uri(image_path)
    if not image_uri:
        return 0.1, False # 图像编码失败也算评分失败

    prompt = (
        f"评估图像质量 (文件名: '{filename}')。考虑清晰度、光照、构图和视觉信息。给出0-100分（100最佳）。仅输出数字分数。"
    )
    messages = [{'role': 'user', 'content': [{'type': 'image_url', 'image_url': {'url': image_uri}}, {'type': 'text', 'text': prompt}]}]
    score_str = _call_qwen_chat_completion(QWEN_VL_MODEL, messages, 0.0, 10)
    if score_str:
        match = re.search(r'\b(\d+)\b', score_str)
        if match:
            score = max(0.0, min(1.0, int(match.group(0)) / 100.0))
            return score, True
            
    print(f"无法解析'{filename}'的LLM图像质量分数: 响应 '{score_str}'")
    return 0.5, False

# --- 简单启发式评分函数 (无变化) ---
def simple_score_text_quality(text_content: str, file_size_bytes: int) -> float:
    score, text_len = 0.3, len(text_content)
    if text_len == 0: return 0.05
    if text_len > 2000: score += 0.4
    elif text_len > 100: score += 0.15
    if file_size_bytes > 10240: score += 0.1
    if '\ufffd' in text_content: score -= 0.3
    return max(0.05, min(1.0, score))
def simple_score_json_quality(json_data: any, file_size_bytes: int) -> float:
    score = 0.3 # 有效JSON的基础分
    num_items, num_keys_total, max_depth = 0, 0, 0
    def get_stats(data, depth=0):
        nonlocal num_items, num_keys_total, max_depth
        max_depth = max(max_depth, depth)
        if isinstance(data, dict):
            num_keys_total += len(data)
            for v in data.values(): get_stats(v, depth + 1)
        elif isinstance(data, list):
            num_items += len(data)
            for i in data: get_stats(i, depth + 1)
    get_stats(json_data)
    score_modifier = 0
    if num_items > 50: score_modifier += 0.3
    elif num_items > 5: score_modifier += 0.15
    if num_keys_total > 30: score_modifier += 0.2
    elif num_keys_total > 5: score_modifier += 0.1
    if max_depth > 4: score_modifier += 0.1
    if isinstance(json_data, list) and len(json_data) > 1 and all(isinstance(i, dict) for i in json_data):
        keys_sample = [set(item.keys()) for item in json_data[:5]]
        if len(keys_sample) > 0 and all(k_set == keys_sample[0] for k_set in keys_sample) and len(keys_sample[0]) > 1:
            score_modifier += 0.25; print("表格化JSON奖励已应用。")
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
    if obj_type == JSON_CONTAINER_TYPE and file_size_bytes > 1e5: score += 0.25
    return max(0.05, min(1.0, score))

# --- 主要处理逻辑 (已更新评分逻辑) ---
def process_data_objects():
    """
    从数据库获取'new'状态的数据对象并进行处理。
    此函数的作用是分析已存在的文件。它不会创建新文件。
    """
    print("正在获取'new'状态的数据对象...")
    data_objects = get_data_objects(status="new", limit=15)

    if not data_objects:
        print("没有新的数据对象需要处理。")
        return

    for obj in data_objects:
        obj_id, obj_name, obj_type, obj_loc, obj_source = obj['id'], obj['name'], obj['type'], obj['content_location'], obj['source']
        print(f"\n正在处理 ID: {obj_id}, 名称: '{obj_name}', 类型: {obj_type}, 来源: {obj_source}")
        update_data_object(obj_id, status="processing")

        tags_final = obj.get('tags', [])
        if "unclassified" in tags_final: tags_final.remove("unclassified")

        try:
            if not os.path.exists(obj_loc):
                raise FileNotFoundError(f"数据库中引用的文件路径不存在: {obj_loc}")

            file_size = os.path.getsize(obj_loc)
            tags_filename = extract_tags_from_filename(obj_name)
            tags_llm = []
            score_llm, llm_success = 0.5, False
            score_simple = 0.5

            if obj_type.startswith('text/'):
                with open(obj_loc, 'r', encoding='utf-8', errors='ignore') as f: content = f.read()
                tags_llm = qwen_classify_text(content, obj_name)
                score_llm, llm_success = qwen_score_text_quality(content, obj_name)
                score_simple = simple_score_text_quality(content, file_size)
            elif obj_type.startswith('image/'):
                tags_llm = qwen_classify_image(obj_loc, obj_name)
                score_llm, llm_success = qwen_score_image_quality(obj_loc, obj_name)
                score_simple = simple_score_image_quality(obj_loc, file_size)
            elif obj_type == 'application/json' or obj_type == JSON_ITEM_TYPE:
                with open(obj_loc, 'r', encoding='utf-8') as f: json_obj = json.load(f)
                json_str = json.dumps(json_obj, ensure_ascii=False)
                tags_llm = qwen_classify_json(json_str, obj_name)
                score_llm, llm_success = qwen_score_json_quality(json_str, obj_name)
                score_simple = simple_score_json_quality(json_obj, file_size)
                tags_final.append('json_item' if obj_type == JSON_ITEM_TYPE else 'json')
            else: # 其他类型
                summary_for_llm = obj.get('content', obj_name)
                tags_llm = qwen_classify_text(summary_for_llm, obj_name)
                score_llm, llm_success = qwen_score_text_quality(summary_for_llm, obj_name)
                score_simple = simple_score_other_quality(obj_type, file_size, obj_name)
                if obj_type == JSON_CONTAINER_TYPE: tags_final.append('json_container')
                if '/' in obj_type: tags_filename.append(obj_type.split('/')[-1])

            all_tags = list(set(
                [t.lower().strip() for t_list in [tags_final, tags_filename, tags_llm] for t in t_list if t and isinstance(t, str) and t.strip()]
            ))
            updated_tags = sorted(all_tags if all_tags else ["unclassified"])
            
            # --- (已更新) 最终评分的动态加权计算 ---
            if llm_success:
                # 如果LLM评分成功，给予它高权重
                llm_weight = 0.85
                simple_weight = 0.15
            else:
                # 如果LLM评分失败，主要依赖简单启发式评分
                print("  LLM评分失败，主要依赖启发式评分。")
                llm_weight = 0.10
                simple_weight = 0.90
            
            base_score = (score_llm * llm_weight) + (score_simple * simple_weight)
            
            source_weight = SOURCE_QUALITY_WEIGHTS.get(obj_source, SOURCE_QUALITY_WEIGHTS["default"])
            final_score = round(base_score * source_weight, 4)
            final_score = min(1.0, final_score) # 确保分数不超过1.0

            update_data_object(obj_id, tags=updated_tags, quality_score=final_score, status="classified")
            print(f"  成功处理 ID: {obj_id}。标签: {updated_tags}")
            print(f"  --> 最终得分: {final_score:.3f} (LLM: {score_llm:.2f}, Heuristic: {score_simple:.2f}, Weights: L={llm_weight}/H={simple_weight}, Source-Mod: x{source_weight})")

        except FileNotFoundError as e:
            print(f"  错误 (文件未找到): {e}")
            update_data_object(obj_id, status="error", tags=(tags_final + ["file_not_found_error"]))
        except json.JSONDecodeError as e:
            print(f"  错误 (无效JSON): {e} for {obj_id} at {obj_loc}.")
            update_data_object(obj_id, status="error", tags=(tags_final + ["json_decode_error"]))
        except Exception as e:
            print(f"  处理 {obj_id} 时发生意外错误: {e}")
            import traceback; traceback.print_exc()
            err_tag = re.sub(r'[^a-zA-Z0-9_]', '', type(e).__name__)[:20]
            update_data_object(obj_id, status="error", tags=(tags_final + ["processing_error", err_tag]))

def run_processor_service(interval_seconds: int = 10):
    """持续运行数据处理服务。"""
    print(f"启动数据处理服务。使用的API密钥: '{str(client.api_key)[:5]}...'。每 {interval_seconds}s 处理一次'new'状态的项。")
    init_db()
    while True:
        try:
            process_data_objects()
        except Exception as e:
            print(f"处理器服务主循环中发生严重错误: {e}")
            import traceback; traceback.print_exc()
            time.sleep(interval_seconds * 3)
        time.sleep(interval_seconds)

if __name__ == '__main__':
    print("\n--- 启动数据处理器服务 ---")
    print("请确保 data_ingestor.py 已运行或数据库中有'new'状态的项。")
    run_processor_service(interval_seconds=7)