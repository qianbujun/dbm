# db_manager.py
import sqlite3
import json
from datetime import datetime
import os
import uuid

DATABASE_FILE = 'data.db'
FILE_STORAGE_DIR = 'data_files'



SYSTEM_TAG_STOP_LIST = [
        'item','json', 'json_item', 'json_container', 'unclassified', 'unclassified_list',
        'text', 'image', 'pdf', 'docx', 'xlsx', 'csv', 'txt', 'md', 'jpg', 'jpeg', 'png',
        'gif', 'bmp', 'svg', 'zip', 'gz', 'tar',
        'text_llm_failed', 'json_llm_failed', 'image_llm_failed', 'image_base64_error',
        'processing_error', 'file_not_found_error', 'json_decode_error', 'invalid_json',
        'needs_review','object list','中文文本','chinese text',]

def init_db():
    """
    初始化数据库和文件存储目录。
    如果它们不存在，则创建它们。
    应用新的数据库结构，包括'source'字段和用于高效标签查询的规范化标签表。
    """
    if not os.path.exists(FILE_STORAGE_DIR):
        os.makedirs(FILE_STORAGE_DIR)
        print(f"已创建文件存储目录: {FILE_STORAGE_DIR}")

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # 开启外键约束支持
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 创建主数据对象表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_objects (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            source TEXT,                               -- 新增: 数据来源标签 (例如, 'finance_reports')
            content_location TEXT NOT NULL,
            content TEXT,
            quality_score REAL DEFAULT 0.0,
            status TEXT DEFAULT 'new',
            created_at TEXT NOT NULL,
            last_updated TEXT NOT NULL,
            source_original_id TEXT,
            source_item_key TEXT,
            FOREIGN KEY (source_original_id) REFERENCES data_objects(id) ON DELETE CASCADE
        );
    ''')
    
    # 创建独立的标签表 (标签库)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
    ''')

    # 创建关联表，用于数据对象和标签的多对多关系
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_object_tags (
            data_object_id TEXT NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (data_object_id, tag_id),
            FOREIGN KEY (data_object_id) REFERENCES data_objects(id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
        );
    ''')
    
    # 为常用查询列添加索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_objects_status ON data_objects (status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_objects_type ON data_objects (type);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_data_objects_source ON data_objects (source);") # 新增
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_tags_name ON tags (name);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dot_data_object_id ON data_object_tags (data_object_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dot_tag_id ON data_object_tags (tag_id);")

    conn.commit()
    conn.close()
    print(f"已初始化数据库: {DATABASE_FILE}")

def get_db_connection():
    """获取一个数据库连接，并将row_factory设置为sqlite3.Row。"""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn

def _manage_tags(cursor, object_id, tags_list):
    """私有辅助函数，用于管理标签的插入和关联。"""
    # 1. 删除此对象旧的所有标签关联
    cursor.execute("DELETE FROM data_object_tags WHERE data_object_id = ?", (object_id,))

    if not tags_list:
        return

    # 2. 插入新标签并建立新关联
    tag_ids = []
    for tag_name in set(tags_list): # 使用set确保标签唯一
        # 插入或忽略已存在的标签，然后获取其ID
        cursor.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag_name,))
        cursor.execute("SELECT id FROM tags WHERE name = ?", (tag_name,))
        tag_row = cursor.fetchone()
        if tag_row:
            tag_ids.append(tag_row['id'])

    # 3. 批量插入新的关联关系
    if tag_ids:
        bindings = [(object_id, tag_id) for tag_id in tag_ids]
        cursor.executemany("INSERT INTO data_object_tags (data_object_id, tag_id) VALUES (?, ?)", bindings)


def insert_data_object(name: str, file_type: str, content_location: str,
                       source: str = None, tags: list = None,
                       content_summary: str = None, quality_score: float = 0.0,
                       status: str = "new", source_original_id: str = None,
                       source_item_key: str = None) -> str | None:
    """
    向数据库中插入一个新的数据对象记录。
    成功时返回新对象的UUID，失败时返回None。
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    current_time = datetime.now().isoformat(timespec='seconds') + 'Z'
    new_id = str(uuid.uuid4())
    
    tags_to_insert = tags if tags else ["unclassified"]

    try:
        # 启动事务
        cursor.execute("BEGIN")
        
        # 插入主对象
        cursor.execute(
            """INSERT INTO data_objects
               (id, name, type, source, content_location, content, quality_score, status,
                created_at, last_updated, source_original_id, source_item_key)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (new_id, name, file_type, source, content_location, content_summary,
             quality_score, status, current_time, current_time,
             source_original_id, source_item_key)
        )
        
        # 管理标签
        _manage_tags(cursor, new_id, tags_to_insert)

        conn.commit()
        print(f"已插入数据对象: {name} (ID: {new_id})")
        return new_id
    except sqlite3.Error as e:
        print(f"数据库插入 '{name}' 时出错: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

def get_data_objects(status: str = None, file_type: str = None, 
                     tags: list = None, name_like: str = None, 
                     limit: int = 100, offset: int = 0) -> list:
    """
    (已重构) 检索数据对象，使用更健壮的子查询来正确处理多标签的AND逻辑。
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 基础查询
    query = """
        SELECT do.*, (SELECT GROUP_CONCAT(t.name) FROM tags t 
                      JOIN data_object_tags dot ON t.id = dot.tag_id 
                      WHERE dot.data_object_id = do.id) as tags
        FROM data_objects do
    """
    params = []
    conditions = []

    if status:
        conditions.append("do.status = ?")
        params.append(status)
    if file_type:
        conditions.append("do.type = ?")
        params.append(file_type)
    if name_like:
        conditions.append("do.name LIKE ?")
        params.append(f"%{name_like}%")

    # (核心修复) 使用子查询和HAVING COUNT来强制执行AND逻辑
    # 在 db_manager.py 中替换原有的 if tags ... 块
    if tags and isinstance(tags, list) and len(tags) > 0:
        # 核心修复：为每个 LIKE 条件创建一个 CASE 语句
        # e.g., "CASE WHEN t.name LIKE ? THEN 1 ELSE 0 END"
        # 我们用不同的整数来代表不同的匹配条件
        case_statements = []
        for i, tag in enumerate(tags):
            case_statements.append(f"CASE WHEN t.name LIKE ? THEN {i+1} ELSE NULL END")
        
        # 将所有 CASE 语句合并，用于后续的 COUNT(DISTINCT ...)
        # e.g., "COUNT(DISTINCT CASE WHEN ... END)"
        count_expression = f"COUNT(DISTINCT ({' + '.join(case_statements).replace('+','').replace('CASE','COALESCE(CASE') + ')'*len(case_statements)}))"

        # 更简单、更健壮的 SQLite 写法是使用 DISTINCT 对一个组合表达式求值
        # 我们要统计匹配上了多少个输入模式（pattern）
        # e.g., COUNT(DISTINCT CASE WHEN t.name LIKE '%航天%' THEN 'pattern1' WHEN t.name LIKE '%政策%' THEN 'pattern2' END)
        
        when_clauses = []
        for i, _ in enumerate(tags):
            # 为每个输入标签创建一个唯一的标识符 'pattern_i'
            when_clauses.append(f"WHEN t.name LIKE ? THEN 'pattern_{i}'")
        
        count_logic = f"COUNT(DISTINCT (CASE {' '.join(when_clauses)} END))"
        
        subquery = f"""
            do.id IN (
                SELECT dot.data_object_id
                FROM data_object_tags dot
                JOIN tags t ON dot.tag_id = t.id
                GROUP BY dot.data_object_id
                HAVING {count_logic} = ?  -- <== 使用新的 COUNT 逻辑
            )
        """
        conditions.append(subquery)
        
        # 准备参数
        # 首先是 WHEN 子句中的 LIKE 参数
        for tag in tags:
            params.append(f"%{tag}%")
        # 然后是 HAVING 子句中的 COUNT 数量
        params.append(len(tags))


    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY do.created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    results = []
    for row in rows:
        obj = dict(row)
        if obj.get('tags'):
            obj['tags'] = obj['tags'].split(',')
        else:
            obj['tags'] = []
        results.append(obj)
    return results

def get_data_objects_count(status: str = None, file_type: str = None,
                           tags: list = None, name_like: str = None) -> int:
    """(已重构) 获取数据对象的总数，正确支持所有过滤条件。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 查询的主体部分与 get_data_objects 非常相似
    query = "SELECT COUNT(DISTINCT do.id) FROM data_objects do"
    params = []
    conditions = []

    if status:
        conditions.append("do.status = ?")
        params.append(status)
    if file_type:
        conditions.append("do.type = ?")
        params.append(file_type)
    if name_like:
        conditions.append("do.name LIKE ?")
        params.append(f"%{name_like}%")
        
    if tags and isinstance(tags, list) and len(tags) > 0:
        # (核心修复) 使用与上面完全相同的子查询逻辑
        subquery_conditions = " OR ".join(["t.name LIKE ?"] * len(tags))
        subquery = f"""
            do.id IN (
                SELECT dot.data_object_id
                FROM data_object_tags dot
                JOIN tags t ON dot.tag_id = t.id
                WHERE {subquery_conditions}
                GROUP BY dot.data_object_id
                HAVING COUNT(DISTINCT t.name) = ?
            )
        """
        conditions.append(subquery)
        for tag in tags:
            params.append(f"%{tag}%")
        params.append(len(tags))

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cursor.execute(query, params)
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_data_object_by_id(object_id: str) -> dict | None:
    """通过UUID检索单个数据对象。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        SELECT do.*, (SELECT GROUP_CONCAT(t.name) FROM tags t 
                      JOIN data_object_tags dot ON t.id = dot.tag_id 
                      WHERE dot.data_object_id = do.id) as tags
        FROM data_objects do
        WHERE do.id = ?
    """
    cursor.execute(query, (object_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        obj = dict(row)
        if obj.get('tags'):
            obj['tags'] = obj['tags'].split(',')
        else:
            obj['tags'] = []
        return obj
    return None

def update_data_object(object_id: str, **kwargs) -> bool:
    """
    通过UUID更新数据对象的字段。
    成功时返回True，失败或没有行被更新时返回False。
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    updates = []
    params = []
    tags_to_update = None

    kwargs['last_updated'] = datetime.now().isoformat(timespec='seconds') + 'Z'

    for key, value in kwargs.items():
        if key == 'id': continue
        if key == 'tags':
            tags_to_update = value # 单独处理标签
        else:
            updates.append(f"{key} = ?")
            params.append(value)

    try:
        cursor.execute("BEGIN")
        
        rows_updated = 0
        if updates:
            set_clause = ", ".join(updates)
            final_params = params + [object_id]
            cursor.execute(f"UPDATE data_objects SET {set_clause} WHERE id = ?", final_params)
            rows_updated = cursor.rowcount

        # 如果提供了标签，则更新标签（即使其他字段没有更新）
        if tags_to_update is not None:
            _manage_tags(cursor, object_id, tags_to_update)
            # 如果之前没有更新行，检查对象是否存在
            if rows_updated == 0:
                cursor.execute("SELECT id FROM data_objects WHERE id = ?", (object_id,))
                if cursor.fetchone():
                    rows_updated = 1 # 确认对象存在，标签操作视为一次更新

        conn.commit()
        
        if rows_updated == 0:
            print(f"警告: 没有为对象ID {object_id} 更新任何行（可能不存在或值相同）。")
            return False
        return True
    except sqlite3.Error as e:
        print(f"数据库更新ID '{object_id}' 时出错: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def delete_data_object(object_id: str) -> bool:
    """
    通过UUID删除数据对象。由于设置了ON DELETE CASCADE，关联的标签也会被清理。
    成功时返回True，失败时返回False。
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM data_objects WHERE id = ?", (object_id,))
        conn.commit()
        if cursor.rowcount == 0:
            print(f"警告: 没有为对象ID {object_id} 删除记录（可能不存在）。")
            return False
        print(f"已删除数据对象 ID: {object_id}")
        return True
    except sqlite3.Error as e:
        print(f"数据库删除ID '{object_id}' 时出错: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def get_tag_graph_data(min_frequency: int = 2, min_link_strength: int = 1):
    """
    (已更新) 为标签关系图谱准备数据。
    现在会主动过滤掉系统标签/元数据标签。
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # --- 1. 获取节点 (已过滤的标签和它们的频率) ---
    # (核心修改) 在查询中加入 WHERE ... NOT IN ... 子句来过滤掉停用词
    
    # 构建占位符 '?, ?, ?...'
    placeholders = ', '.join(['?'] * len(SYSTEM_TAG_STOP_LIST))
    
    node_query = f"""
        SELECT t.name, COUNT(dot.data_object_id) as frequency
        FROM tags t
        JOIN data_object_tags dot ON t.id = dot.tag_id
        WHERE t.name NOT IN ({placeholders})
        GROUP BY t.name
        HAVING frequency >= ?
        ORDER BY frequency DESC;
    """
    # 参数包括停用词列表和最低频率
    params = SYSTEM_TAG_STOP_LIST + [min_frequency]
    cursor.execute(node_query, params)
    
    # 节点大小可以调整得更夸张一些以突出差异
    nodes = [{'name': row['name'], 'value': row['frequency'], 'symbolSize': 10 + row['frequency'] * 2.5} for row in cursor.fetchall()]
    
    valid_node_names = {node['name'] for node in nodes}

    # --- 2. 获取边 (标签之间的共现关系) ---
    # 这个查询不需要修改，因为它依赖于上面已经过滤过的 valid_node_names
    link_query = """
        SELECT t1.name as source, t2.name as target, COUNT(dot1.data_object_id) as strength
        FROM data_object_tags dot1
        JOIN data_object_tags dot2 ON dot1.data_object_id = dot2.data_object_id AND dot1.tag_id < dot2.tag_id
        JOIN tags t1 ON dot1.tag_id = t1.id
        JOIN tags t2 ON dot2.tag_id = t2.id
        GROUP BY source, target
        HAVING strength >= ?
        ORDER BY strength DESC;
    """
    cursor.execute(link_query, (min_link_strength,))
    
    links = []
    for row in cursor.fetchall():
        if row['source'] in valid_node_names and row['target'] in valid_node_names:
            links.append({'source': row['source'], 'target': row['target'], 'value': row['strength']})

    conn.close()
    
    print(f"为图谱生成了 {len(nodes)} 个内容节点和 {len(links)} 条内容关系边。")
    return {"nodes": nodes, "links": links}


if __name__ == '__main__':
    # 此块用于演示和测试 db_manager 功能。
    print("运行 db_manager.py 示例...")
    if os.path.exists(DATABASE_FILE):
        print(f"为测试移除现有数据库: {DATABASE_FILE}")
        os.remove(DATABASE_FILE)
    
    init_db()

    # 创建一个虚拟测试文件
    if not os.path.exists(FILE_STORAGE_DIR):
        os.makedirs(FILE_STORAGE_DIR)
    
    test_doc_path_1 = os.path.join(FILE_STORAGE_DIR, f"example_doc_{uuid.uuid4().hex[:6]}.txt")
    with open(test_doc_path_1, 'w', encoding='utf-8') as f:
        f.write("This is a test document for db_manager.")
    
    summary1 = f"文本文件: {os.path.basename(test_doc_path_1)}, 大小: {os.path.getsize(test_doc_path_1)} 字节。内容: This is a test..."
    
    obj1_id = insert_data_object(
        name=os.path.basename(test_doc_path_1),
        file_type="text/plain",
        source="manual_test", # 新增source
        content_location=test_doc_path_1,
        content_summary=summary1,
        tags=["example", "text", "测试"],
        status="new"
    )
    if obj1_id:
        print(f"已插入 obj1, ID: {obj1_id}")
        retrieved_obj1 = get_data_object_by_id(obj1_id)
        print(f"已检索 obj1: {retrieved_obj1['name'] if retrieved_obj1 else '未找到'}")
        print(f"  - 它的标签: {retrieved_obj1.get('tags')}")
        update_data_object(obj1_id, status="classified", tags=["example", "text", "processed_test", "已处理测试"])
        retrieved_obj1_updated = get_data_object_by_id(obj1_id)
        print(f"已更新 obj1 的标签: {retrieved_obj1_updated['tags'] if retrieved_obj1_updated else '未找到'}")
    
    print("db_manager.py 示例运行完毕。")