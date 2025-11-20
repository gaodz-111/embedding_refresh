from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel, Field
from psycopg2.extras import RealDictCursor
import psycopg2
from pgvector.psycopg2 import register_vector
import requests
import os
from dotenv import load_dotenv
import logging

# 1. 加载环境变量
load_dotenv()
VECTOR_SERVICE_URL = os.getenv("VECTOR_SERVICE_URL")
VECTOR_MODEL = os.getenv("VECTOR_MODEL")
VECTOR_USER = os.getenv("VECTOR_USER")
VECTOR_HEADERS = {
    "Accept": os.getenv("VECTOR_HEADER_ACCEPT"),
    "Accept-Encoding": os.getenv("VECTOR_HEADER_ACCEPT_ENCODING"),
    "Connection": os.getenv("VECTOR_HEADER_CONNECTION"),
    "Content-Type": os.getenv("VECTOR_HEADER_CONTENT_TYPE"),
    "User-Agent": os.getenv("VECTOR_HEADER_USER_AGENT")
}

# 2. 日志配置
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 3. 初始化 FastAPI 实例
app = FastAPI(title="PostgreSQL 向量批量刷新服务", version="4.0")


# 4. 核心请求模型：一个模型包含所有需要的参数（一次性接收）
class RefreshEmbeddingParams(BaseModel):
    """所有请求参数统一放在一个模型中，一次性接收"""
    # 数据库连接参数
    db_ip: str = Field(..., description="PostgreSQL 服务器IP（例：172.70.10.163）")
    db_port: int = Field(default=5432, description="PostgreSQL 端口（默认 5432）")
    db_username: str = Field(..., description="PostgreSQL 登录用户名（例：root）")
    db_password: str = Field(..., description="PostgreSQL 登录密码（例：3or+l2fZuXoSiLsFSwo）")
    db_name: str = Field(..., description="目标数据库名（例：nlp_service）")
    # 表和字段映射参数
    table_name: str = Field(..., description="目标表名（例：konx_pg_embedding）")
    text_field_name: str = Field(..., description="存储文本的字段名（例：text_content）")
    vector_field_name: str = Field(..., description="存储向量的字段名（例：embedding）")


# 5. 向量服务调用（无修改，内部固定配置）
def call_vector_service(text: str) -> list[float]:
    if not all([VECTOR_SERVICE_URL, VECTOR_MODEL, VECTOR_USER]):
        logger.error("向量服务环境变量配置不完整")
        raise ValueError("向量服务内部配置缺失")

    payload = {
        "input": [text.strip()],
        "model": VECTOR_MODEL,
        "user": VECTOR_USER
    }

    try:
        logger.debug(f"调用向量服务：text={text[:50]}...")
        response = requests.post(
            url=VECTOR_SERVICE_URL,
            headers=VECTOR_HEADERS,
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        result = response.json()

        if not result.get("data") or not isinstance(result["data"], list) or len(result["data"]) == 0:
            logger.error(f"向量服务响应格式错误：{result}")
            raise ValueError("向量服务返回无有效embedding数据")

        embedding = result["data"][0].get("embedding")
        if not embedding or not isinstance(embedding, list):
            logger.error(f"向量服务响应缺失embedding字段：{result}")
            raise ValueError("向量服务返回格式非法")

        return embedding

    except requests.exceptions.RequestException as e:
        error_msg = f"向量服务调用失败：{str(e)}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f"，响应内容：{e.response.text[:200]}"
        logger.error(error_msg)
        raise ConnectionError(error_msg)
    except Exception as e:
        logger.error(f"向量生成异常：{str(e)}")
        raise RuntimeError(f"向量生成失败：{str(e)}")



def batch_refresh_embeddings(params: RefreshEmbeddingParams):
    """连接数据库 → 动态匹配表名 → 读取文本 → 更新向量"""
    conn = None
    try:
        logger.info(f"连接数据库：{params.db_ip}:{params.db_port}/{params.db_name}")
        conn = psycopg2.connect(
            host=params.db_ip,
            port=params.db_port,
            dbname=params.db_name,
            user=params.db_username,
            password=params.db_password
        )
        register_vector(conn)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 关键步骤1：动态查询 public 下匹配的表（忽略大小写）
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND lower(table_name) = lower(%s)  -- 强制忽略大小写匹配
        """, (params.table_name,))  # 传入用户参数，避免SQL注入
        match_table = cur.fetchone()

        if not match_table:
            logger.error(f"public 下无匹配表：{params.table_name}（忽略大小写）")

            # 新增：打印当前数据库 public 下的所有表名（核心排查逻辑）
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;  -- 按表名排序，方便查看
            """)
            all_tables = cur.fetchall()  # 获取所有表名
            if all_tables:
                # 提取表名列表，拼接成字符串
                table_names = [table["table_name"] for table in all_tables]
                logger.error(f"当前数据库 {params.db_name} 的 public 下所有表名：{', '.join(table_names)}")
            else:
                logger.error(f"当前数据库 {params.db_name} 的 public 下无任何表！")

            return
        actual_table_name = match_table["table_name"]  # 获取表的实际名称（解决大小写问题）
        full_table_name = f"public.{actual_table_name}"  # 拼接完整表路径（public.实际表名）
        logger.info(f"匹配到目标表：{full_table_name}")

        # 关键步骤2：用完整表路径查询数据（100% 能找到）
        query = f"""
            SELECT id, "{params.text_field_name}" AS text 
            FROM {full_table_name}
        """
        cur.execute(query)
        rows = cur.fetchall()
        total = len(rows)
        logger.info(f"查询到 {total} 条记录，开始批量更新向量")

        if total == 0:
            logger.info("无记录需要更新")
            return

        success_count = 0
        fail_count = 0
        for idx, row in enumerate(rows, 1):
            record_id = row["id"]
            text = row["text"]

            try:
                embedding = call_vector_service(text)
                # 关键步骤3：用完整表路径更新（100% 能定位到表）
                update_query = f"""
                    UPDATE {full_table_name}
                    SET "{params.vector_field_name}" = %s
                    WHERE id = %s
                """
                cur.execute(update_query, (embedding, record_id))
                success_count += 1
                if idx % 10 == 0:
                    logger.info(f"已更新 {idx}/{total} 条记录")
            except Exception as e:
                fail_count += 1
                logger.error(f"第 {idx} 条记录（ID: {record_id}）更新失败：{str(e)}")
                continue

        conn.commit()
        logger.info(f"向量刷新完成：成功 {success_count} 条，失败 {fail_count} 条")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"数据库操作失败：{str(e)}")
    except Exception as e:
        logger.error(f"批量刷新异常：{str(e)}")
    finally:
        if conn:
            conn.close()
            logger.info("数据库连接已关闭")


# 7. 唯一核心接口：一次性接收所有参数，触发刷新
@app.post("/refresh-embeddings", summary="批量刷新向量")
async def refresh_embeddings(
    params: RefreshEmbeddingParams,  # 仅一个参数对象，包含所有信息
    background_tasks: BackgroundTasks
):
    background_tasks.add_task(batch_refresh_embeddings, params)
    return {
        "status": "success",
        "message": "已接收到所有参数，正在后台刷新向量",
        "received_params": {
            "db_ip": params.db_ip,
            "db_name": params.db_name,
            "table_name": params.table_name,
            "text_field": params.text_field_name,
            "vector_field": params.vector_field_name
        }  # 可选：返回接收的关键参数，方便用户确认
    }


# 健康检查接口（可选保留）
@app.get("/health", summary="服务健康检查")
async def health_check():
    return {
        "status": "healthy",
        "service": "pg-embedding-refresher",
        "vector_service_configured": bool(VECTOR_SERVICE_URL)
    }