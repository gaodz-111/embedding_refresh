# 依赖包
markdown==3.5.2 \
fastapi==0.104.1 \
uvicorn[standard]==0.24.0 \
pydantic==2.5.2 \
psycopg2-binary==2.9.9 \
pgvector==0.2.0 \
requests==2.31.0 \
python-dotenv==1.0.0



# 接口 URL：
http://.../refresh-embeddings

# 请求方式：
POST

# 请求体格式（JSON）：
{
  "db_ip": "",
  "db_port": ,
  "db_username": "",
  "db_password": "",
  "db_name": "",
  "table_name": "",
  "text_field_name": "",
  "vector_field_name": ""
}
# 向量刷新接口请求体字段
db_ip：string 类型，必选，无默认值。PostgreSQL 数据库 IP 地址。
db_port：integer 类型，必选，默认值 5432。PostgreSQL 数据库端口（默认 5432，若修改过需对应调整）。
db_name：string 类型，必选，无默认值。目标数据库名称。
table_name：string 类型，必选，无默认值。目标表名。
text_field_name：string 类型，必选，无默认值。文本字段名（存储需生成向量的原始文本，如：text_content,需要跟数据库字段名匹配）。
vector_field_name：string 类型，必选，无默认值。向量字段名（存储生成的高维向量，如：embedding，类型需为 VECTOR (1024) 需要跟数据库字段名匹配）。


# 文件配置
程序 main.py
配置向量化服务环境 .env
