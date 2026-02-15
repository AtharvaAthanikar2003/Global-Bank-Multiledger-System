from databricks import sql
import os

def get_connection():
    host = os.getenv("DATABRICKS_HOST")
    path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    # 🔍 DEBUG (safe)
    print("DEBUG HOST:", repr(host))
    print("DEBUG PATH:", repr(path))
    print("DEBUG TOKEN PREFIX:", token[:4] if token else None)
    print("DEBUG TOKEN LEN:", len(token) if token else None)

    return sql.connect(
        server_hostname=host,
        http_path=path,
        access_token=token,
    )