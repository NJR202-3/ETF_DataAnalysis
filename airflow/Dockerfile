# 使用 Ubuntu 22.04 作為基礎映像檔
FROM ubuntu:22.04

# 更新套件列表，並安裝必要的系統依賴
RUN apt-get update && \
    apt-get install -y \
        python3.10 \
        python3-pip \
        curl \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 安裝 uv
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh \
 && uv --version

# 安裝 Google Cloud SDK
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y

# 建立 Airflow 工作目錄
RUN mkdir -p /opt/airflow

# 安裝 Airflow 2.10.4 及其所有依賴（包括 PostgreSQL、GCP 和 Docker）
RUN uv pip install --system "apache-airflow[google,docker,postgres]==2.10.4" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.10.txt"

# 強制安裝 PyMySQL（解決依賴衝突）
RUN uv pip install --system --force-reinstall pymysql==1.1.2

# 複製專案檔案到 Airflow 工作目錄
COPY ../pyproject.toml ../uv.lock ../README.md /opt/airflow/
COPY ../data_ingestion /opt/airflow/data_ingestion/
COPY ../key.json /opt/airflow/key.json

# 安裝專案依賴，然後強制重新安裝指定版本的 SQLAlchemy
RUN cd /opt/airflow && uv pip install --system -e .
RUN uv pip install --system --reinstall sqlalchemy==1.4.36

# 設定時區和 Airflow 主目錄
ENV AIRFLOW_HOME=/opt/airflow

# 設定 Google Cloud 憑證環境變數
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/key.json

# 設定語系環境和時區
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8 TZ=Asia/Taipei

# 設定 Airflow 工作目錄
WORKDIR /opt/airflow
