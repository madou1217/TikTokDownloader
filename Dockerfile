# 使用单阶段镜像，仅提供运行环境，源码通过卷挂载
FROM python:3.12-bookworm

ARG NODE_VERSION=20

# 添加元数据标签
LABEL name="DouK-Downloader" authors="JoeanAmier" repository="https://github.com/JoeanAmier/TikTokDownloader"

# 安装 Python 依赖构建环境、Node.js 和 ffmpeg
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        ffmpeg \
        gnupg \
    && curl -fsSL "https://deb.nodesource.com/setup_${NODE_VERSION}.x" | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 仅安装依赖，不复制源码（源码通过卷挂载）
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

EXPOSE 5555
VOLUME /app/Volume
CMD ["python", "main.py"]
