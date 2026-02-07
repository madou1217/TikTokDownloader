# Docker 使用说明

本项目镜像仅提供运行环境，源码通过卷挂载进入容器，不打包进镜像。

## 运行方式

1. 构建前端静态资源（输出到 `static/admin` 和 `static/client`）

```bash
docker compose --profile build run --rm frontend-build
```

2. 启动服务

```bash
docker compose up -d
```

默认服务：
- 后端：`http://127.0.0.1:5555`
- Nginx：`http://127.0.0.1`，并转发 `/admin`、`/client` 与文档接口

## 关键文件说明

- `docker-compose.yaml`：服务编排，包含 `backend`、`frontend-build`、`nginx`
- `Dockerfile`：运行环境镜像（Python、Node、ffmpeg、依赖），不复制源码
- `nginx/nginx.conf`：容器内 Nginx 配置
- `nginx/nginx.host.conf`：宿主机 Nginx 配置模板
- `.env.backend`：后端环境变量
- `admin-ui/.env.production`、`client-ui/.env.production`：前端生产环境变量

## Nginx 容器

使用 `docker compose up -d` 会自动启动 Nginx 容器并加载 `nginx/nginx.conf`。
该配置会：
- `/` 重定向到 `/client-ui/`
- `/admin-ui/`、`/client-ui/` 直接读取 `static/admin` 与 `static/client`
- `/admin/`、`/client/`、`/docs`、`/redoc`、`/openapi.json` 转发到后端

## 宿主机已有 Nginx 的配置方式

如果服务器已经有 Nginx，可以不启动 Nginx 容器，仅启动后端：

```bash
docker compose up -d backend
```

然后把 `nginx/nginx.host.conf` 拷贝到宿主机并修改：
- `server_name` 改成你的域名
- `alias` 路径改成项目 `static` 目录的绝对路径

例如（仅示例，路径与域名请按实际修改）：

```bash
cp "./nginx/nginx.host.conf" "/etc/nginx/conf.d/tiktok-downloader.conf"
```

## 环境变量说明

### 后端 `.env.backend`

- `SERVER_HOST`：后端监听地址，默认 `0.0.0.0`
- `SERVER_PORT`：后端监听端口，默认 `5555`
- `TZ`：时区，例如 `Asia/Shanghai`
- `APP_ENV`：运行环境标识（默认 `production`）

### 前端 `.env.production`

`admin-ui/.env.production` 与 `client-ui/.env.production` 使用同一变量：

- `VITE_API_BASE`：后端 API Base URL

推荐值：
- 与 Nginx 同域部署：留空即可（使用相对路径）
- 跨域部署：设置为 `https://example.com`（不要以 `/` 结尾）

修改 `VITE_API_BASE` 后需要重新执行前端构建命令。
