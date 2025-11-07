# Fustor Registry 服务文档

本目录包含 Fustor Registry 服务的相关文档。

## 概述

Fustor Registry 服务负责管理整个 Fustor 平台的核心元数据，包括存储环境、数据存储库、用户、API Key 和凭据。它提供统一的注册和发现机制，是 Fustor 平台的基础组件。

## 模块

*   **API**: 提供 RESTful API 接口，用于外部系统与 Registry 服务进行交互。
*   **Database**: 负责与数据库进行交互，持久化元数据。
*   **Models**: 定义了 Registry 服务中使用的所有数据模型。
*   **Security**: 处理用户认证、授权和 API Key 管理。

## 更多信息

*   **API 文档**: 访问 `/docs` (Swagger UI) 或 `/redoc` (ReDoc) 查看详细的 API 接口说明。
