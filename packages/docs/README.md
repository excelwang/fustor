# Fustor Packages 文档

本目录包含 Fustor Monorepo 中所有共享包和可插拔驱动的文档。

## 概述

`packages/` 目录是 Fustor Monorepo 的核心组成部分，它包含了平台中所有可重用、可插拔的组件。这些组件可以是：

*   **共享库**: 例如 `common` (通用工具、异常、日志配置等) 和 `core` (核心抽象、基类、数据模型等)。
*   **SDKs**: 例如 `fusion-sdk` (用于与 Fusion 服务交互的客户端库)。
*   **驱动**: 例如 `source-*` (数据源驱动) 和 `sender-*` (数据推送驱动)。

## 结构

每个包通常都有自己的子目录，其中可能包含一个 `docs/` 目录来存放该包特有的文档。

*   `common/`: 包含平台通用的工具、异常、日志配置等。
*   `core/`: 包含核心抽象、基类和数据模型，是构建驱动和服务的基础。
*   `fusion-sdk/`: Fusion 服务的 Python SDK，用于简化与其他服务或外部应用与 Fusion 的集成。
*   `source-*/`: 各种数据源驱动，例如 `source-mysql`, `source-fs`, `source-elasticsearch` 等。
*   `sender-*/`: 各种数据推送驱动，例如 `sender-fusion`, `sender-echo`, `sender-openapi` 等。

## 更多信息

请查阅每个具体包目录下的 `README.md` 或 `docs/` 目录，以获取更详细的信息。
