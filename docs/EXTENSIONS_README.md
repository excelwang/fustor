# Fustor Packages 文档

本目录包含 Fustor Monorepo 中所有共享包和可插拔驱动的文档。

## 概述

`extensions/` 目录是 Fustor Monorepo 的核心组成部分，它包含了平台中所有可重用、可插拔的组件。这些组件可以是：

*   **驱动**: 例如 `source-*` (数据源), `sender-*` (数据推送), `receiver-*` (数据接收), `view-*` (视图存储)。
*   **模式**: 例如 `schema-*` (数据模式定义)。

## 结构

每个包通常都有自己的子目录，其中可能包含一个 `docs/` 目录来存放该包特有的文档。

*   `source-*/`: 各种数据源驱动，例如 `source-mysql`, `source-fs`, `source-elasticsearch` 等。
*   `sender-*/`: 各种数据推送驱动，例如 `sender-fusion`, `sender-echo`, `sender-openapi` 等。
*   `receiver-*/`: 数据接收驱动，例如 `receiver-http` (Fusion 的 HTTP 入口)。
*   `view-*/`: 数据存储视图驱动，例如 `view-fs` (文件系统后端)。
*   `schema-*/`: 数据模式定义，例如 `schema-fs`。

## 更多信息

请查阅每个具体包目录下的 `README.md` 或 `docs/` 目录，以获取更详细的信息。
