# fustor-cli

This package provides the unified Command Line Interface (CLI) for managing and interacting with various Fustor services within the monorepo. Built with `click`, it consolidates the CLIs of `fustor-registry`, `fustor-fusion`, and `fustor-agent` into a single entry point.

## Features

*   **Unified Interface**: Access commands for `registry`, `fusion`, and `agent` services from a single `fustor` command.
*   **Extensible**: Designed to easily integrate new service CLIs as the Fustor ecosystem grows.
*   **User-Friendly**: Leverages `click` for intuitive command-line argument parsing and help messages.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`. Once installed, the `fustor` command will be available in your shell.

## Usage

The `fustor` command acts as a dispatcher for subcommands related to each Fustor service.

### General Help

To see the available subcommands:

```bash
fustor --help
```

### Registry Service Commands

To see commands specific to the Registry service:

```bash
fustor registry --help
```

Example:

```bash
fustor registry users list
```

### Fusion Service Commands

To see commands specific to the Fusion service:

```bash
fustor fusion --help
```

Example:

```bash
fustor fusion start --port 8003
```

### Agent Service Commands

To see commands specific to the Agent service:

```bash
fustor agent --help
```

Example:

```bash
fustor agent start --reload
```

## Dependencies

*   `click`: A Python package for creating beautiful command line interfaces.
*   `fustor-agent`: The Fustor Agent service package, providing its CLI commands.
*   `fustor-fusion`: The Fustor Fusion service package, providing its CLI commands.
*   `fustor-registry`: The Fustor Registry service package, providing its CLI commands.
*   `fustor-common`: Shared utilities and components.
