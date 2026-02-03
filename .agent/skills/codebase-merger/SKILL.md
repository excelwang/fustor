---
name: codebase-merger
description: Merge all Python source files into a single text file to provide large-scale codebase context for analysis or refactoring. Use when you need to see the entire project's Python code at once, perform cross-module analysis, or prepare for major architectural changes.
---

# Codebase Merger

This skill provides a utility to consolidate all Python files within a project into a single text document. This is particularly useful for LLMs to understand complex interactions and dependencies across a whole codebase.

## Workflow

1.  **Identify the target directory**: Usually the root of a package or the entire monorepo.
2.  **Define output location**: A temporary or persistent text file (e.g., `merged_codebase.txt`).
3.  **Execute the merger**: Run the provided script.
4.  **Analyze the results**: Read the merged file to gain full context.

## Usage

### Basic Merge
Run the script without arguments to merge all `.py` files in the current working directory:

```bash
python3 .agent/skills/codebase-merger/scripts/merge_codebase.py
```

### Targeted Merge
Specify a source directory and an output file:

```bash
python3 .agent/skills/codebase-merger/scripts/merge_codebase.py <source_path> <output_path>
```

### Exclusions
The script automatically excludes common non-source directories:
- `.git`, `.venv`, `__pycache__`, `node_modules`
- `dist`, `build`, `.agent`, `.gemini`, etc.

## Benefits
- **Full Visibility**: Eliminates the need to open dozens of files individually.
- **Improved Context**: Allows the model to see global constants, shared models, and cross-module call chains.
- **Efficiency**: Reduces the number of `view_file` tool calls required for initial codebase exploration.
