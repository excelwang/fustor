#!/usr/bin/env python3

import os
import toml
from pathlib import Path

# Base directory
base_dir = Path("/home/huajin/fustor_monorepo")

# Get all pyproject.toml files in packages directory
package_dirs = [d for d in (base_dir / "packages").iterdir() if d.is_dir()]
pyproject_files = [d / "pyproject.toml" for d in package_dirs if (d / "pyproject.toml").exists()]

# Add main services
main_services = ["agent", "fusion", "registry"]
for service in main_services:
    pyproject_files.append(base_dir / service / "pyproject.toml")

for pyproject_file in pyproject_files:
    print(f"Processing {pyproject_file}")
    
    # Read the pyproject.toml file
    with open(pyproject_file, 'r', encoding='utf-8') as f:
        data = toml.load(f)
    
    # Extract package name from the path
    pkg_path = pyproject_file.parent.relative_to(base_dir)
    
    # Update or add project.urls
    if 'project.urls' not in data:
        data['project.urls'] = {}
    
    data['project.urls']['Homepage'] = f"https://github.com/excelwang/fustor/{pkg_path}"
    data['project.urls']['Bug Tracker'] = "https://github.com/excelwang/fustor/issues"
    
    # Write the updated pyproject.toml file
    with open(pyproject_file, 'w', encoding='utf-8') as f:
        toml.dump(data, f)
    
    print(f"  Updated URLs for {pkg_path}")