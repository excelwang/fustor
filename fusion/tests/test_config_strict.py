"""
Strict YAML validation test to detect duplicate keys in configuration files.
"""
import pytest
import os
import yaml
from yaml.loader import SafeLoader
from pathlib import Path

class StrictLoader(SafeLoader):
    """
    Custom YAML loader that raises an error when duplicate keys are found.
    """
    def construct_mapping(self, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ValueError(f"Duplicate YAML key found: '{key}' at line {key_node.start_mark.line + 1}")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping

def get_all_yaml_files():
    root_dir = Path(__file__).parent.parent.parent # Project root
    yaml_files = list(root_dir.glob("**/*.yaml"))
    yaml_files.extend(list(root_dir.glob("**/*.yml")))
    # Exclude venv and hidden dirs
    return [f for f in yaml_files if ".venv" not in str(f) and "/." not in str(f)]

@pytest.mark.parametrize("yaml_path", get_all_yaml_files())
def test_yaml_no_duplicate_keys(yaml_path):
    """
    Scan every YAML file in the repository for duplicate keys.
    """
    with open(yaml_path, 'r') as f:
        content = f.read()
        if not content.strip():
            return
        
        try:
            yaml.load(content, Loader=StrictLoader)
        except ValueError as e:
            pytest.fail(f"YAML Duplicate Key Error in {yaml_path}: {e}")
        except Exception as e:
            # Other YAML errors (syntax, etc) should also be reported but focus is duplicates
            pytest.fail(f"YAML Syntax Error in {yaml_path}: {e}")

if __name__ == "__main__":
    # For manual run
    files = get_all_yaml_files()
    print(f"Scanning {len(files)} YAML files...")
    for f in files:
        try:
            with open(f, 'r') as stream:
                yaml.load(stream, Loader=StrictLoader)
            print(f"✓ {f}")
        except Exception as e:
            print(f"✗ {f}: {e}")
