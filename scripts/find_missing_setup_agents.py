import re
import glob

def find_missing_fixture():
    test_files = glob.glob("it/consistency/test_*.py")
    results = []

    for file_path in test_files:
        with open(file_path, "r") as f:
            content = f.read()
            
        # Regex to find test functions and their arguments
        matches = re.findall(r"def (test_[a-zA-Z0-9_]+)\s*\(([^)]*)\):", content)
        
        for func_name, args in matches:
            arg_list = [arg.strip() for arg in args.split(",")]
            if "setup_agents" not in arg_list:
                results.append(f"{file_path}::{func_name}")

    return results

if __name__ == "__main__":
    missing = find_missing_fixture()
    for item in missing:
        print(item)
