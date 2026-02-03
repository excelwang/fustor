import os

def merge_python_files(root_dir, output_file):
    # Directories to exclude
    exclude_dirs = {
        '.git', '.venv', '__pycache__', 'node_modules', 
        'dist', 'build', '.agent', '.idea', '.vscode', 
        '.gemini', 'site-packages'
    }
    
    # Extensions to include
    include_exts = {'.py'}
    
    print(f"Scanning directory: {root_dir}")
    print(f"Writing to: {output_file}")
    
    start_path = os.path.abspath(root_dir)
    file_count = 0
    
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for root, dirs, files in os.walk(start_path):
            # Modify dirs in-place to skip ignored directories
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            for file in files:
                ext = os.path.splitext(file)[1]
                if ext in include_exts:
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, start_path)
                    
                    try:
                        with open(full_path, 'r', encoding='utf-8') as infile:
                            content = infile.read()
                            
                        header = f"\n{'='*80}\nFile: {rel_path}\n{'='*80}\n"
                        outfile.write(header)
                        outfile.write(content)
                        outfile.write("\n")
                        file_count += 1
                        # print(f"Added: {rel_path}")
                    except Exception as e:
                        print(f"Error reading {rel_path}: {e}")

    print(f"Done. Merged {file_count} files into {output_file}")

if __name__ == '__main__':
    # Use current working directory as root
    ROOT_DIR = os.getcwd()
    OUTPUT_FILE = os.path.join(ROOT_DIR, 'merged_python_scripts.txt')
    
    merge_python_files(ROOT_DIR, OUTPUT_FILE)
