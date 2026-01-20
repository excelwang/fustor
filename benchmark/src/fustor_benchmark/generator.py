import os
import uuid
import time
import shutil
import click
from concurrent.futures import ThreadPoolExecutor

class DataGenerator:
    def __init__(self, base_dir: str):
        self.base_dir = os.path.abspath(base_dir)
        self.submit_dir = os.path.join(self.base_dir, "upload/submit")

    def _create_batch(self, args):
        """
        Creates subdirectories and files within a specific UUID directory.
        args: (uuid_path, num_subdirs, files_per_subdir)
        """
        uuid_path, num_subdirs, files_per_subdir = args
        try:
            for s in range(num_subdirs):
                sub_path = os.path.join(uuid_path, f"sub_{s}")
                os.makedirs(sub_path, exist_ok=True)
                for i in range(files_per_subdir):
                    # Create empty dummy files
                    file_path = os.path.join(sub_path, f"data_{i:04d}.dat")
                    with open(file_path, "w") as f:
                        pass
        except Exception as e:
            print(f"Error generating data in {uuid_path}: {e}")

    def generate(self, num_uuids: int = 1000, num_subdirs: int = 4, files_per_subdir: int = 250):
        # Safety Check: Only allow operations in directories ending with 'fustor-benchmark-run'
        # Note: self.base_dir is typically run-dir/data, so we check the parent run-dir
        run_dir = os.path.dirname(self.base_dir)
        if not run_dir.endswith("fustor-benchmark-run"):
            click.echo(click.style(f"FATAL: Operation denied. Target run-dir '{run_dir}' must end with 'fustor-benchmark-run' for safety.", fg="red", bold=True))
            return

        if os.path.exists(self.base_dir):
            click.echo(f"Cleaning up old data in {self.base_dir}...")
            shutil.rmtree(self.base_dir)

        total_files = num_uuids * num_subdirs * files_per_subdir
        click.echo(f"Generating {total_files:,} files in 1000 UUID directories...")
        click.echo(f"Structure: {self.submit_dir}/{{c1}}/{{c2}}/{{uuid}}/sub_X/{{250 files}}")
        
        tasks = []
        for _ in range(num_uuids):
            uid = str(uuid.uuid4())
            # Target path at depth 5 (relative to base_dir/data):
            # 1:upload / 2:submit / 3:c1 / 4:c2 / 5:uuid
            path = os.path.join(self.submit_dir, uid[0], uid[1], uid)
            tasks.append((path, num_subdirs, files_per_subdir))

        start_gen = time.time()
        # Using a high worker count for I/O bound file creation
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 8) as executor:
            list(executor.map(self._create_batch, tasks))
        
        duration = time.time() - start_gen
        click.echo(f"Generation Complete: {duration:.2f}s (Average: {total_files/duration:.1f} files/sec)")
        return self.base_dir