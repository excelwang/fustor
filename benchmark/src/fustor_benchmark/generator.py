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
        dir_path, file_count = args
        os.makedirs(dir_path, exist_ok=True)
        for i in range(file_count):
            with open(os.path.join(dir_path, f"data_{i:04d}.dat"), "w") as f:
                pass

    def generate(self, num_uuids: int = 100, files_per_dir: int = 1000):
        if os.path.exists(self.base_dir):
            click.echo(f"Cleaning up old data in {self.base_dir}...")
            shutil.rmtree(self.base_dir)

        click.echo(f"Generating {num_uuids * files_per_dir} files in {self.submit_dir}/{{c1}}/{{c2}}/{{uuid}} ...")
        
        tasks = []
        for _ in range(num_uuids):
            uid = str(uuid.uuid4())
            # Path: .../upload/submit/{c0}/{c1}/{uuid}
            path = os.path.join(self.submit_dir, uid[0], uid[1], uid)
            tasks.append((path, files_per_dir))

        start_gen = time.time()
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 4) as executor:
            list(executor.map(self._create_batch, tasks))
        
        duration = time.time() - start_gen
        click.echo(f"Generation Complete: {duration:.2f}s")
        return self.base_dir
