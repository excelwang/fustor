import click
import os
from .runner import BenchmarkRunner
from .generator import DataGenerator

# Hardcoded run directory name for safety and consistency
DEFAULT_RUN_DIR = "fustor-benchmark-run"

@click.group()
def cli():
    """Fustor Benchmark Tool"""
    pass

@cli.command()
@click.argument("target-dir", type=click.Path(exists=True))
@click.option("-c", "--concurrency", default=20, help="Number of concurrent workers.")
@click.option("-n", "--num-requests", default=200, help="Total number of requests to perform.")
@click.option("-d", "--target-depth", default=5, help="Relative depth to probe for targets.")
@click.option("--integrity-interval", default=60.0, help="Wait interval (seconds) for OS Integrity check (simulating NFS sync).")
@click.option("--fusion-api", help="External Fusion API URL (skips local setup).")
@click.option("--api-key", help="API Key for external Fusion API.")
@click.option("--view-id", default="bench-view", help="View ID to query.")
@click.option("--base-port", default=18100, help="Base port for benchmark microservices.")
def query(target_dir, concurrency, num_requests, target_depth, integrity_interval, fusion_api, api_key, view_id, base_port):
    """Executes the automated metadata query & performance benchmark."""
    run_dir = os.path.abspath(DEFAULT_RUN_DIR)
    runner = BenchmarkRunner(run_dir, target_dir, fusion_api, api_key, base_port=base_port, view_id=view_id)
    runner.run(
        concurrency=concurrency, 
        reqs=num_requests, 
        target_depth=target_depth,
        integrity_interval=integrity_interval
    )

@cli.command()
@click.argument("target-dir", type=click.Path(exists=True))
@click.option("--view-id", default="bench-view", help="View ID to query.")
@click.option("--base-port", default=18100, help="Base port for benchmark microservices.")
def fs_scan(target_dir, view_id, base_port):
    """Executes the file system scanning (Pre-scan, Snapshot, Audit, Sentinel) benchmarks."""
    run_dir = os.path.abspath(DEFAULT_RUN_DIR)
    runner = BenchmarkRunner(run_dir, target_dir, base_port=base_port, view_id=view_id)
    runner.run_fs_scan()

@cli.command()
@click.argument("target-dir", type=click.Path(exists=False))
@click.option("--num-dirs", default=1000, help="Number of UUID directories")
@click.option("--num-subdirs", default=4, help="Number of subdirectories per UUID directory")
@click.option("--files-per-subdir", default=250, help="Files per subdirectory")
def generate(target_dir, num_dirs, num_subdirs, files_per_subdir):
    """Generate benchmark dataset in the specified TARGET-DIR"""
    gen = DataGenerator(os.path.abspath(target_dir))
    gen.generate(num_dirs, num_subdirs, files_per_subdir)

if __name__ == "__main__":
    cli()