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
def run(target_dir, concurrency, num_requests, target_depth, integrity_interval, fusion_api, api_key):
    """Executes the automated performance benchmark."""
    run_dir = os.path.abspath(DEFAULT_RUN_DIR)
    runner = BenchmarkRunner(run_dir, target_dir, fusion_api, api_key)
    runner.run(
        concurrency=concurrency, 
        reqs=num_requests, 
        target_depth=target_depth,
        integrity_interval=integrity_interval
    )

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