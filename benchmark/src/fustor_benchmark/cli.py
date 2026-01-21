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
@click.option("--concurrency", "-c", default=20, help="Number of concurrent workers")
@click.option("--num-requests", "-n", default=200, help="Total requests to perform")
@click.option("--target-depth", "-d", default=5, help="Depth of target directories to benchmark")
@click.option("--fusion-api", help="URL of an already running Fusion API (e.g., http://localhost:18102)")
@click.option("--api-key", help="API Key for the external Fusion API")
def run(target_dir, concurrency, num_requests, target_depth, fusion_api, api_key):
    """Run benchmark suite. Defaults to auto-starting services unless --fusion-api is provided."""
    run_dir = os.path.abspath(DEFAULT_RUN_DIR)
    
    runner = BenchmarkRunner(
        run_dir, 
        target_dir=os.path.abspath(target_dir),
        fusion_api_url=fusion_api,
        api_key=api_key
    )
    runner.run(
        concurrency=concurrency, 
        reqs=num_requests, 
        target_depth=target_depth
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
