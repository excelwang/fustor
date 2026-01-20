import click
import os
from .runner import BenchmarkRunner
from .generator import DataGenerator

@click.group()
def cli():
    """Fustor Benchmark Tool"""
    pass

@cli.command()
@click.argument("run-dir", type=click.Path(exists=False))
@click.option("--concurrency", "-c", default=20, help="Number of concurrent workers")
@click.option("--num-requests", "-n", default=200, help="Total requests to perform")
@click.option("--target-depth", "-d", default=5, help="Depth of target directories to benchmark")
@click.option("--gen-mode", "-m", type=click.Choice(['auto', 'force', 'skip']), default='auto', 
              help="Data generation strategy: auto (if missing), force (always), skip (never)")
def run(run_dir, concurrency, num_requests, target_depth, gen_mode):
    """Run full benchmark suite"""
    runner = BenchmarkRunner(run_dir)
    runner.run(
        concurrency=concurrency, 
        reqs=num_requests, 
        target_depth=target_depth, 
        gen_mode=gen_mode
    )

@cli.command()
@click.argument("run-dir", type=click.Path(exists=False))
@click.option("--num-dirs", default=1000, help="Number of UUID directories")
@click.option("--num-subdirs", default=4, help="Number of subdirectories per UUID directory")
@click.option("--files-per-subdir", default=250, help="Files per subdirectory")
def generate(run_dir, num_dirs, num_subdirs, files_per_subdir):
    """Generate benchmark dataset (Standalone)"""
    gen = DataGenerator(os.path.join(run_dir, "data"))
    gen.generate(num_dirs, num_subdirs, files_per_subdir)

if __name__ == "__main__":
    cli()
