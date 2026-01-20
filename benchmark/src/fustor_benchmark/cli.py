import click
import os
from .generator import DataGenerator
from .runner import BenchmarkRunner

@click.group()
def cli():
    """Fustor Benchmark Tool"""
    pass

@cli.command()
@click.argument("run-dir", type=click.Path(exists=False))
@click.option("--num-dirs", default=1000, help="Number of UUID directories")
@click.option("--num-subdirs", default=4, help="Number of subdirectories per UUID directory")
@click.option("--files-per-subdir", default=250, help="Files per subdirectory")
def generate(run_dir, num_dirs, num_subdirs, files_per_subdir):
    """Generate benchmark dataset"""
    gen = DataGenerator(os.path.join(run_dir, "data"))
    gen.generate(num_dirs, num_subdirs, files_per_subdir)

@cli.command()
@click.argument("run-dir", type=click.Path(exists=True))
@click.option("--concurrency", "-c", default=20, help="Number of concurrent workers")
@click.option("--requests", "-n", default=200, help="Total number of requests to run")
@click.option("--target-depth", "-d", default=5, help="Depth of target directories for benchmarking")
@click.option("--force-gen", is_flag=True, help="Force regeneration of test data")
@click.option("--skip-gen", is_flag=True, default=True, help="Skip generation if data exists (default: True)")
def run(run_dir, concurrency, requests, target_depth, force_gen, skip_gen):
    """Run the full benchmark suite"""
    abs_run_dir = os.path.abspath(run_dir)
    runner = BenchmarkRunner(run_dir=abs_run_dir)
    # If force_gen is False and skip_gen is True, we treat it as custom_target=True (skip generation)
    custom_target = skip_gen and not force_gen
    runner.run(concurrency=concurrency, reqs=requests, target_depth=target_depth, force_gen=force_gen, custom_target=custom_target)

cli.add_command(generate)
cli.add_command(run)

if __name__ == "__main__":
    cli()