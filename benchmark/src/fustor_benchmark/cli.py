import click
import os
from .generator import DataGenerator
from .runner import BenchmarkRunner

@click.group()
def cli():
    """Fustor Benchmark Tool"""
    pass

@cli.command()
@click.argument("data-dir", type=click.Path(exists=False))
@click.option("--num-dirs", default=100, help="Number of UUID directories")
@click.option("--files-per-dir", default=1000, help="Files per directory")
def generate(data_dir, num_dirs, files_per_dir):
    """Generate benchmark dataset"""
    gen = DataGenerator(data_dir)
    gen.generate(num_dirs, files_per_dir)

@cli.command()
@click.argument("data-dir", type=click.Path(exists=True))
@click.option("--concurrency", "-c", default=20, help="Number of concurrent workers")
@click.option("--requests", "-n", default=200, help="Total number of requests to run")
@click.option("--target-depth", "-d", default=5, help="Depth of target directories for benchmarking")
@click.option("--force-gen", is_flag=True, help="Force regeneration of test data")
@click.option("--skip-gen", is_flag=True, default=True, help="Skip generation if data exists (default: True)")
def run(data_dir, concurrency, requests, target_depth, force_gen, skip_gen):
    """Run the full benchmark suite"""
    abs_data_dir = os.path.abspath(data_dir)
    runner = BenchmarkRunner(data_dir=abs_data_dir)
    # If force_gen is False and skip_gen is True, we treat it as custom_target=True (skip generation)
    custom_target = skip_gen and not force_gen
    runner.run(concurrency=concurrency, reqs=requests, target_depth=target_depth, force_gen=force_gen, custom_target=custom_target)

cli.add_command(generate)
cli.add_command(run)

if __name__ == "__main__":
    cli()