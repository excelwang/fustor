import click
import os
from .generator import DataGenerator
from .runner import BenchmarkRunner

@click.group()
def cli():
    """Fustor Benchmark Tool"""
    pass

@click.command()
@click.option("--num-dirs", default=100, help="Number of UUID directories")
@click.option("--files-per-dir", default=1000, help="Files per directory")
def generate(num_dirs, files_per_dir):
    """Generate benchmark dataset"""
    gen = DataGenerator("benchmark_data")
    gen.generate(num_dirs, files_per_dir)

@click.command()
@click.option("--concurrency", "-c", default=20, help="Number of concurrent workers")
@click.option("--requests", "-n", default=200, help="Total number of requests to run")
@click.option("--force-gen", is_flag=True, help="Force regeneration of test data")
@click.option("--target-dir", help="Path to an existing directory to benchmark (skips generation)")
def run(concurrency, requests, force_gen, target_dir):
    """Run the full benchmark suite"""
    # If target_dir is provided, use it. Otherwise default to "benchmark_data"
    data_dir = target_dir if target_dir else "benchmark_data"
    if target_dir:
        data_dir = os.path.abspath(target_dir)

    runner = BenchmarkRunner(data_dir=data_dir)
    # Pass flag to indicate if we are using custom target
    runner.run(concurrency=concurrency, reqs=requests, force_gen=force_gen, custom_target=bool(target_dir))

cli.add_command(generate)
cli.add_command(run)

if __name__ == "__main__":
    cli()