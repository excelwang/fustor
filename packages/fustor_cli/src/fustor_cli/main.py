import click

from fustor_registry.cli import cli as registry_cli
from fustor_fusion.cli import cli as fusion_cli
from fustor_agent.cli import cli as agent_cli

@click.group()
def cli():
    """Fustor Unified CLI"""
    pass

cli.add_command(registry_cli, name="registry")
cli.add_command(fusion_cli, name="fusion")
cli.add_command(agent_cli, name="agent")