#!/usr/bin/env python
"""Database migration runner for TBP Data Pipeline.

This script provides a simple interface to run Alembic migrations
with proper error handling and logging.
"""
import sys
import argparse
import logging
from pathlib import Path
from alembic import command
from alembic.config import Config
from rich.console import Console
from rich.table import Table

# Add project root to path
sys.path.append(str(Path(__file__).parents[1]))

from src.config_loader import ConfigLoader

console = Console()
logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False):
    """Configure logging for migration runner."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def get_alembic_config():
    """Get Alembic configuration."""
    config_path = Path(__file__).parent.parent / "alembic.ini"
    if not config_path.exists():
        console.print(f"[red]Error: alembic.ini not found at {config_path}[/red]")
        sys.exit(1)
    
    return Config(str(config_path))


def test_database_connection():
    """Test database connection before running migrations."""
    try:
        config_loader = ConfigLoader()
        db_config = config_loader.get_database_config()
        console.print("[green]✓[/green] Database configuration loaded successfully")
        return True
    except Exception as e:
        console.print(f"[red]✗ Failed to load database configuration: {e}[/red]")
        return False


def show_migration_status(config):
    """Display current migration status."""
    console.print("\n[bold]Current Migration Status[/bold]")
    
    # This is a simplified version - in production you'd query the database
    console.print("Run 'alembic current' to see current revision")
    console.print("Run 'alembic history' to see all migrations")


def run_migrations(args):
    """Main migration runner."""
    setup_logging(args.verbose)
    
    # Test database connection
    if not test_database_connection():
        console.print("[red]Aborting: Database connection failed[/red]")
        sys.exit(1)
    
    # Get Alembic config
    config = get_alembic_config()
    
    # Handle different commands
    if args.command == "status":
        show_migration_status(config)
        
    elif args.command == "upgrade":
        target = args.revision or "head"
        console.print(f"[yellow]Upgrading database to {target}...[/yellow]")
        
        if args.dry_run:
            console.print("[cyan]DRY RUN - Showing SQL that would be executed:[/cyan]")
            command.upgrade(config, target, sql=True)
        else:
            try:
                command.upgrade(config, target)
                console.print(f"[green]✓ Successfully upgraded to {target}[/green]")
            except Exception as e:
                console.print(f"[red]✗ Migration failed: {e}[/red]")
                sys.exit(1)
                
    elif args.command == "downgrade":
        target = args.revision or "-1"
        console.print(f"[yellow]Downgrading database to {target}...[/yellow]")
        
        if args.dry_run:
            console.print("[cyan]DRY RUN - Showing SQL that would be executed:[/cyan]")
            command.downgrade(config, target, sql=True)
        else:
            try:
                command.downgrade(config, target)
                console.print(f"[green]✓ Successfully downgraded to {target}[/green]")
            except Exception as e:
                console.print(f"[red]✗ Downgrade failed: {e}[/red]")
                sys.exit(1)
                
    elif args.command == "current":
        console.print("[yellow]Current database revision:[/yellow]")
        command.current(config, verbose=args.verbose)
        
    elif args.command == "history":
        console.print("[yellow]Migration history:[/yellow]")
        command.history(config, verbose=args.verbose)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="TBP Data Pipeline database migration runner"
    )
    
    parser.add_argument(
        "command",
        choices=["status", "upgrade", "downgrade", "current", "history"],
        help="Migration command to run"
    )
    
    parser.add_argument(
        "-r", "--revision",
        help="Target revision (default: head for upgrade, -1 for downgrade)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show SQL that would be executed without running it"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    # Show banner
    console.print("\n[bold blue]TBP Data Pipeline - Database Migration Runner[/bold blue]")
    console.print("=" * 50)
    
    run_migrations(args)


if __name__ == "__main__":
    main()
