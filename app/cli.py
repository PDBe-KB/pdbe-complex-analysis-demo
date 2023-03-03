from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click

from app.app import run_complex_portal, run_entry
from app.pdbe_complex import run_pdbe_complex
from app.utils import create_schema_indexes, drop_everything


@click.group()
def main():
    pass


@main.command(
    help="Create schema indexes",
)
def create_indexes():
    create_schema_indexes()


@main.command(
    help="Remove all nodes and relationships",
)
def remove_all():
    drop_everything()


@main.command(
    help="Load data for a PDB entry",
)
@click.option(
    "--entry",
    "-e",
    required=True,
    help="PDB entry ID",
)
def load_entry(entry: str):
    run_entry(entry)


@main.command(
    help="Load a list of PDB entries",
)
@click.option(
    "--entries",
    required=True,
    help="PDB entry IDs separated by comma or a file with one entry per line",
)
@click.option(
    "--threads",
    default=4,
    help="Number of threads to use",
)
def load_entries(entries: str, threads: int):
    entries_list = []

    if Path(entries).is_file():
        with open(entries) as f:
            entries_list = [x.strip() for x in f.readlines()]
    else:
        entries_list = entries.split(",")

    with ThreadPoolExecutor(max_workers=threads) as executor:
        executor.map(run_entry, entries_list)


@main.command(
    help="Load Complex portal data",
)
def load_complex_portal_data():
    run_complex_portal()


@main.command(
    help="Run PDBe Complex analysis",
)
@click.option(
    "--outcsv",
    "-o",
    help="Complex-Subcomplex output CSV file",
    required=True,
)
def run_pdbe_complex_analysis(outcsv: str):
    run_pdbe_complex(outcsv)


if __name__ == "__main__":
    main()
