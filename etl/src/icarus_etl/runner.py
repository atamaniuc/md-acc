import logging

import click
from neo4j import GraphDatabase

from icarus_etl.pipelines.cnpj import CNPJPipeline
from icarus_etl.pipelines.sanctions import SanctionsPipeline
from icarus_etl.pipelines.transparencia import TransparenciaPipeline
from icarus_etl.pipelines.tse import TSEPipeline

PIPELINES: dict[str, type] = {
    "cnpj": CNPJPipeline,
    "tse": TSEPipeline,
    "transparencia": TransparenciaPipeline,
    "sanctions": SanctionsPipeline,
}


@click.group()
def cli() -> None:
    """ICARUS ETL — Data ingestion pipelines for Brazilian public data."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@cli.command()
@click.option("--source", required=True, help="Pipeline: cnpj, tse, transparencia, sanctions")
@click.option("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j URI")
@click.option("--neo4j-user", default="neo4j", help="Neo4j user")
@click.option("--neo4j-password", required=True, help="Neo4j password")
@click.option("--data-dir", default="./data", help="Directory for downloaded data")
@click.option("--limit", type=int, default=None, help="Limit rows processed")
@click.option("--chunk-size", type=int, default=50_000, help="Chunk size for batch processing")
@click.option("--streaming/--no-streaming", default=False, help="Streaming mode")
@click.option("--start-phase", type=int, default=1, help="Skip to phase N")
def run(
    source: str,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    data_dir: str,
    limit: int | None,
    chunk_size: int,
    streaming: bool,
    start_phase: int,
) -> None:
    """Run an ETL pipeline."""
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    if source not in PIPELINES:
        available = ", ".join(PIPELINES.keys())
        raise click.ClickException(f"Unknown source: {source}. Available: {available}")

    pipeline_cls = PIPELINES[source]
    pipeline = pipeline_cls(driver=driver, data_dir=data_dir, limit=limit, chunk_size=chunk_size)

    if streaming and hasattr(pipeline, "run_streaming"):
        pipeline.run_streaming(start_phase=start_phase)
    else:
        pipeline.run()

    driver.close()


@cli.command()
@click.option("--output-dir", default="./data/cnpj", help="Output directory")
@click.option("--files", type=int, default=10, help="Number of files per type (0-9)")
@click.option("--skip-existing/--no-skip-existing", default=True)
def download(output_dir: str, files: int, skip_existing: bool) -> None:
    """Download CNPJ data from Receita Federal."""
    import zipfile
    from pathlib import Path

    import httpx

    logger = logging.getLogger(__name__)

    base_url = "https://dadosabertos.rfb.gov.br/CNPJ/"
    file_types = ["Empresas", "Socios", "Estabelecimentos"]

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    for file_type in file_types:
        for i in range(min(files, 10)):
            filename = f"{file_type}{i}.zip"
            url = f"{base_url}{filename}"
            dest = out / filename
            try:
                if skip_existing and dest.exists():
                    logger.info("Skipping (exists): %s", dest.name)
                    continue

                logger.info("Downloading %s...", url)
                with httpx.stream("GET", url, follow_redirects=True, timeout=300) as response:
                    response.raise_for_status()
                    with open(dest, "wb") as f:
                        for chunk in response.iter_bytes(chunk_size=8192):
                            f.write(chunk)
                logger.info("Downloaded: %s", dest.name)

                logger.info("Extracting %s...", dest.name)
                with zipfile.ZipFile(dest, "r") as zf:
                    zf.extractall(out)
            except httpx.HTTPError:
                logger.warning("Failed to download %s (may not exist)", filename)


@cli.command()
def sources() -> None:
    """List available data sources."""
    click.echo("Available pipelines:")
    click.echo("  cnpj          - Receita Federal (Company Registry)")
    click.echo("  tse           - Tribunal Superior Eleitoral (Elections)")
    click.echo("  transparencia - Portal da Transparencia (Federal Spending)")
    click.echo("  sanctions     - CEIS/CNEP/CEPIM/CEAF (Sanctions)")


if __name__ == "__main__":
    cli()
