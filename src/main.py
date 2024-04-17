from asset_mapping_scrapping.scrapper.scrapper_base import ScrapperFactory
from asset_mapping_scrapping.utils.utils import parse_config
import logging
from asset_mapping_scrapping.utils.logger import logging_counter, logger
import typer
from typing import Annotated, List
from pathlib import Path
from enum import Enum

app = typer.Typer()


class Mode(str, Enum):
    dev = "dev"
    prod = "prod"


@app.command()
def main(
    path_yaml: Annotated[
        str, typer.Option(help="Path to yaml containing the scrapers to launch")
    ] = None,
    scrapper_name: Annotated[
        str,
        typer.Option(
            help="Name of the scraper to launch. Needs to be the name of some class that inherits from class Scrapper."
        ),
    ] = None,
    scrapper_url: Annotated[
        List[str], typer.Option(help="URL(s) of the website to scrape")
    ] = None,
    mode: Annotated[
        Mode,
        typer.Option(
            case_sensitive=False, help="If dev mode is activated, then no export to s3."
        ),
    ] = Mode.prod,
    monitoring: bool = False,
):
    logger.info("START SCRAPPING")

    if path_yaml is not None:
        config = parse_config(path_yaml)
    else:
        config = {"sources": {scrapper_name: scrapper_url}}

    sources = config.get("sources").items()
    for scrapper_name, url in sources:
        scrapper = ScrapperFactory.get_handler(scrapper_name)(mode=mode)

        logger.info(f"Scraping {scrapper_name}")

        try:
            # raise Exception("I would like the traceback to be correctly logged")
            scrapper(url)

            logger.info(f"{scrapper_name} ended gracefully")
        except Exception as e:
            logger.error(f"In {scrapper_name}:")
            logger.exception(e)

    logger.info(f"{len(sources)} sources were scrapped,")
    logger.info(f"{logging_counter.warning_count} warnings have been encountered.")
    logger.info(f"{logging_counter.error_count} errors have been encountered.")


if __name__ == "__main__":
    app()
