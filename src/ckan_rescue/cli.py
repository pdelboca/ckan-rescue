import sys

import click

from .datajson import DataJsonDownloader
from .dcat import DCATDownloader

@click.group()
@click.argument("url")
@click.pass_context
def cli(ctx, url):
    ctx.obj = url


@cli.command()
@click.option("--threads", required=False, type=int, default=5, help="Number of threads for parallel downloads")
@click.pass_obj
def datajson_download(url, threads):
    downloader = DataJsonDownloader(url, threads)
    success = downloader.run()

    sys.exit(0 if success else 1)


@cli.command()
@click.option("--threads", required=False, type=int, default=5, help="Number of threads for parallel downloads")
@click.pass_obj
def dcat_download(url, threads):
    downloader = DCATDownloader(url, threads)
    success = downloader.run()

    sys.exit(0 if success else 1)


def main():
    cli()
