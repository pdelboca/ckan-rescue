import sys

import click

from .datajson import DataJsonDownloader

@click.group()
@click.argument("url")
@click.pass_context
def cli(ctx, url):
    ctx.obj = url


@cli.command()
@click.option("--output", required=False, default="output", help="Output directory")
@click.option("--threads", required=False, type=int, default=5, help="Number of threads for parallel downloads")
@click.pass_obj
def datajson_download(url, output, threads):
    downloader = DataJsonDownloader(url, output, threads)
    success = downloader.run()

    sys.exit(0 if success else 1)


def main():
    cli()
