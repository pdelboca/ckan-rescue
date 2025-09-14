import argparse
import sys

from .dcat import DCATDownloader


def main():
    parser = argparse.ArgumentParser(description="Download data from a CKAN portal implementing ckanext-datajson")
    parser.add_argument("url", help="URL of the data.json file")
    parser.add_argument("-o", "--output", default="output", help="Output directory (default: output)")
    parser.add_argument(
        "-t", "--threads", type=int, default=5, help="Number of threads for parallel downloads (default: 5)"
    )
    parser.add_argument("-v", "--version", action="version", version="%(prog)s 0.1.0")

    args = parser.parse_args()

    downloader = DCATDownloader(args.url, args.output, args.threads)
    success = downloader.run()

    sys.exit(0 if success else 1)
