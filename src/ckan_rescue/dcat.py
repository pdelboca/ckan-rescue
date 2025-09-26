from rdflib import Graph, Namespace, RDF
import rdflib
from rdflib.util import guess_format

import logging
import os
import queue
import threading
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Define namespaces
DCAT = Namespace("http://www.w3.org/ns/dcat#")
DCT = Namespace("http://purl.org/dc/terms/")

class DCATDownloader:
    def __init__(self, url, max_threads=5):
        self.url = url
        self.netloc = urlparse(url).netloc
        self.base_path = Path("output") / self.netloc
        self.logs_path = self.base_path / "logs.txt"
        self.data_path = self.base_path / "data"
        self.max_threads = max_threads
        self.download_queue = queue.Queue()
        self.failed_downloads = []
        self.lock = threading.Lock()

    def create_directory_structure(self):
        """Create the required directory structure"""
        self.base_path.mkdir(parents=True, exist_ok=True)
        (self.base_path / "data").mkdir(exist_ok=True)

    def fetch_dcatfile(self):
        """Download and parse the RDF file"""
        filename = os.path.basename(urlparse(self.url).path)
        filepath = self.base_path / filename
        try:
            urllib.request.urlretrieve(self.url, filepath)
        except Exception as e:
            logger.error(f"Error fetching DCAT file: {e}")
            return None

        data = Graph()
        data.parse(filepath, format=guess_format(str(filepath)))
        return data

    def _get_identifier(self, resource):
        """Get the CKAN identifier for the resource, fallback to URI fragment"""
        # Dataset: https://example.org/dataset/dcf6958e-1027-4d59-979a-57d3b5a8dc41
        # Resource: https://example.org/dataset/dcf6958e-1027-4d59-979a-57d3b5a8dc41/resource/534e2760-c9e6-4c73-a019-d70ffb66cafd
        return resource.split("/")[-1]

    def _extract_file_from_url(self, download_url):
        """Extract file from URL."""
        parsed_url = urlparse(download_url)
        filename = os.path.basename(parsed_url.path)
        if not filename or filename == parsed_url.path:
            return ""
        return filename

    def _get_download_url(self, distribution, data):
        """Returns an URL pointing to the actual file to download."""
        download_url = data.value(distribution, DCAT.downloadURL)
        if download_url:
            return download_url

        # Some CKAN instances returns the downloadURL as accessURL. Others just a link to the resource page.
        # We return it and double check when performing the download if it is an actual file
        access_url = data.value(distribution, DCAT.accessURL)
        return access_url

    def prepare_download_tasks(self, data):
        for dataset in list(data.subjects(RDF.type, DCAT.Dataset))[:5]:
            for distribution in data.objects(dataset, DCAT.distribution):
                download_url = self._get_download_url(distribution, data)
                dist_id = self._get_identifier(distribution)

                if download_url:
                    dataset_id = self._get_identifier(dataset)
                    dist_filename = self._extract_file_from_url(download_url)

                    # Create the directory structure for this distribution
                    dist_dir = self.data_path / dataset_id / dist_id
                    dist_dir.mkdir(parents=True, exist_ok=True)

                    file_path = dist_dir / dist_filename
                    if file_path.exists():
                        logger.info(f"Skipping download: {file_path} already exists.")
                        continue
                    self.download_queue.put((download_url, str(file_path), dist_id))

    def download_worker(self):
        """Worker thread function to process download tasks"""
        while True:
            try:
                url, file_path, dist_id = self.download_queue.get(timeout=10)
                try:
                    logger.info(f"Downloading: {file_path}")
                    with urllib.request.urlopen(url) as response:
                        if response.info().get_content_type() == 'text/html':
                            # Some CKAN instances provide the access URL as download URL, therefore we are not downloading
                            # the file, but the HTML of the portal's resource/distribution.
                            logger.warning(f"The Download URL of {dist_id} might not be a file..")
                        with open(file_path, "wb") as out_file:
                            out_file.write(response.read())
                    logger.info(f"Downloaded: {file_path}")
                except Exception as e:
                    with self.lock:
                        self.failed_downloads.append(f"{url} - {e}")
                    logger.error(f"Failed to download {url}: {e}")

                self.download_queue.task_done()
            except queue.Empty:
                break

    def run(self):
        """Main method to execute the download process"""
        print("Creating directory structure...")
        self.create_directory_structure()

        # basicConfig requires directory structure created.
        logging.basicConfig(
            filename=self.logs_path,
            level=logging.INFO,
            filemode="w",
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

        print(f"Fetching data.json from {self.url}")
        data = self.fetch_dcatfile()
        if not data:
            return False

        print(f"Processing portal: {self.url}")

        self.prepare_download_tasks(data)

        total_files = self.download_queue.qsize()
        print(f"Found {total_files} files to download")

        if total_files == 0:
            print(f"No files to download. See {self.logs_path} for details.")
            return True

        print(f"Download in progress. See {self.logs_path} for details.")
        threads = []
        # for _ in range(self.max_threads):
        for _ in range(1):
            thread = threading.Thread(target=self.download_worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        # Wait for all downloads to complete
        self.download_queue.join()

        if self.failed_downloads:
            print(f"{len(self.failed_downloads)} downloads failed. See {self.logs_path} for details.")
        else:
            print("All downloads completed successfully.")

        return True
