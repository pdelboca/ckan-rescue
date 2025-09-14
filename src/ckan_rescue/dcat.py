import json
import logging
import os
import queue
import threading
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class DCATDownloader:
    def __init__(self, datajson_url, output_dir="output", max_threads=5):
        self.datajson_url = datajson_url
        self.url = urlparse(datajson_url).netloc
        self.output_dir = output_dir
        self.base_path = Path(self.output_dir) / self.url
        self.logs_path = self.base_path / "logs.txt"
        self.max_threads = max_threads
        self.download_queue = queue.Queue()
        self.failed_downloads = []
        self.lock = threading.Lock()

    def _extract_file_from_url(download_url):
        """Extract file from URL."""
        parsed_url = urlparse(download_url)
        filename = os.path.basename(parsed_url.path)
        if not filename or filename == parsed_url.path:
            return ""
        return filename

    def fetch_datajson(self):
        """Download and parse the data.json file"""
        try:
            with urllib.request.urlopen(self.datajson_url) as response:
                data = json.loads(response.read().decode())
                return data
        except Exception as e:
            logger.error(f"Error fetching data.json: {e}")
            return None

    def create_directory_structure(self):
        """Create the required directory structure"""
        self.base_path.mkdir(parents=True, exist_ok=True)
        (self.base_path / "data").mkdir(exist_ok=True)

    def prepare_download_tasks(self, data, base_path):
        """Prepare all download tasks from the data.json"""
        datajson_path = base_path / "data.json"
        with open(datajson_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        for dataset in data.get("dataset", []):
            dataset_id = dataset.get("identifier", "unknown_dataset")
            for distribution in dataset.get("distribution", []):
                download_url = distribution.get("downloadURL")
                if download_url:
                    dist_id = distribution.get("identifier", "unknown_distribution")

                    filename = distribution.get("fileName")
                    if not filename:
                        filename = self._extract_file_from_url(download_url)
                    if not filename:
                        filename = f"dist_{dist_id}"

                    # Create the directory structure for this distribution
                    dist_dir = base_path / "data" / dataset_id / dist_id
                    dist_dir.mkdir(parents=True, exist_ok=True)

                    file_path = dist_dir / filename

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
                    # Download the file
                    with urllib.request.urlopen(url) as response:
                        with open(file_path, "wb") as out_file:
                            out_file.write(response.read())
                    logger.info(f"Downloaded: {file_path}")
                except Exception as e:
                    # Log failed download
                    with self.lock:
                        self.failed_downloads.append(f"{url} - {e}")
                    logger.error(f"Failed to download {url}: {e}")

                self.download_queue.task_done()
            except queue.Empty:
                break

    def run(self):
        """Main method to execute the download process"""
        print(f"Fetching data.json from {self.datajson_url}")
        data = self.fetch_datajson()
        if not data:
            return False

        print(f"Processing portal: {self.url}")

        self.create_directory_structure()

        # basicConfig requires directory structure created.
        logging.basicConfig(
            filename=self.logs_path,
            level=logging.INFO,
            filemode="w",
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

        self.prepare_download_tasks(data, self.base_path)

        total_files = self.download_queue.qsize()
        print(f"Found {total_files} files to download")

        if total_files == 0:
            print(f"No files to download. See {self.logs_path} for details.")
            return True

        print(f"Download in progress. See {self.logs_path} for details.")
        threads = []
        for i in range(self.max_threads):
            thread = threading.Thread(target=self.download_worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        # Wait for all downloads to complete
        self.download_queue.join()

        if self.failed_downloads:
            print(f"{len(self.failed_downloads)} downloads failed. See {self.log_file} for details.")
        else:
            print("All downloads completed successfully.")

        return True
