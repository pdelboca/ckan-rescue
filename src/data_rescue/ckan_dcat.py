import json
import os
import urllib.request
import urllib.error
import threading
import queue
from urllib.parse import urlparse
from pathlib import Path


class CKANDataDownloader:
    def __init__(self, datajson_url, output_dir="output", max_threads=5):
        self.datajson_url = datajson_url
        self.output_dir = output_dir
        self.max_threads = max_threads
        self.download_queue = queue.Queue()
        self.failed_downloads = []
        self.lock = threading.Lock()

    def fetch_datajson(self):
        """Download and parse the data.json file"""
        try:
            with urllib.request.urlopen(self.datajson_url) as response:
                data = json.loads(response.read().decode())
                return data
        except Exception as e:
            print(f"Error fetching data.json: {e}")
            return None

    def create_directory_structure(self, homepage):
        """Create the required directory structure"""
        base_path = Path(self.output_dir) / homepage
        base_path.mkdir(parents=True, exist_ok=True)
        (base_path / "data").mkdir(exist_ok=True)
        return base_path

    def prepare_download_tasks(self, data, base_path):
        """Prepare all download tasks from the data.json"""
        # Save the data.json file
        datajson_path = base_path / "data.json"
        with open(datajson_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        # Prepare download tasks for each distribution
        for dataset in data.get('dataset', []):
            dataset_id = dataset.get('identifier', 'unknown_dataset')
            for distribution in dataset.get('distribution', []):
                download_url = distribution.get('downloadURL')
                if download_url:
                    # Create the directory structure for this distribution
                    dist_id = distribution.get('identifier', 'unknown_distribution')
                    dist_dir = base_path / "data" / dataset_id / dist_id
                    dist_dir.mkdir(parents=True, exist_ok=True)

                    # Get filename from distribution or extract from URL
                    filename = distribution.get('fileName')
                    if not filename:
                        # Extract filename from URL
                        parsed_url = urlparse(download_url)
                        filename = os.path.basename(parsed_url.path)
                        if not filename or filename == parsed_url.path:
                            filename = f"file_{dist_id}"

                    file_path = dist_dir / filename

                    # Add to download queue
                    self.download_queue.put((download_url, str(file_path), dist_id))

    def download_worker(self):
        """Worker thread function to process download tasks"""
        while True:
            try:
                url, file_path, dist_id = self.download_queue.get(timeout=10)
                try:
                    # Download the file
                    with urllib.request.urlopen(url) as response:
                        with open(file_path, 'wb') as out_file:
                            out_file.write(response.read())
                    print(f"Downloaded: {file_path}")
                except Exception as e:
                    # Log failed download
                    with self.lock:
                        self.failed_downloads.append(f"{url} - {e}")
                    print(f"Failed to download {url}: {e}")

                self.download_queue.task_done()
            except queue.Empty:
                break

    def run(self):
        """Main method to execute the download process"""
        print(f"Fetching data.json from {self.datajson_url}")
        data = self.fetch_datajson()
        if not data:
            return False

        homepage = data.get('homepage', 'unknown_portal')
        print(f"Processing portal: {homepage}")

        base_path = self.create_directory_structure(homepage)
        log_file = base_path / "logs.txt"

        # Prepare all download tasks
        self.prepare_download_tasks(data, base_path)

        total_files = self.download_queue.qsize()
        print(f"Found {total_files} files to download")

        if total_files == 0:
            print("No files to download")
            return True

        # Start worker threads
        threads = []
        for i in range(self.max_threads):
            thread = threading.Thread(target=self.download_worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)

        # Wait for all downloads to complete
        self.download_queue.join()

        # Write log file
        if self.failed_downloads:
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write("Failed downloads:\n")
                for entry in self.failed_downloads:
                    f.write(f"{entry}\n")
            print(f"{len(self.failed_downloads)} downloads failed. See {log_file} for details.")
        else:
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write("All downloads completed successfully.")
            print("All downloads completed successfully.")

        return True
