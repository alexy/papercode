#!/usr/bin/env python3

import os
import gzip
import json
import requests
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Optional
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PapersWithCodeDatasetDownloader:
    """Downloads the official Papers with Code JSON datasets"""
    
    # Official dataset URLs from Papers with Code
    DATASETS = {
        'papers': {
            'url': 'https://production-media.paperswithcode.com/about/papers-with-abstracts.json.gz',
            'description': 'Papers with abstracts',
            'filename': 'papers-with-abstracts.json.gz'
        },
        'links': {
            'url': 'https://production-media.paperswithcode.com/about/links-between-papers-and-code.json.gz',
            'description': 'Links between papers and code',
            'filename': 'links-between-papers-and-code.json.gz'
        },
        'evaluations': {
            'url': 'https://production-media.paperswithcode.com/about/evaluation-tables.json.gz',
            'description': 'Evaluation tables',
            'filename': 'evaluation-tables.json.gz'
        },
        'methods': {
            'url': 'https://production-media.paperswithcode.com/about/methods.json.gz',
            'description': 'Methods',
            'filename': 'methods.json.gz'
        },
        'datasets': {
            'url': 'https://production-media.paperswithcode.com/about/datasets.json.gz',
            'description': 'Datasets',
            'filename': 'datasets.json.gz'
        }
    }
    
    def __init__(self, base_dir: str = "."):
        """Initialize downloader with base directory"""
        self.base_dir = Path(base_dir)
        self.download_date = datetime.now().strftime("%Y%m%d")
        self.download_dir = self.base_dir / f"pwc-{self.download_date}"
        
        # Create download directory
        self.download_dir.mkdir(exist_ok=True)
        
        # HTTP session for downloads
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PapersWithCode-Dataset-Downloader/1.0'
        })
        
        # Download statistics
        self.stats = {
            'files_downloaded': 0,
            'total_size_mb': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
        
        logger.info(f"📁 Download directory: {self.download_dir.absolute()}")
    
    def download_file(self, dataset_key: str, show_progress: bool = True) -> bool:
        """Download a single dataset file"""
        if dataset_key not in self.DATASETS:
            logger.error(f"Unknown dataset: {dataset_key}")
            return False
        
        dataset = self.DATASETS[dataset_key]
        url = dataset['url']
        filename = dataset['filename']
        description = dataset['description']
        
        local_path = self.download_dir / filename
        
        logger.info(f"⬇️  Downloading {description}...")
        logger.info(f"   URL: {url}")
        logger.info(f"   File: {local_path}")
        
        try:
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            
            # Get file size if available
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Show progress
                        if show_progress and total_size > 0:
                            progress = (downloaded_size / total_size) * 100
                            print(f"\r   Progress: {progress:.1f}% ({downloaded_size / (1024*1024):.1f} MB)", end='', flush=True)
            
            if show_progress:
                print()  # New line after progress
            
            # Update statistics
            file_size_mb = local_path.stat().st_size / (1024 * 1024)
            self.stats['files_downloaded'] += 1
            self.stats['total_size_mb'] += file_size_mb
            
            logger.info(f"✅ Downloaded {description} ({file_size_mb:.1f} MB)")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to download {description}: {e}")
            self.stats['errors'] += 1
            return False
        except Exception as e:
            logger.error(f"❌ Error downloading {description}: {e}")
            self.stats['errors'] += 1
            return False
    
    def download_all(self, delay_seconds: float = 1.0) -> Dict[str, bool]:
        """Download all dataset files"""
        logger.info("🚀 Starting Papers with Code dataset download")
        self.stats['start_time'] = datetime.now()
        
        results = {}
        
        for i, (dataset_key, dataset_info) in enumerate(self.DATASETS.items()):
            if i > 0:
                logger.info(f"⏳ Waiting {delay_seconds} seconds before next download...")
                time.sleep(delay_seconds)
            
            results[dataset_key] = self.download_file(dataset_key)
        
        self.stats['end_time'] = datetime.now()
        duration = self.stats['end_time'] - self.stats['start_time']
        
        logger.info("\n🎉 Download complete!")
        logger.info(f"✅ Files downloaded: {self.stats['files_downloaded']}/{len(self.DATASETS)}")
        logger.info(f"📊 Total size: {self.stats['total_size_mb']:.1f} MB")
        logger.info(f"⏰ Duration: {duration}")
        logger.info(f"❌ Errors: {self.stats['errors']}")
        
        return results
    
    def extract_file(self, dataset_key: str) -> Optional[str]:
        """Extract a gzipped JSON file and return the path to extracted file"""
        if dataset_key not in self.DATASETS:
            logger.error(f"Unknown dataset: {dataset_key}")
            return None
        
        filename = self.DATASETS[dataset_key]['filename']
        compressed_path = self.download_dir / filename
        extracted_path = self.download_dir / filename.replace('.gz', '')
        
        if not compressed_path.exists():
            logger.error(f"Compressed file not found: {compressed_path}")
            return None
        
        if extracted_path.exists():
            logger.info(f"Already extracted: {extracted_path}")
            return str(extracted_path)
        
        logger.info(f"📦 Extracting {filename}...")
        
        try:
            with gzip.open(compressed_path, 'rb') as f_in:
                with open(extracted_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            
            extracted_size_mb = extracted_path.stat().st_size / (1024 * 1024)
            logger.info(f"✅ Extracted to {extracted_path} ({extracted_size_mb:.1f} MB)")
            return str(extracted_path)
            
        except Exception as e:
            logger.error(f"❌ Failed to extract {filename}: {e}")
            return None
    
    def extract_all(self) -> Dict[str, Optional[str]]:
        """Extract all downloaded files"""
        logger.info("📦 Extracting all downloaded files...")
        
        results = {}
        for dataset_key in self.DATASETS.keys():
            results[dataset_key] = self.extract_file(dataset_key)
        
        return results
    
    def load_json_file(self, dataset_key: str, max_records: Optional[int] = None) -> Optional[List[Dict]]:
        """Load and parse a JSON dataset file"""
        if dataset_key not in self.DATASETS:
            logger.error(f"Unknown dataset: {dataset_key}")
            return None
        
        # First extract the file if needed
        extracted_path = self.extract_file(dataset_key)
        if not extracted_path:
            return None
        
        logger.info(f"📖 Loading {dataset_key} dataset...")
        
        try:
            with open(extracted_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                
                # Try to parse as a single JSON array first
                try:
                    data = json.loads(content)
                    if isinstance(data, list):
                        records = data[:max_records] if max_records else data
                        logger.info(f"✅ Loaded {len(records)} records from {dataset_key} (JSON array)")
                        return records
                    elif isinstance(data, dict):
                        # Single object, wrap in list
                        logger.info(f"✅ Loaded 1 record from {dataset_key} (single JSON object)")
                        return [data]
                except json.JSONDecodeError:
                    pass
                
                # If that fails, try line-by-line JSON
                records = []
                lines = content.split('\n')
                for line_num, line in enumerate(lines, 1):
                    line = line.strip()
                    if line:
                        try:
                            record = json.loads(line)
                            records.append(record)
                            
                            if max_records and len(records) >= max_records:
                                logger.info(f"   Limited to {max_records} records")
                                break
                                
                        except json.JSONDecodeError as e:
                            if line_num <= 5:  # Only warn for first few lines
                                logger.warning(f"   Invalid JSON on line {line_num}: {e}")
                            continue
                
                logger.info(f"✅ Loaded {len(records)} records from {dataset_key} (line-by-line JSON)")
                return records
            
        except Exception as e:
            logger.error(f"❌ Failed to load {dataset_key}: {e}")
            return None
    
    def get_download_info(self) -> Dict:
        """Get information about the download directory and files"""
        info = {
            'download_dir': str(self.download_dir),
            'download_date': self.download_date,
            'files': {},
            'total_size_mb': 0
        }
        
        for dataset_key, dataset_info in self.DATASETS.items():
            compressed_file = self.download_dir / dataset_info['filename']
            extracted_file = self.download_dir / dataset_info['filename'].replace('.gz', '')
            
            file_info = {
                'description': dataset_info['description'],
                'compressed_exists': compressed_file.exists(),
                'extracted_exists': extracted_file.exists(),
                'compressed_size_mb': 0,
                'extracted_size_mb': 0
            }
            
            if compressed_file.exists():
                size_mb = compressed_file.stat().st_size / (1024 * 1024)
                file_info['compressed_size_mb'] = round(size_mb, 2)
                info['total_size_mb'] += size_mb
            
            if extracted_file.exists():
                size_mb = extracted_file.stat().st_size / (1024 * 1024)
                file_info['extracted_size_mb'] = round(size_mb, 2)
            
            info['files'][dataset_key] = file_info
        
        info['total_size_mb'] = round(info['total_size_mb'], 2)
        return info
    
    def cleanup_compressed_files(self):
        """Remove compressed files after extraction to save space"""
        logger.info("🧹 Cleaning up compressed files...")
        
        for dataset_key, dataset_info in self.DATASETS.items():
            compressed_file = self.download_dir / dataset_info['filename']
            extracted_file = self.download_dir / dataset_info['filename'].replace('.gz', '')
            
            if compressed_file.exists() and extracted_file.exists():
                try:
                    compressed_file.unlink()
                    logger.info(f"   Removed {compressed_file.name}")
                except Exception as e:
                    logger.warning(f"   Failed to remove {compressed_file.name}: {e}")

def main():
    """Main function to download Papers with Code datasets"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Download Papers with Code official JSON datasets"
    )
    parser.add_argument(
        "--output-dir", 
        default=".",
        help="Output directory for downloads (default: current directory)"
    )
    parser.add_argument(
        "--delay", 
        type=float, 
        default=2.0,
        help="Delay between downloads in seconds (default: 2.0)"
    )
    parser.add_argument(
        "--datasets", 
        nargs="+",
        choices=["papers", "links", "evaluations", "methods", "datasets"],
        help="Specific datasets to download (default: all)"
    )
    parser.add_argument(
        "--extract", 
        action="store_true", 
        default=True,
        help="Extract downloaded files (default: True)"
    )
    parser.add_argument(
        "--no-extract", 
        action="store_true",
        help="Skip extraction of downloaded files"
    )
    parser.add_argument(
        "--cleanup", 
        action="store_true",
        help="Remove compressed files after extraction"
    )
    
    args = parser.parse_args()
    
    # Initialize downloader
    downloader = PapersWithCodeDatasetDownloader(args.output_dir)
    
    # Download files
    if args.datasets:
        # Download specific datasets
        logger.info(f"Downloading specific datasets: {args.datasets}")
        results = {}
        for dataset_key in args.datasets:
            if dataset_key in downloader.DATASETS:
                results[dataset_key] = downloader.download_file(dataset_key)
                if args.delay > 0 and dataset_key != args.datasets[-1]:
                    logger.info(f"⏳ Waiting {args.delay} seconds...")
                    time.sleep(args.delay)
            else:
                logger.error(f"Unknown dataset: {dataset_key}")
    else:
        # Download all files
        results = downloader.download_all(delay_seconds=args.delay)
    
    # Extract files if requested
    extract_files = args.extract and not args.no_extract
    if extract_files:
        if args.datasets:
            extracted = {}
            for dataset_key in args.datasets:
                if results.get(dataset_key):
                    extracted[dataset_key] = downloader.extract_file(dataset_key)
        else:
            extracted = downloader.extract_all()
    
    # Cleanup compressed files if requested
    if args.cleanup and extract_files:
        downloader.cleanup_compressed_files()
    
    # Show download info
    info = downloader.get_download_info()
    logger.info(f"\n📋 Download Summary:")
    logger.info(f"   Directory: {info['download_dir']}")
    logger.info(f"   Total size: {info['total_size_mb']} MB")
    
    for dataset_key, file_info in info['files'].items():
        if not args.datasets or dataset_key in args.datasets:
            status = "✅" if file_info['extracted_exists'] else "❌"
            logger.info(f"   {status} {file_info['description']}: {file_info['extracted_size_mb']} MB")
    
    logger.info(f"\n🎯 Use the downloaded data with:")
    logger.info(f"   python pwc_offline_loader.py {downloader.download_dir}")
    logger.info(f"   # Or in Python:")
    logger.info(f"   from pwc_offline_loader import PapersWithCodeOfflineLoader")
    logger.info(f"   loader = PapersWithCodeOfflineLoader('{downloader.download_dir}')")

if __name__ == "__main__":
    main()