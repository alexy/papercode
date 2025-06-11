#!/usr/bin/env python3

import os
import requests
import json
import csv
from pathlib import Path
from urllib.parse import urlparse
import time

class GlamaDownloader:
    def __init__(self, github_token=None):
        self.github_token = github_token
        self.headers = {
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'glama-downloader/1.0'
        }
        if github_token:
            self.headers['Authorization'] = f'token {github_token}'
        
        self.glama_dir = Path('./glama')
        self.glama_dir.mkdir(exist_ok=True)
        
        self.csv_file = Path('./glama_files.csv')
        self.csv_writer = None
        self.csv_file_handle = None
    
    def search_glama_files(self):
        """Search for glama.json files across GitHub"""
        print("🔍 Searching for glama.json files on GitHub...")
        
        if not self.github_token:
            print("⚠️  No GitHub token provided. The GitHub search API requires authentication.")
            print("   Set GITHUB_TOKEN environment variable for higher rate limits and access.")
            print("   Trying with basic search anyway...")
        
        search_url = "https://api.github.com/search/code"
        params = {
            'q': 'filename:glama.json',
            'per_page': 100
        }
        
        try:
            response = requests.get(search_url, headers=self.headers, params=params)
            
            if response.status_code == 401:
                print("❌ GitHub API requires authentication for code search.")
                print("   Please set GITHUB_TOKEN environment variable with a personal access token.")
                print("   You can create one at: https://github.com/settings/tokens")
                return []
            
            response.raise_for_status()
            
            data = response.json()
            total_count = data.get('total_count', 0)
            items = data.get('items', [])
            
            print(f"📊 Found {total_count} glama.json files")
            return items
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error searching GitHub: {e}")
            return []
    
    def download_file(self, item, index, total):
        """Download a single glama.json file"""
        repo_name = item['repository']['full_name']
        file_path = item['path']
        
        # Get the SHA from the item and use Contents API instead
        file_sha = item.get('sha', '')
        
        print(f"📥 [{index}/{total}] Downloading from {repo_name}: {file_path}")
        
        try:
            # Use GitHub Contents API to get the file content
            contents_url = f"https://api.github.com/repos/{repo_name}/contents/{file_path}"
            response = requests.get(contents_url, headers=self.headers)
            response.raise_for_status()
            
            content_data = response.json()
            
            # Decode base64 content
            import base64
            file_content = base64.b64decode(content_data['content']).decode('utf-8')
            
            # Create safe filename
            safe_repo_name = repo_name.replace('/', '_')
            safe_file_path = file_path.replace('/', '_')
            local_filename = f"{safe_repo_name}_{safe_file_path}"
            
            local_path = self.glama_dir / local_filename
            
            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(file_content)
            
            print(f"✅ Saved: {local_filename}")
            
            # Write to CSV
            self.write_to_csv(repo_name, file_path)
            
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to download {repo_name}/{file_path}: {e}")
            return False
        except Exception as e:
            print(f"❌ Error saving {repo_name}/{file_path}: {e}")
            return False
    
    def write_to_csv(self, repo_name, file_path):
        """Write download info to CSV file"""
        try:
            org, repo = repo_name.split('/', 1)
            filename = file_path.split('/')[-1]  # Get just the filename
            
            if self.csv_writer:
                self.csv_writer.writerow([org, repo, filename])
                self.csv_file_handle.flush()  # Ensure immediate write
        except Exception as e:
            print(f"⚠️  Error writing to CSV: {e}")
    
    def setup_csv(self):
        """Initialize CSV file and writer"""
        try:
            self.csv_file_handle = open(self.csv_file, 'w', newline='', encoding='utf-8')
            self.csv_writer = csv.writer(self.csv_file_handle)
            self.csv_writer.writerow(['org', 'repo', 'filename'])  # Header
            print(f"📊 CSV tracking file: {self.csv_file.absolute()}")
        except Exception as e:
            print(f"⚠️  Error setting up CSV: {e}")
    
    def cleanup_csv(self):
        """Close CSV file handle"""
        if self.csv_file_handle:
            self.csv_file_handle.close()
    
    def run(self):
        """Main function to search and download all glama.json files"""
        print("🚀 Starting Glama JSON Downloader")
        print(f"📁 Files will be saved to: {self.glama_dir.absolute()}")
        
        # Setup CSV tracking
        self.setup_csv()
        
        try:
            # Search for files
            files = self.search_glama_files()
            
            if not files:
                print("❌ No glama.json files found")
                return
            
            print(f"\n⬇️  Starting download of {len(files)} files...\n")
            
            # Download files with progress
            successful = 0
            failed = 0
            
            for i, item in enumerate(files, 1):
                if self.download_file(item, i, len(files)):
                    successful += 1
                else:
                    failed += 1
                
                # Rate limiting - be nice to GitHub API
                time.sleep(0.5)
            
            print(f"\n🎉 Download complete!")
            print(f"✅ Successfully downloaded: {successful}")
            print(f"❌ Failed downloads: {failed}")
            print(f"📁 Files saved in: {self.glama_dir.absolute()}")
            print(f"📊 CSV file: {self.csv_file.absolute()}")
            
        finally:
            # Always cleanup CSV file handle
            self.cleanup_csv()

def main():
    # Get GitHub token from environment variable
    github_token = os.getenv('GITHUB_TOKEN')
    
    downloader = GlamaDownloader(github_token)
    downloader.run()

if __name__ == "__main__":
    main()