import json
import hashlib
import time
from pathlib import Path
import os

CACHE_FILE = "cache/file_counts.json"
os.makedirs('cache', exist_ok=True)

def count_lines_in_file(file_path):
    """Count the number of lines in a file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return sum(1 for _ in file)

def get_line_count_file(file_path: str):
    """
    Get the line count of a file. If the file is cached and the MD5 hash is correct,
    return the cached line count. If not, compute the line count, hash, and update the cache.

    Args:
    - file_path (str): Path to the file.

    Returns:
    - int: Line count of the file.
    """
    # Load cached data
    cached_data = {entry["path"]: entry for entry in load_cached_data()}

    # Check if file is cached
    if file_path in cached_data:
        file_hash = compute_md5(file_path)  # Compute hash of current file
        cached_hash = cached_data[file_path]["hash"]

        # If hash matches, return the cached line count
        if file_hash == cached_hash:
            print(f"Using cached line count for {file_path}")
            return cached_data[file_path]["line_count"]

    # If no cache or hash mismatch, compute line count and hash
    print(f"Computing line count for {file_path}")
    line_count = count_lines_in_file(file_path)  # Replace with actual line count function
    file_hash = compute_md5(file_path)  # Compute file hash

    # Update cache
    file_data = {"path": file_path, "line_count": line_count, "hash": file_hash}
    cached_data[file_path] = file_data
    save_cached_data(list(cached_data.values()))  # Save updated cache

    return line_count

def compute_md5(file_path, chunk_size=4096, size_limit=5):
    """
    Compute MD5 hash of the file. If the file is larger than `size_limit`,
    it computes the hash of the first chunk only.

    Args:
    - file_path (str): Path to the file.
    - chunk_size (int): Size of the chunk to read for large files (default: 4096 bytes).
    - size_limit (int): The file size limit in GB's (default: 5GB).

    Returns:
    - str: MD5 hash of the file or the first chunk.
    """
    size_limit = size_limit * 1024 * 1024 * 1024
    file_size = os.path.getsize(file_path)

    if file_size > size_limit:
        # If file is larger than the size limit, hash only the first chunk
        return compute_md5_partial(file_path, chunk_size)
    else:
        # Otherwise, compute the full MD5 hash of the file
        return compute_md5_full(file_path)


def compute_md5_partial(file_path, chunk_size=4096):
    """Compute MD5 hash of only the first chunk of the file."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            chunk = f.read(chunk_size)  # Read only the first chunk
            hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"Error hashing {file_path}: {e}")
        return None  # In case of error


def compute_md5_full(file_path):
    """Compute the full MD5 hash of the file."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"Error hashing {file_path}: {e}")
        return None  # In case of error

def find_files_without_extension(root_folder):
    """Find all files without extensions recursively."""
    return [file.as_posix() for file in Path(root_folder).rglob('*') if file.is_file() and file.suffix == '']

def load_cached_data():
    """Load existing cached file counts and hashes."""
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_cached_data(data):
    """Save updated file counts and hashes to the cache file."""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def make_dict_file_counts(files):
    """Compute line counts, using cache if file hash is unchanged."""
    cached_data = {entry["path"]: entry for entry in load_cached_data()}
    updated_data = []

    for file in files:
        file_hash = compute_md5(file)

        if file in cached_data and cached_data[file]["hash"] == file_hash:
            print(f"Skipping {file} (unchanged)")
            updated_data.append(cached_data[file])  # Keep existing data
        else:
            print(f"Processing {file} (hash changed or new file)")
            time.sleep(0.5)
            line_count = get_line_count_file(file)
            updated_data.append({"path": file, "line_count": line_count, "hash": file_hash})

    return updated_data



if __name__ == '__main__':
    files = find_files_without_extension('data')
    data = make_dict_file_counts(files)
    save_cached_data(data)
