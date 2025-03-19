import json
import hashlib
import time
from pathlib import Path
import os
from tqdm import tqdm

CACHE_FILE = "cache/file_counts.json"
os.makedirs('cache', exist_ok=True)


def count_lines_in_file(file_path):
    """Count the number of lines in a file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return sum(1 for _ in tqdm(file, desc="Counting lines", unit="line"))


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
        file_hash, hash_method = compute_md5(file_path)  # Compute hash of current file
        cached_hash = cached_data[file_path]["hash"]

        # If hash matches, return the cached line count
        if file_hash == cached_hash:
            print(f"Using cached line count for {file_path}")
            return cached_data[file_path]["line_count"]

    # If no cache or hash mismatch, compute line count and hash
    print(f"Computing line count for {file_path}")
    time.sleep(0.2)  # Wait a bit so print dont interfere with tqdm progress bars
    line_count = count_lines_in_file(file_path)  # Replace with actual line count function
    file_hash, hash_method = compute_md5(file_path)  # Compute file hash

    # Update cache
    file_data = {"path": file_path, "line_count": line_count, "hash": file_hash, "hash_method": hash_method}
    cached_data[file_path] = file_data
    save_cached_data(list(cached_data.values()))  # Save updated cache

    return line_count


def compute_md5(file_path, chunk_size=4096, size_threshold=10, preferred_hash_method=None) -> (str, str):
    """
    Compute MD5 hash of the file. If the file is larger than `size_limit`,
    it computes the hash of the first chunk only.

    :param file_path: Path to the file.
    :param chunk_size: Size of the chunk to read for large files (default: 4096 bytes).
    :param size_threshold: The file size threshold in GB's (default: 5GB), if above this threshold (and preferred_hash_method=None) then partial MD5 hash will be used to save time (but reduced accuracy).
    :param preferred_hash_method: The preferred hash method to use ('md5_partial' or 'md5_full'), set to None to determine the preferred hash method based on file size.

    :return (hash, hash_method): MD5 hash of the file and hash method (md5_full or md5_partial)
    """
    # If preferred_hash_method is not None, then already compute with method without looking at file size
    if preferred_hash_method is not None:
        if 'partial' in preferred_hash_method:
            hash_method = 'md5_partial'
            return compute_md5_partial(file_path, chunk_size), hash_method
        else:
            hash_method = 'md5_full'
            return compute_md5_full(file_path), hash_method

    size_threshold = size_threshold * 1024 * 1024 * 1024
    file_size = os.path.getsize(file_path)

    if file_size > size_threshold:
        # If file is larger than the size limit, hash only the first chunk
        hash_method = 'md5_partial'
        return compute_md5_partial(file_path, chunk_size), hash_method
    else:
        # Otherwise, compute the full MD5 hash of the file
        hash_method = 'md5_full'
        return compute_md5_full(file_path), hash_method


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


def find_files_without_extension(root_folder=None):
    """Find all files without extensions recursively."""
    if root_folder is None:
        root_folder = ''
    return [file.as_posix() for file in Path(root_folder).rglob('*') if file.is_file() and file.suffix == '' and '.' not in file.as_posix()]


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
        file_hash, hash_method = compute_md5(file)

        if file in cached_data and cached_data[file]["hash"] == file_hash:
            print(f"Skipping {file} (unchanged)")
            updated_data.append(cached_data[file])  # Keep existing data
        else:
            print(f"Processing {file} (hash changed or new file)")
            time.sleep(0.2)
            line_count = get_line_count_file(file)
            updated_data.append({"path": file, "line_count": line_count, "hash": file_hash, "hash_method": hash_method})

    return updated_data


def clean_invalid_files(file_path):
    # Open the file and read the data
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Filter the list to remove objects where the 'path' doesn't exist
    original_length = len(data)
    valid_data = [obj for obj in data if os.path.exists(obj['path'])]
    removed_count = original_length - len(valid_data)

    # Open the file in write mode and save the updated data back to the file
    with open(file_path, 'w') as file:
        json.dump(valid_data, file, indent=2)

    if removed_count > 0:
        # Print the number of objects removed
        print(f"Number of objects removed: {removed_count}")
        print(f"Cleaned JSON data has been saved to {file_path}")

if __name__ == '__main__':
    files = find_files_without_extension('data')
    data = make_dict_file_counts(files)
    save_cached_data(data)

    # Clean JSON (remove objects with files that do not exist anymore)
    clean_invalid_files(CACHE_FILE)
