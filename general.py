from tqdm import tqdm

def get_line_count_file(file_path: str):
    count = 0
    file_path_short = file_path.split('/')[-1]
    with open(file_path, 'r', encoding='utf-8') as f:
        for _ in tqdm(f, desc=f'Counting lines in {file_path_short}'):
            count += 1
    return count