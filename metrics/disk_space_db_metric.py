import pandas as pd
from matplotlib import pyplot as plt

from classes.DBType import DBTypes, DBType
from general_metrics import expand_excel
import os
import platform
import ctypes

def is_running_as_admin() -> bool:
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

def get_db_storage_path(db_type: DBTypes, sqlite_path: str = None) -> str | None:
    """
    Returns the default/common storage path for each DB type.
    """
    system = platform.system()

    if db_type == DBTypes.SQLITE:
        return sqlite_path if sqlite_path and os.path.isfile(sqlite_path) else None

    if system == "Linux":
        if db_type == DBTypes.MYSQL:
            return "/var/lib/mysql"
        elif db_type == DBTypes.POSTGRESQL:
            return "/var/lib/postgresql"
        elif db_type == DBTypes.MONGODB:
            return "/var/lib/mongodb"

    elif system == "Darwin":  # macOS
        if db_type == DBTypes.MYSQL:
            return "/usr/local/var/mysql"
        elif db_type == DBTypes.POSTGRESQL:
            return "/usr/local/var/postgres"
        elif db_type == DBTypes.MONGODB:
            return "/usr/local/var/mongodb"

    elif system == "Windows":
        if not is_running_as_admin():
            pass
            # raise PermissionError("Please run this script as an administrator to get storage space for Windows databases.")
        if db_type == DBTypes.MYSQL:
            return r"C:\ProgramData\MySQL\MySQL Server 8.0\Data\reddit_data_20m"
        elif db_type == DBTypes.POSTGRESQL:
            return r"C:\Program Files\PostgreSQL\17\data"
        elif db_type == DBTypes.MONGODB:
            return r"C:\Program Files\MongoDB\Server\8.0\data"

    return None

def get_dir_size(path: str) -> int:
    """
    Returns total size of the directory or file at path in bytes.
    """
    if not os.path.exists(path):
        return 0

    if os.path.isfile(path):
        return os.path.getsize(path)

    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.isfile(fp):
                total_size += os.path.getsize(fp)
    return total_size

def format_size(bytes_size: int) -> str:
    """
    Converts size in bytes to human-readable format.
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.2f} PB"

def get_database_disk_usage(sqlite_path: str = None) -> dict[str, str]:
    """
    Returns a dictionary of database types and their on-disk size.
    """
    disk_usages = {}

    for db_type in DBTypes:
        path = get_db_storage_path(db_type, sqlite_path)
        if path:
            size_bytes = get_dir_size(path)
            size_gbs = size_bytes / 1024 / 1024 / 1024
            disk_usages[db_type.display_name] = size_gbs
        else:
            disk_usages[db_type.display_name] = "Path not found"

    return disk_usages

def create_dataframe_disk_usage(sqlite_db_path: str) -> pd.DataFrame:
    """
    """
    disk_usage = get_database_disk_usage(sqlite_path=sqlite_db_path)
    disk_usage_data = []
    for db, size in disk_usage.items():
        disk_usage_dict = {'database': db, 'size': size}
        disk_usage_data.append(disk_usage_dict)
    df = pd.DataFrame(disk_usage_data)
    return df


if __name__ == '__main__':
    sqlite_db_path = '../databases/reddit_data_20m.db'

    df = create_dataframe_disk_usage(sqlite_db_path)
    disk_usage_excel_path = 'disk_usage.xlsx'
    df.to_excel(disk_usage_excel_path, index=False)
    expand_excel(disk_usage_excel_path)

    # Make plot
    FONT_SIZE = 12
    disk_usage_plot = df.plot.bar(x='database', y='size', color='blue')
    disk_usage_plot.set_ylabel('Size (GB)', fontsize=FONT_SIZE)
    disk_usage_plot.set_title('Storage Usage', fontweight='bold', fontsize=FONT_SIZE+5)
    disk_usage_plot.set_xticklabels(df['database'], rotation=45, ha="right")
    disk_usage_plot.tick_params(axis='x', labelsize=FONT_SIZE)
    disk_usage_plot.tick_params(axis='y', labelsize=FONT_SIZE)
    disk_usage_plot.legend().remove()
    disk_usage_plot.set_xlabel('')

    plt.tight_layout()

    # Save the plot
    os.makedirs('plots', exist_ok=True)
    plt.savefig(f'plots/disk_usage.pdf')

    plt.show()