from enum import Enum, auto

class DBTypes(Enum):
    MYSQL = "mysql"
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MONGODB = "mongodb"

    @property
    def display_name(self) -> str:
        return {
            "mysql": "MySQL",
            "sqlite": "SQLite",
            "postgresql": "PostgreSQL",
            "mongodb": "MongoDB"
        }.get(self.value, f"Unknown {self.value}")

    def is_sql(self) -> bool:
        return self.value in ["mysql", "sqlite", "postgresql"]

class DBType:
    def __init__(self, db_type: DBTypes, name_suffix: str, max_rows: int = None):
        if isinstance(db_type, DBTypes):
            self.db_type = db_type
            self.name_suffix = name_suffix
            self.max_rows = max_rows
        else:
            raise ValueError(f"Invalid DBType: {db_type}")
    def is_type(self, db_type: DBTypes) -> bool:
        return self.db_type == db_type

    def get_type(self) -> DBTypes:
        return self.db_type

    def to_string(self) -> str:
        return self.db_type.value if self.db_type else None

    def __str__(self):
        return self.to_string()

    def to_string_capitalized(self) -> str:
        return self.db_type.display_name if self.db_type else "Unknown"

    def is_sql(self) -> bool:
        return self.db_type.is_sql()

    @property
    def display_name(self) -> str:
        return f'{self.db_type.display_name}_{self.name_suffix}'
