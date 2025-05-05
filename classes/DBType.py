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

class DBType:
    def __init__(self, db_type: DBTypes = None):
        self.db_type = db_type

    def set_type(self, db_type: DBTypes):
        self.db_type = db_type

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
