import sys
from datetime import datetime

class Logger:
    def __init__(self, filename, mode="w"):
        self.terminal = sys.stdout
        self.log = open(filename, mode, encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        if message.strip():  # Only add timestamp if there's actual content
            now = datetime.now()
            formatted_time = now.strftime("%d %B %Y %H:%M.%S")
            message = f"[{formatted_time}] {message}"
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def close(self):
        self.log.close()