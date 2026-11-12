"""Infraestrutura de observabilidade; arquivos não são usados entre nós."""
import json
import os
import time
from pathlib import Path


class EventLog:
    def __init__(self, node_id, directory):
        self.node_id = node_id
        Path(directory).mkdir(parents=True, exist_ok=True)
        self.file = open(Path(directory) / f"{node_id}.jsonl", "a", buffering=1)

    def __call__(self, event, **fields):
        record = {"ts": time.time(), "mono_ns": time.monotonic_ns(),
                  "node": self.node_id, "event": event, **fields}
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        self.file.write(line + "\n")
        if os.environ.get("QUIET") != "1":
            print(line, flush=True)

    def close(self):
        self.file.close()
