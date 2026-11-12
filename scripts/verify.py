"""Verifica intervalos RA e de impressão nos logs de uma execução em um host."""
import argparse
import json
from pathlib import Path


def verify(directory):
    events = []
    files = sorted(Path(directory).glob("*.jsonl"))
    if not files:
        raise AssertionError("nenhum log JSONL encontrado")
    for path in files:
        events.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
    # monotonic_ns usa o mesmo relógio de kernel no ambiente de referência.
    events.sort(key=lambda event: event["mono_ns"])
    active = {}
    printers = {}
    completed = set()
    entries = 0
    for event in events:
        kind = event["event"]
        assert kind not in {"overlap_violation", "job_error"}, event
        if kind == "cs_enter":
            resource = event["resource"]
            assert resource not in active, f"seções críticas sobrepostas: {event} / {active.get(resource)}"
            active[resource] = event
            entries += 1
        elif kind == "cs_exit":
            previous = active.pop(event["resource"], None)
            assert previous and previous["request_id"] == event["request_id"], event
        elif kind == "print_started":
            assert event["printer"] not in printers, event
            holder = active.get(event["printer"])
            assert holder and holder["node"] == event["computer"], "impressão sem seção crítica"
            printers[event["printer"]] = event["job_id"]
        elif kind == "print_completed":
            assert printers.pop(event["printer"], None) == event["job_id"], event
            assert event["job_id"] not in completed, "impressão duplicada"
            completed.add(event["job_id"])
    return {"events": len(events), "cs_entries": entries, "printed_jobs": len(completed),
            "open_cs": sorted(active), "unfinished_printers": sorted(printers)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    args = parser.parse_args()
    print(json.dumps(verify(args.directory), indent=2))
