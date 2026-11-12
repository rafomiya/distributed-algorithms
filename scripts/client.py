"""Cliente de controle usando apenas a biblioteca padrão."""
import argparse
import json
from urllib.request import Request, urlopen


def post(node, path, data):
    req = Request(f"http://localhost:{8000 + int(node)}/{path}",
                  data=json.dumps(data).encode(), headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=3) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=int, default=1)
    parser.add_argument("--printer", type=int, default=1)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--document", default="documento.txt")
    parser.add_argument("--partition", nargs=2)
    parser.add_argument("--heal", action="store_true")
    parser.add_argument("--count", type=int, default=3)
    args = parser.parse_args()
    if args.partition:
        groups = [set(map(int, group.split(","))) for group in args.partition]
        if groups[0] & groups[1] or any(i < 1 or i > args.count for group in groups for i in group):
            parser.error("grupos devem ser disjuntos e conter participantes válidos")
        for group, other in ((groups[0], groups[1]), (groups[1], groups[0])):
            for node in sorted(group):
                print(post(node, "fault", {"blocked_peers": [f"node{i}" for i in sorted(other)]}))
    elif args.heal:
        for node in range(1, args.count + 1):
            print(post(node, "fault", {"blocked_peers": [], "loss": 0, "delay_ms": 0, "types": []}))
    else:
        print(json.dumps(post(args.node, "jobs", {"printer": f"printer{args.printer}",
            "pages": args.pages, "document": args.document}), indent=2))


if __name__ == "__main__":
    main()
