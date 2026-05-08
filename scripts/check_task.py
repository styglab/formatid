import argparse
import asyncio
from ops_lib import fetch_task, print_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect stored task lifecycle status")
    parser.add_argument("task_id", help="task id to inspect")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    result = asyncio.run(fetch_task(args.task_id))
    print_json(result)


if __name__ == "__main__":
    main()
