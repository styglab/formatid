import argparse
import asyncio
from ops_lib import inspect_dlq, print_json
from shared.tasking.catalog import list_queue_names


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect DLQ messages by queue")
    parser.add_argument(
        "--queues",
        nargs="*",
        default=list(list_queue_names()),
        help="source queue names to inspect before applying the DLQ suffix",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="maximum messages to preview per DLQ queue",
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    result = asyncio.run(inspect_dlq(args.queues, limit=args.limit))
    print_json(result)


if __name__ == "__main__":
    main()
