import argparse
import asyncio
from ops_lib import check_workers, print_json
from shared.worker_health.health import DEFAULT_EXPECTED_WORKERS

DEFAULT_QUEUES = list(DEFAULT_EXPECTED_WORKERS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect worker heartbeats and queue sizes")
    parser.add_argument(
        "--queues",
        nargs="*",
        default=DEFAULT_QUEUES,
        help="queue names to inspect",
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    report = asyncio.run(check_workers(args.queues))
    print_json(report)


if __name__ == "__main__":
    main()
