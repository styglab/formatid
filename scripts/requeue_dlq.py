import argparse
import asyncio
from ops_lib import print_json, requeue_dlq_messages
from core.runtime.task_runtime.catalog import list_queue_names


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Requeue messages from DLQ back to the source queue")
    parser.add_argument("queue_name", choices=list(list_queue_names()), help="source queue name")
    parser.add_argument(
        "--task-id",
        help="requeue only the matching task id from the DLQ",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="number of DLQ messages to requeue when --task-id is not provided",
    )
    parser.add_argument(
        "--keep-attempts",
        action="store_true",
        help="preserve attempts instead of resetting them to zero",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="bypass catalog-based DLQ requeue limits",
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    result = asyncio.run(
        requeue_dlq_messages(
            queue_name=args.queue_name,
            task_id=args.task_id,
            count=args.count,
            keep_attempts=args.keep_attempts,
            force=args.force,
        )
    )
    print_json(result)


if __name__ == "__main__":
    main()
