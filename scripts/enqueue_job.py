import argparse
import asyncio

from ops_lib import enqueue, parse_json_object, print_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enqueue a task into Redis")
    parser.add_argument("queue_name", help="target Redis queue")
    parser.add_argument("task_name", help="registered task name")
    parser.add_argument(
        "--payload",
        default="{}",
        help='JSON object payload, for example: --payload \'{"source":"cli"}\'',
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=0,
        help="initial attempts count",
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    payload = parse_json_object(args.payload)

    message = asyncio.run(enqueue(args.queue_name, args.task_name, payload, args.attempts))
    print_json(
        {
            "queue_name": message.queue_name,
            "task_id": message.task_id,
            "task_name": message.task_name,
        }
    )


if __name__ == "__main__":
    main()
