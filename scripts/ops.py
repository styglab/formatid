import argparse
from ops_lib import (
    build_ops_parser,
    build_workers_summary,
    print_json,
    render_workers_table,
    run_ops_command,
)


def main() -> None:
    parser = build_ops_parser()
    args = parser.parse_args()
    result = run_ops_command(args)

    if args.command == "workers":
        if args.format == "table":
            print(render_workers_table(result))
            return
        if args.verbose:
            print_json(result)
            return
        print_json(build_workers_summary(result))
        return

    print_json(result)


if __name__ == "__main__":
    main()
