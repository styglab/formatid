import sys
from ops_lib import print_json, run_compose_smoke_test


def main() -> None:
    print_json(run_compose_smoke_test())


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
