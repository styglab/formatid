from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ops.cli import build_ops_parser, run_ops_command
from scripts.ops.common import print_json


def main() -> None:
    parser = build_ops_parser()
    args = parser.parse_args()
    result = run_ops_command(args)
    if result is not None:
        print_json(result)


if __name__ == "__main__":
    main()
