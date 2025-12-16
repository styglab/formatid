import argparse
import asyncio
#
from app.core.logger import get_logger
from app.db.init_db import init_db
from app.jobs.collect_bid_notice_job import run as collect_job
from app.jobs.download_attachment_job import run as download_job
#
#
logger = get_logger("main")

async def main(args):
    logger.info("▶ initializing database...")
    await init_db()

    if args.command == "collect":
        logger.info("▶ running collect job...")
        
        await collect_job(
            inqry_bgn_dt=args.from_dt,
            inqry_end_dt=args.to_dt,
            inqry_div=args.div,
            num_of_rows=args.rows,
        )

    elif args.command == "download":
        logger.info("▶ running download job...")
        await download_job()

    else:
        raise ValueError(f"unknown mode: {args.command}")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Bid notice pipeline jobs"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # collect
    collect = subparsers.add_parser("collect", help="collect bid notices")
    collect.add_argument("--from", dest="from_dt", required=True)
    collect.add_argument("--to", dest="to_dt", required=True)
    collect.add_argument("--rows", type=int, default=100)
    collect.add_argument("--div", default="1")

    # download
    subparsers.add_parser("download", help="download attachments")

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args))

    
