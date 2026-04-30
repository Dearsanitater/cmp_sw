import argparse
import time

from execution.job import CompareJob
from execution.runner import CompareJobRunner


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cmp", nargs="?")
    parser.add_argument("--src", "-src", dest="src", required=True)
    parser.add_argument("--src-schema", dest="src_schema", default=None)
    parser.add_argument("--tgt", "-tgt", dest="tgt", required=True)
    parser.add_argument("--tgt-schema", dest="tgt_schema", default=None)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--skip-data", action="store_true")
    parser.add_argument("--tgt-uppercase", "--ignore-case", dest="tgt_uppercase", action="store_true")

    args = parser.parse_args()

    job = CompareJob(
        src_key=args.src,
        src_schema=args.src_schema,
        tgt_key=args.tgt,
        tgt_schema=args.tgt_schema,
        workers=args.workers,
        batch_size=args.batch_size,
        compare_rows=not args.skip_data,
        tgt_uppercase=args.tgt_uppercase,
    )
    start=time.monotonic()
    CompareJobRunner(job).run()
    print(f'对比总计消耗: {time.monotonic() - start:.2f}秒')

if __name__ == "__main__":
    main()
