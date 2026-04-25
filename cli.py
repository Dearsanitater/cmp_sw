import argparse

from execution.job import CompareJob
from execution.runner import CompareJobRunner


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cmp")
    parser.add_argument("-src", required=True)
    parser.add_argument("--src_dbn",type=str,default=None)
    parser.add_argument("--src_schema", type=str, default=None)
    parser.add_argument("-tgt", required=True)
    parser.add_argument("--tgt_dbn", type=str, default=None)
    parser.add_argument("--tgt_schema", type=str, default=None)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=1000)

    args = parser.parse_args()

    job = CompareJob(
        src_key=args.src,
        src_dbn=args.src_dbn,
        src_schema=args.src_schema,
        tgt_key=args.tgt,
        tgt_dbn=args.tgt_dbn,
        tgt_schema=args.tgt_schema,
        workers=args.workers,
        batch_size=args.batch_size,
    )

    runner = CompareJobRunner(job)
    runner.run()


if __name__ == "__main__":
    main()