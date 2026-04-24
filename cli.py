import argparse

from execution.job import CompareJob
from execution.runner import CompareJobRunner


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cmp")
    parser.add_argument("--src", required=True)
    parser.add_argument("--tgt", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=1000)

    args = parser.parse_args()

    job = CompareJob(
        src_key=args.src,
        tgt_key=args.tgt,
        workers=args.workers,
        batch_size=args.batch_size,
    )

    runner = CompareJobRunner(job)
    runner.run()


if __name__ == "__main__":
    main()