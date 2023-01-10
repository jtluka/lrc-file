import os
import sys
import argparse
import re
import json

from lrc_file.LrcFile import LrcFile
from .run_comparison import compare_lnst_runs


def main():
    args = parse_args()

    run1 = LrcFile(args.file1)
    run2 = LrcFile(args.file2)
    compare_lnst_runs(
        run1, run2, run_info=args.run_info, ignored_params=args.ignored_params
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare two lnst run data files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--info",
        dest="run_info",
        action="store_true",
        help="Switch to turn on/off print of run info",
    )
    parser.add_argument(
        "-I",
        "--ignore-param",
        dest="ignored_params",
        action="append",
        default=[],
        help="Do not consider specified parameter for comparison",
    )
    parser.add_argument("file1", type=str, help="First file to compare")
    parser.add_argument("file2", type=str, help="Second file to compare")

    return parser.parse_args()
