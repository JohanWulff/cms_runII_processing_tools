import os
import json
from glob import glob
from argparse import ArgumentParser


def make_parser():
    parser = ArgumentParser()
    parser.add_argument("--json", type=str, help="josn")
    parser.add_argument("--year", type=str, help="year")
    parser.add_argument("-o","--output_dir", type=str, default="",
                        help="dir, where the processed output.root files are stored. \
If provided, will check if file numbers match.")
    return parser


def count_processed_files(output_dir: str, sample: str):
    directory = output_dir.rstrip('/')+f"/{sample}"
    files = glob(f"{directory}/*.root")
    return len(files)


def main(jsonfile, year, output_dir = ""):
    with open(jsonfile) as j:
        d = json.load(j)

    n_files = 0
    for sample in d[year]:
        print(f"Sample {sample}:")
        path =  d[year][sample]["Path"]
        goodfile = path+"/goodfiles.txt"
        with open(goodfile) as gfile:
            gfiles = sorted([line.rstrip() for line in gfile])
        n_files += len(gfiles)
        if output_dir != "":
            n_processed = count_processed_files(output_dir=output_dir, sample=sample)
            if n_processed != len(gfiles):
                print(f"{n_processed} files in {output_dir.rstrip('/')}/{sample}.")
                print(f"{len(gfiles)} files in {goodfile}!")
        else:
            print(f"{len(gfiles)} files.")
    print("------------")
    print(f"{n_files} in total.")


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    main(jsonfile=args.json, year=args.year, output_dir=args.output_dir)




