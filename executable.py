#!/eos/home-j/jowulff/mambaforge/bin/python
from argparse import ArgumentParser
from subprocess import Popen, PIPE


def make_parser():
    parser = ArgumentParser(description="Run executable.sh for N files")
    parser.add_argument('-i', '--infiles', type=str, default=None, nargs='*')
    parser.add_argument('-o', '--outpath', type=str, default=None)
    parser.add_argument('-s', '--sample', type=str, default=None,
                        help="Sample Name. Used to create dir to store at.")
    parser.add_argument('--sum_w', type=float, default=None,
                        help="Sum of Weights for given sample.")
    parser.add_argument('-y', '--year', type=str, default=None,
                        help="2016, 2017 or 2018")
    parser.add_argument('-c', '--channel', type=str, default=None,
                        help="tauTau, muTau or eTau")
    parser.add_argument("-e", "--executable", type=str, default=None, 
                        help="Executable that will be used for the processing.")
    return parser


def main(exe, infiles, outpath, sample, sum_w,  year, channel):
    for file in infiles:
        outfile = outpath.rstrip('/')+"/"+file.split('/')[-1]
        command = f"{exe} -i {file} -o {outfile} -c {channel} -y {year} -w {sum_w} -s {sample}"
        print("Running:")
        print(command)
        prcs = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, encoding="utf-8")
        out, err = prcs.communicate()
        if err:
            raise RuntimeError(f"running: {command} failed with: {err}")
        else:
            print(out)


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    main(exe=args.executable,
         infiles=args.infiles,
         outpath=args.outpath,
         sample=args.sample,
         sum_w=args.sum_w,
         year=args.year,
         channel=args.channel)
