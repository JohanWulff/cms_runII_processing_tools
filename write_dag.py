import json
from glob import glob
import os
from argparse import ArgumentParser
from subprocess import Popen, PIPE
from pathlib2 import Path

from datasamples import data_samples



def make_parser():
    parser = ArgumentParser(description="Submit processing of LLR Samples to DNN inputs")
    parser.add_argument("-s", "--submit_base", type=str, 
                        help="Base dir to submit from")
    parser.add_argument("-y", "--year", type=str, 
                        help="2016, 2017 or 2018")
    parser.add_argument("-j", "--json", type=str,
                        help="JSON File containing paths to samples")
    parser.add_argument("-b", '--broken', type=str, default="", required=False,
                        help=".txt file with broken files.")
    return parser
                            

def checkmake_dir(path):
    if not os.path.exists(path):
        print(f"{path} does not exist.")
        print("Shall I create it now?")
        yn = input("[y/n] ?")
        if yn.strip().lower() == 'y':
            print('Creating dir(s)!')
            os.makedirs(path)
        else:
            raise ValueError(f"{path} does not exist")


def return_executable():
    exec_str = ('#!/usr/bin/bash\n'
    'usage() { echo "Usage: $0 [-i <input file> ] [-o <outfile>] [-s sample] [-w sum_w] [-y year]" 1>&2; exit 1; }\n'
    'while getopts "i:o:s:w:y:" opt; do\n'
    '    case "$opt" in\n'
    '        i) INFILE=$OPTARG\n'
    '            ;;\n'
    '        o) OUTFILE=$OPTARG\n'
    '            ;;\n'
    '        s) SAMPLE=$OPTARG\n'
    '            ;;\n'
    '        w) SUM_W=$OPTARG\n'
    '            ;;\n'
    '        y) YEAR=$OPTARG\n'
    '            ;;\n'
    '        *)\n'
    '            echo "Invalid argument $OPTARG";\n'
    '            usage\n'
    '            exit 1\n'
    '    esac\n'
    'done\n'
    'CMSSW_SRC="/eos/user/j/jowulff/res_HH/giles_data_proc/CMSSW_10_2_15/src"\n'
    'EXE="/eos/user/j/jowulff/res_HH/giles_data_proc/CMSSW_10_2_15/bin/slc7_amd64_gcc700/RunLoop"\n'
    'cd $CMSSW_SRC || exit 1\n'
    'cmsenv && cd - || exit 1\n'
    'echo "running: ${EXE} -i $INFILE -o $OUTFILE --sample $SAMPLE --sum_w $SUM_W -y $YEAR"\n'
    '${EXE} -i $INFILE -o $OUTFILE --sample $SAMPLE --sum_w $SUM_W -y $YEAR || rm $OUTFILE\n'
    'exit 0\n')
    return exec_str


def return_subfile(executable, year, sample):
    arguments = f"-i $(INFILES) -o $(OUTFILE) -s $(SAMPLE) -w $(SUM_W) -y $(YEAR)"
    file_str = f"executable={executable}\n\
\n\
output                = $(ClusterId).$(ProcId).out\n\
error                 = $(ClusterId).$(ProcId).err\n\
log                   = $(ClusterId).$(ProcId).log\n\
output_destination    = root://eosuser.cern.ch//eos/user/j/jowulff/res_HH/Condor_out/dnn_data_{year}/{sample}\n\
MY.XRDCP_CREATE_DIR   = True\n\
\n\
MY.JobFlavour = \"espresso\"\n\
Arguments = {arguments}\n\
queue"
    return file_str


def parse_goodfile_txt(goodfile:Path,):
    skims_dir = goodfile.absolute().parent
    with open(goodfile) as gfile:
        gfiles = sorted([Path(line.rstrip()) for line in gfile])
        if len(gfiles) == 0:
            print(f"Found 0 files in {goodfile}. Globbing all .root files in skim dir.")
            # goodfiles.txt is empty: just glob all .root files in 
            # skims dir and hope they're good
            gfiles = sorted([i for i in skims_dir.glob("*.root")])
        else:
            # check if the paths have been updated
            if gfiles[0].parent != skims_dir:
                # if not stick the filename on the end of the provided path
                # and hope for the best
                gfiles = [skims_dir / i.name for i in gfiles]
    return [str(gfile) for gfile in gfiles]


def main(submit_base_dir: str, 
         year: str,
         sample_json: str,
         broken_files: str=""):
    #executable = "/eos/user/j/jowulff/res_HH/giles_data_proc/\
#CMSSW_10_2_15/src/cms_runII_data_proc/highLevel/executable.py"
    shellscript = "/eos/user/j/jowulff/res_HH/cms_runII_processing_tools/executable.sh"
    # check if it starts with /afs
    if not submit_base_dir.startswith("/afs"):
        raise ValueError("Submission must happen from /afs!")
    checkmake_dir(submit_base_dir)
    # copy executables to /afs. Condor cannot access /eos at the time of writing
    for script in [shellscript]:
        prcs = Popen(f"cp {script} {submit_base_dir}", 
                                shell=True, stdin=PIPE, stdout=PIPE,
                                encoding='utf-8')
        out, err = prcs.communicate()
        if err:
            print(err)
            raise ValueError(f"Unable to move {script} to {submit_base_dir}")

    #afs_exe = submit_base_dir+"/executable.py"
    afs_shscript = submit_base_dir+"/executable.sh"

    with open(sample_json) as f:
        d = json.load(f)
        # select the year
        d = d[year]

    if not broken_files=="":
        with open(broken_files) as f:
            broken_list = [line.rstrip() for line in f] 
    
    for i, sample in enumerate(d):
        print(f"Creating submission dir and writing dag \
files for sample ({i+1}/{len(d)})\r", end="")
        # data samples are channel-dependant
        if "Run" in sample:
            continue
        submit_dir = submit_base_dir.rstrip("/")+f"/{sample}"
        if not os.path.exists(submit_dir):
            os.mkdir(submit_dir)
        dagfile = submit_dir+f"/{sample}.dag"
        submitfile = submit_dir+f"/{sample}.submit"
        path = d[sample]["Path"]
        sum_w = d[sample]["Sum_w"]
        goodfile = path+"/goodfiles.txt"
        if not os.path.exists(goodfile):
            print(f"{sample} does not have a goodfile.txt at \
{path}")
            gfiles = glob(d[sample]["Path"]+"/*.root")
        else:
            gfiles = parse_goodfile_txt(Path(goodfile))
        # filter files for broken files
        if not broken_files == "":
            gfiles = [file for file in gfiles if file not in broken_list]
        filechunks = [gfiles[i:i+100] for i in range(0, len(gfiles), 100)]
        if not os.path.exists(dagfile):
            with open(dagfile, "x") as dfile:
                #for chunk in filechunks:
                    #print(f"JOB {chunk[0]} {submitfile}", file=dfile)
                    #print(f'VARS {chunk[0]} INFILES="{" ".join(chunk)}" \
    #OUTDIR="{outdir.rstrip("/")+f"/{sample}"}" EXE="{afs_shscript}" SAMPLE="{sample}" \
    #SUM_W="{sum_w}" YEAR="{year}" CHANNEL="{channel}"', file=dfile)
                for file in gfiles:
                    print(f"JOB {file.split('/')[-1]} {submitfile}", file=dfile)
                    print(f'VARS {file.split("/")[-1]} INFILES="{file}" \
OUTFILE="{file.split("/")[-1]}" SAMPLE="{sample}" SUM_W="{sum_w}" \
YEAR="{year}"', file=dfile)
            submit_string = return_subfile(executable=afs_shscript,
                                        year=year,
                                        sample=sample)
        else:
            print(f"\n {dagfile} already exists.. Not creating new one \n")

        if not os.path.exists(submitfile):
            submit_string = return_subfile(executable=afs_shscript,
                                        year=year,
                                        sample=sample)
            with open(submitfile, "x") as subfile:
                print(submit_string, file=subfile)
        else:
            print(f"\n {submitfile} already exists.. Not creating new one \n")

if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    main(submit_base_dir=args.submit_base,
         year=args.year,
         sample_json=args.json,
         broken_files=args.broken)
