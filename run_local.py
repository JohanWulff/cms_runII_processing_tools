#!/eos/home-j/jowulff/mamba_condaforge/bin/python
import json
from glob import glob
import os
from argparse import ArgumentParser
from subprocess import Popen, PIPE


def load_json(sample_json, year=2018):
    with open(sample_json) as f:
        d = json.load(f)
        # select the year
        d = d[str(year)]
    return d
    

def run_sample(infiles, outdir, sample_name, exe, sum_w, year, channel, executable):
    cmd = f"{exe} -i {infiles} -o {outdir} -s {sample_name} --sum_w {sum_w} -y {year} -c {channel} -e {executable}"
    prcs = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, encoding="utf-8")
    out, err = prcs.communicate()
    if err:
        raise RuntimeError(f"running: {cmd} failed with: {err}")
    else:
        print(out)


def get_sample_files(goodfile):
    with open(goodfile) as gfile:
        gfiles = sorted([line.rstrip() for line in gfile])
        if len(gfiles) == 0:
            print(f"Found {len(gfiles)} files in {goodfile}")
    return gfiles


def process(sample_json, outdir, sample_name, exe)