import uproot
from glob import glob
from argparse import ArgumentParser
import os
import re
import json
from collections import defaultdict
from pathlib2 import Path

broken_dict = defaultdict(lambda: defaultdict(list))


sample_2id = {
    "radion_ggf": -4,
    "radion_vbf": -3,
    "graviton_ggf": -2,
    "graviton_vbf": -1,
    "data": 0,
    "DY": 1,
    "TT" : 2,
    "ST" : 3,
    "WJets" : 4,
    "EWK" : 5,
    "TTV" : 6,
    "TTVV" : 7,
    "TTVH" : 8,
    "TTH" : 9,
    "SMH" : 10,
    "SMHH" : 11,
    "WH" : 12,
    "ZH" : 13,
    "VVV" : 14,
    "VV" : 15,
}


def get_sample_id(skim: str):
    if any(y in skim for y in ['2016', '2017', '2018']):
        # data skims
        return 0
    elif "radion" in skim.lower():
        if "vbf" in skim.lower():
            return -4 
        elif "ggf" in skim.lower():
            return -3 
        else:
            raise ValueError(f"Radion (signal) skim found but neither vbf nor ggf in {skim}")
    elif "graviton" in skim.lower():
        if "vbf" in skim.lower():
            return -2 
        elif "ggf" in skim.lower():
            return -1 
        else:
            raise ValueError(f"Graviton (signal) skim found but neither vbf nor ggf in {skim}")
    else:
        if "DY_" in skim:
            return 1
        elif "TT_" in skim:
            return 2
        elif "ST_" in skim:
            return 3
        elif "WJets_" in skim:
            return 4
        elif "EWK" in skim:
            return 5
        elif any(s in skim for s in ["TTZTo", "TTWJets"]):
            return 6
        elif any(s in skim for s in ["TTWW", 'TTWZ', 'TTZZ']):
            return 7
        elif any(s in skim for s in ["TTZH", "TTWH"]):
            return 8
        elif "ttHTo" in skim:
            return 9
        elif any(s in skim for s in ["VBFHTo","GluGluHTo"]):
            return 10
        elif "GGHH_SM" in skim:
            return 11
        elif any(s in skim for s in ["WminusHTo","WplusHTo"]):
            return 12
        elif any(s in skim for s in ["ZH_HTo","ZHTo"]):
            return 13
        elif any(s in skim for s in ["WWW","WWZ","WZZ","ZZZ"]):
            return 14
        elif any(s in skim for s in ["WW", "WZ", "ZZ"]):
            return 15
        else:
            raise ValueError(f"Cannot associate skim {skim} with an ID")


def make_parser():
    parser = ArgumentParser(description="Generate json for submission")
    parser.add_argument("-p", "--paths", type=str, nargs="*",
                        help="Paths containing .root files and goodfiles.txt")
    parser.add_argument("-y", "--year", type=str, help="2016, 2016APV, 17, or 18")
    parser.add_argument("-j", "--json", type=str, required=False, default="",
                        help=".json file to append to. If none, new one will be written.")
    return parser


def parse_goodfile_txt(goodfile:Path,):
    skims_dir = goodfile.absolute().parent
    try:
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
    except:
        print(f"Unable to read goodfiles from {gfile}. Globbing all .root files in skim dir.")
        gfiles = sorted([i for i in skims_dir.glob("*.root")])

    return gfiles


def read_sumw(gfiles:list,):
    global broken_dict
    sum_w = 0
    for j, gfile in enumerate(gfiles):
        print(f"Opening file {j+1}/{len(gfiles)}.\r", end="")
        print(f"Opening file {gfile}.\r", end="")
        sample = gfile.absolute().parent.name
        try:
            with uproot.open(gfile) as data:
                h_eff = data['h_eff'].to_numpy()
            # h_eff is a tuple like: (array([N1, N2,...], dtype=float32),
            #                         array([0,1,2,3,4,5]))
            sum_w += h_eff[0][0]
        except ValueError as ve:
            if ve == "cannot mmap an empty file":
                print(f"\n FILE {gfile} seems to be empty!")
                broken_dict[sample]["Empty"].append(str(gfile))
            continue
        except OSError as oe:
            print(oe)
            print(f"{gfile} seems to be broken")
            broken_dict[sample]["Broken"].append(str(gfile))
            continue
        except uproot.KeyInFileError as upe:
            print(upe)
            print(f"{gfile} seems to be broken")
            broken_dict[sample]["Empty"].append(str(gfile))
        except:
            print("{gfile} seems to be broken")
    return sum_w
    

def main(paths: list, year: str, jsonfile:str = ""):
    global broken_dict
    if jsonfile == "":
        jsonfile = f"./{year}.json"
    d = defaultdict(lambda: defaultdict(dict))
    paths = [i for i in paths if os.path.isdir(i)]
    ## remove data samples
    #paths = [i for i in paths if not f"Run{year}" in i]
    for i, path in enumerate(paths):
        print(f"\nWorking on sample {i+1}/{len(paths)}.\n")
        print(f"\nWorking on sample {path}.\n")
        path = Path(path).absolute()
        skim = path.stem
        sample_id = get_sample_id(skim)
        goodfile = path / 'goodfiles.txt'
        if not os.path.exists(goodfile):
            print(f"{path} does not have a goodfile.txt")
            # in this case we don't know if these files are actually good
            # we just take all .root files in the skims dir. 
            # in that sense this variable name is a misnomer
            # but this script was first written at a time at which the author 
            # assumed there would always be a "goodfiles.txt".. So here we are.
            gfiles = sorted([i for i in path.glob("*.root")])
        else:
            try:
                gfiles = parse_goodfile_txt(goodfile) 
            except:
                print(f"Unable to read {goodfile}")
        n_files = len(gfiles) 
        if year in path.name:
            # Data sample
            d[year][skim]["Path"] = str(path)
            d[year][skim]["Sum_w"] = 1
            d[year][skim]["N_files"] = n_files
            d[year][skim]["Sample_ID"] = sample_id
        else:
            try:
                sum_w = read_sumw(gfiles,)
                d[year][skim]["Path"] = str(path)
                d[year][skim]["Sum_w"] = sum_w
                d[year][skim]["N_files"] = n_files
                d[year][skim]["Sample_ID"] = sample_id
            except:
                print(f"Unable to read weights for skim {skim}")
                continue
    with open(f"./{jsonfile}", "a") as f:
        print(f"writing to ./{jsonfile}. You may want to change the name.")
        json.dump(d, f)
    with open(f"./{year}_broken_stats.json", "w") as f:
        json.dump(broken_dict, f)


if __name__ == "__main__":
    parser = make_parser()
    args = parser.parse_args()
    main(paths= args.paths,
         year= args.year,
         jsonfile=args.json)
