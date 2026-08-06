import argparse, json
from pathlib import Path
from .config import PipelineConfig
from .pipeline import run_pipeline

def main():
    parser=argparse.ArgumentParser(description="Run the 6E-style trade invoice CSV ETL pipeline")
    parser.add_argument("--input-path",type=Path,required=True)
    parser.add_argument("--output-dir",type=Path,default=Path("output"))
    args=parser.parse_args()
    print(json.dumps(run_pipeline(args.input_path,args.output_dir,PipelineConfig()),indent=2))
if __name__=="__main__": main()
