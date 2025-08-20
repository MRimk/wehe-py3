import argparse
from datetime import datetime
import subprocess
import os
import json
import shutil

OUTPUT_DIR = "topos/"
PREDEFINED_DIR = "topos/test/"
DOWNLOAD_FILE = "topos/ytopologies.json"
CLEAN_FILE = "topos/ytopologies-clean.json"
QUERY_FILE = "Ytopology_finder_local.sql"


def run_bq_query(output_file):
    try:
        with open(QUERY_FILE, "r") as sql_file, open(output_file, "w") as out_file:
            subprocess.run(
                ["bq", "query", "--use_legacy_sql=false", "--format=json"],
                stdin=sql_file,
                stdout=out_file,
                check=True
            )
    except subprocess.CalledProcessError as error:
        print(f"Command failed with error: {error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("Query executed")


def cleanup_json_file(input_file, output_file):
    unwanted_substrings = [
        'Created measurement-lab',
        'Number of affected rows',
        'Replaced measurement-lab'
    ]
    first_comma_removed = False
    first_comma = ',['
    
    with open(input_file, 'r', encoding='utf-8') as fin, \
         open(output_file, 'w', encoding='utf-8') as fout:
        
        for line in fin:
            if any(phrase in line for phrase in unwanted_substrings):
                stripped = line.lstrip()
                if stripped.startswith('['):
                    line = '[' + '\n'
                    fout.write(line)
                continue
            elif not first_comma_removed and line.startswith(first_comma):
                fout.write(line[1::])
                first_comma_removed = True
            fout.write(line)
    print("Cleanup executed")
    
def copy_files(src_dir: str, dest_dir: str) -> None:
    # Validate source directory
    if not os.path.isdir(src_dir):
        raise ValueError(f"Source directory not found or not a directory: {src_dir}")

    # Ensure destination directory exists
    os.makedirs(dest_dir, exist_ok=True)

    # Iterate through items in source directory
    for entry in os.listdir(src_dir):
        src_path = os.path.join(src_dir, entry)
        # Only copy files (skip subdirectories)
        if os.path.isfile(src_path):
            shutil.copy2(src_path, dest_dir)


def split_json_by_subnet(input_file, date_string):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # data is expected to be a list of lists based on the structure in clean_result.json
    # each sub-list contains items (dictionaries) with a "subnet" field
    # we split them out and write each item to its own file
    # file name pattern: ytopologies-<subnet>-000000000000.json
    
    def sanitize_subnet(subnet_str):
        # Replace characters that could cause issues on most filesystems
        return subnet_str.replace("/", "-")
    
    directory = f"{OUTPUT_DIR}{date_string}"
    os.makedirs(directory, exist_ok=True)
    for sublist in data:
        # Each sublist is itself a list of items
        for item in sublist:
            # Skip if "subnet" is missing
            if 'subnet' not in item:
                continue
            
            subnet = item['subnet']
            sanitized = sanitize_subnet(subnet)
            filename = f"{directory}/ytopologies-{sanitized}.json"
            
            with open(filename, 'w', encoding='utf-8') as out:
                json.dump(item, out, indent=2, ensure_ascii=False)
    # copy files that are used for local testing
    copy_files(PREDEFINED_DIR, directory)

if __name__ == "__main__":
    
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--download', action='store_true', help='download all query output')
    arg_parser.add_argument('--clean', action='store_true', help='cleanup downloaded query output')
    arg_parser.add_argument('--parse', action='store_true', help='split downloaded query into many Y-topologies')
    
    args = arg_parser.parse_args()
   
    if args.download:
        run_bq_query(DOWNLOAD_FILE)
    if args.clean:
        cleanup_json_file(DOWNLOAD_FILE, CLEAN_FILE)
    if args.parse:
        date = datetime.now().strftime("%Y-%m-%d")
        split_json_by_subnet(CLEAN_FILE, date)