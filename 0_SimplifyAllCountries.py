import os
import pandas as pd
from tqdm import tqdm

input_file = "allCountries.txt"
output_dir = "data/"

os.makedirs(output_dir, exist_ok=True)
columns = [
    "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
    "feature_class", "feature_code", "country_code", "cc2", "admin1_code",
    "admin2_code", "admin3_code", "admin4_code", "population", "elevation",
    "dem", "timezone", "modification_date"
]

def process_chunk_and_write(chunk):
    buffers = {chr(i): [] for i in range(65, 91)}
    buffers["Misc"] = []

    for _, row in chunk.iterrows():
        first_letter = str(row["name"])[0].upper() if pd.notnull(row["name"]) else "Misc"
        buffer_key = first_letter if 'A' <= first_letter <= 'Z' else "Misc"
        buffers[buffer_key].append("\t".join(map(str, row.tolist())))

    for buffer_key, rows in buffers.items():
        if rows:
            file_name = f"{buffer_key}.txt"
            output_path = os.path.join(output_dir, file_name)
            with open(output_path, "a", encoding="utf-8") as f:
                f.write("\n".join(rows) + "\n")

chunk_size = 100000
reader = pd.read_csv(input_file, sep='\t', header=None, names=columns, low_memory=False, chunksize=chunk_size)

print("Counting total rows...")
with open(input_file, "r", encoding="utf-8") as f:
    total_rows = sum(1 for _ in f)

print(f"Total rows: {total_rows}\n")

progress = tqdm(total=total_rows, desc="Processing", unit="rows")
for chunk in reader:
    process_chunk_and_write(chunk)
    progress.update(len(chunk))

progress.close()
print(f"Files have been successfully split and saved in {output_dir}.")
