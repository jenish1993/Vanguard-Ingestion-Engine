import pandas as pd
import os

def split_csv(input_file: str, output_dir: str, rows_per_file: int = 1000):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    # Read and split the file in chunks to optimize memory
    chunk_container = pd.read_csv(input_file, chunksize=rows_per_file)
    
    for i, chunk in enumerate(chunk_container):
        output_file = os.path.join(output_dir, f"world_energy_consumption_batch_{i+1}.csv")
        chunk.to_csv(output_file, index=False)
        print(f"Generated: {output_file} ({len(chunk)} rows)")

# Execution variables
source_csv = "Data/world_energy_consumption.csv"  # Replace with your actual filename
target_folder = "Data/pipeline_ingress_batches"

split_csv(source_csv, target_folder, rows_per_file=1000)