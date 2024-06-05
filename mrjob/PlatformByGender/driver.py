import csv
from platform_by_gender import PlatformByGender
from mrjob.job import MRJob
import subprocess
import tempfile

# Function to run the MapReduce job
def run_mrjob(input_file, output_dir):
    # Create a temporary local file to store the intermediate output
    temp_output_file = tempfile.NamedTemporaryFile(delete=False)
    temp_output_filename = temp_output_file.name

    # Run the MRJob with HDFS input and temporary local output
    mr_job = PlatformByGender(args=['-r', 'hadoop', input_file, '--output-dir', output_dir])

    with mr_job.make_runner() as runner:
        runner.run()

        # Read the intermediate output from HDFS
        subprocess.run(['hdfs', 'dfs', '-getmerge', f'{output_dir}/part-*', temp_output_filename])

        # Write the final output to the desired HDFS path
        final_output_path = f'{output_dir}/output_gender.csv'
        with open(temp_output_filename, 'r') as infile:
            with open('final_output.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Gender', 'Platform', 'Count'])

                # Read each line and split it by tab ('\t') to separate key and value
                for line in infile:
                    key, value = line.strip().split('\t')
                    gender, platform = eval(key)  # Parse key
                    count = int(value)  # Convert value to integer
                    writer.writerow([gender, platform, count])

        # Put the final output CSV to HDFS
        subprocess.run(['hdfs', 'dfs', '-put', 'final_output.csv', final_output_path])

if __name__ == '__main__':
    input_file = 'hdfs:///test_data/input.csv'  # Your input HDFS CSV file
    output_dir = 'hdfs:///output_data/'  # Your output HDFS directory
    run_mrjob(input_file, output_dir)
