from pywebhdfs.webhdfs import PyWebHdfsClient

# Create the client
client = PyWebHdfsClient(host="localhost", port=9870, user_name="sandbox")

# Local file path
local_file_path = 'input.csv'

# HDFS file path
hdfs_file_path = '/user/sandbox/test_data/input.csv'

# Read the local CSV file
with open(local_file_path, 'rb') as file_data:
    # Upload the file to HDFS
    client.create_file(hdfs_file_path, file_data)

# Verify the file was uploaded by listing the contents of the directory
listing = client.list_dir('/user/sandbox/test_data')
print(listing)

