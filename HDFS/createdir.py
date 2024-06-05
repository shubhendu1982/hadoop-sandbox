from pywebhdfs.webhdfs import PyWebHdfsClient

# Create the client
client = PyWebHdfsClient(host="localhost", port=9870, user_name="sandbox")

# Create the directory 'test_data'
client.make_dir('/user/sandbox/test_data')

# Verify the directory was created by listing the contents
listing = client.list_dir('/user/sandbox')
print(listing)

