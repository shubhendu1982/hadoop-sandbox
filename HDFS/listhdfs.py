from pywebhdfs.webhdfs import PyWebHdfsClient
client = PyWebHdfsClient(host="localhost", port=9870, user_name="sandbox")
listing = client.list_dir("/user/sandbox")
print(listing)
