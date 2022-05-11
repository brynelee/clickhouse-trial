import hashlib

pwString = hashlib.sha256("spark-master".encode('utf-8')).hexdigest()
print(pwString)
