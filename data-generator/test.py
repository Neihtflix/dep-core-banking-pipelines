import os
from dotenv import load_dotenv, find_dotenv

# find_dotenv() automatically walks up the folder tree to find the .env file
load_dotenv()

# Test if it worked
print(os.getenv("POSTGRES_USER"))
print(os.getenv("POSTGRES_PASSWORD"))
print(os.getenv("POSTGRES_HOST"))
print(os.getenv("POSTGRES_PORT"))
print(os.getenv("POSTGRES_DB"))
