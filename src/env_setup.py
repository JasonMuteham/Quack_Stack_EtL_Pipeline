import os
import tomllib

def load():
    MOTHERDUCK_DB = os.getenv('MOTHERDUCK_DB')
    MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')

    if MOTHERDUCK_DB is None:
        # Set environment variables
        try:
            with open("config/secret.toml", "rb") as f:
                shush = tomllib.load(f)
        except Exception as e:
            print(f"*** Can not find the secret hiding place! ***\n{e}")
            raise

        os.environ['MOTHERDUCK_DB'] = shush['MOTHERDUCK_DB']
        os.environ['MOTHERDUCK_TOKEN'] = shush['MOTHERDUCK_TOKEN']
