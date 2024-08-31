import sys
import os

# Adding the root directory of the project to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tasks.data_extraction import fetch_from_api

def test_install() : 
    api_url = "https://dataeng-interview-project-zomet35f7q-uc.a.run.app/installs" 
    data = fetch_from_api(api_url) 
    print(data)







if __name__ == "__main__": 
    test_install()