import sys
import os

# Adding the root directory of the project to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tasks.data_extraction import fetch_from_api

def test_api_connections(): 

    test_cases = [ 
    {
        "description": "Test API call (installs)", 
        "input": {"api_url": "https://dataeng-interview-project-zomet35f7q-uc.a.run.app/installs" }, 
        "expected_output": True 
    }, 
    {
        "description": "Test API call with parameters (network costs)",
        "input": {"api_url": "https://dataeng-interview-project-zomet35f7q-uc.a.run.app/network_cost", 
                  "params": {"cost_date": "2024-08-30"}},
        "expected_output": True
    }, 
    {
        "description": "Test API call (events)", 
        "input": {"api_url": "https://dataeng-interview-project-zomet35f7q-uc.a.run.app/events" }, 
        "expected_output": True 
    },
    {
        "description": "Test API call (invalid enpoint)",
        "input": {"api_url": "https://dataeng-interview-project-zomet35f7q-uc.a.run.app/invalid_endpoint"},
        "expected_output": False 
    }
    
    ]

    for test in test_cases:
        is_connected, response = fetch_from_api(**test["input"])
        if is_connected == test["expected_output"]:
            print(f"{test['description']}: PASSED")
        else:
            print(f"{test['description']}: FAILED")


if __name__ == "__main__": 
    test_api_connections() 