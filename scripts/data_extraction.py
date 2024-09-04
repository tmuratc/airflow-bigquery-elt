import requests 

def fetch_from_api(api_url, params=None):
    """
    Fetch data from an API endpoint.
    
    Args:
        api_url (str): The URL of the API endpoint.
        params (dict, optional): A dictionary of parameters to be sent with the request.
        
    Returns:
        tuple: A tuple containing:
            - success (bool): Indicates if the request was successful.
            - data (dict): The JSON response from the API if successful, else an error message.
    """
    try:
        response = requests.get(api_url, params=params, timeout=10)  # Added timeout for better reliability
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        return True, response.json()
    except requests.exceptions.HTTPError as http_err:
        error_message = f"HTTP error occurred: {http_err} - Status Code: {response.status_code}"
        print(error_message)
        return False, {"error": error_message}
    except requests.exceptions.RequestException as req_err:
        error_message = f"Request exception occurred: {req_err}"
        print(error_message)
        return False, {"error": error_message}
    except Exception as e:
        error_message = f"Unexpected error occured while extracting data: {e}"
        print(error_message)
        return False, {"error": error_message}
