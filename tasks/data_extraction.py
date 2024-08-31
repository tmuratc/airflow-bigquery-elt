import requests 

def fetch_from_api (api_url, params = None): 
    """
    Fetch data from an API endpoint.
    
    Args:
        api_url (str): The URL of the API endpoint.
        params (dict, optional): A dictionary of parameters to be sent with the request.
        
    Returns:
        dict: The JSON response from the API.
    """
    try:
        # Send the GET request with or without parameters
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raises an error for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {api_url}: {e}")
        return None 
