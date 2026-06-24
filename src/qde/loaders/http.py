import time
import requests

def get_with_requests(url, params, max_retries=4):
    """"
    Make a get request with retries on temporary failure.

    Retries on rate limits (429) and server errors (5xx)
    using exponential backoff. Raises immediately on client
    errors (4XX other than 429) since retrying won't help.

    Arg:
        url (str): the endpoint URL.
        params (dict): query parameters.
        max_retries (int): maximum number of retries before giving up.

    Returns:
        requests.Response: the successful response.

    Raises:
          ValueError: if all retries are exhausted or a permanent error occurs.
    """
    for attempt in range(max_retries):
        response = requests.get(url, params=params)

        # Success
        if response.status_code == 200:
            return response

        # Rate limited or server error - wait and retry
        if response.status_code == 429 or response.status_code >= 500:
            wait = 2 ** attempt     # 1, 2, 4, 8
            time.sleep(wait)
            continue

        # Permanent client error - don't retry
        raise ValueError(f"Request failed with status code {response.status_code}: {url}")

    # Exhausted all retries
    raise ValueError(f"Request failed after {max_retries} retries: {url}")