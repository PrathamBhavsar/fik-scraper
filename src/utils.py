def wait_for_element(driver, xpath, timeout=10):
    """Wait for an element to be present in the DOM."""
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException

    try:
        element = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath)))
        return element
    except TimeoutException:
        print(f"Element with XPath '{xpath}' not found within {timeout} seconds.")
        return None

def format_output(data):
    """Format the scraped data for better readability."""
    formatted_data = "\n".join(f"{key}: {value}" for key, value in data.items())
    return formatted_data