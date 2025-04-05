from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
import logging
import json
from webdriver_manager.chrome import ChromeDriverManager
from send_data_to_aws import send_data_to_aws
from datetime import datetime

logging.basicConfig(level=logging.INFO)
options = Options()
options.add_argument('--headless')
options.add_argument("--no-sandbox")
options.binary_location = "/usr/bin/chromium"
driver = webdriver.Chrome(
    service=Service("/usr/bin/chromedriver"),
    options=options
)

def save_data_to_json(data, filename="/german_scholarships_data.json"):
    """
    Saves scholarship data to a JSON file.

    Args:
        data (list): A list of dictionaries containing scholarship data.
        filename (str): The name of the JSON file to save the data to.
    """
    with open(filename, mode="w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

def extract_origin_numbers():
    """
    Extracts all possible country of origin options (origin numbers) from the dropdown on the given URL.

    Args:
        url (str): The URL of the webpage containing the dropdown.

    Returns:
        list: A list of origin numbers (integers).
    """
    
    try:
        dropdown = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "s-land"))
        )

        # Extract all <option> elements within the dropdown
        options = dropdown.find_elements(By.TAG_NAME, "option")

        # Collect the values (numbers) from the options
        origin_numbers = []

        origin_mapping = {}
        iteration_count=0
        max_iterations=1
        for option in options:
            if iteration_count >= max_iterations:
                break
            value = option.get_attribute("value")
            text = option.get_attribute('innerText').strip()
            if not value or not text:
                continue
            if value:
                origin_mapping[int(value)] = text
                origin_numbers.append(int(value))
            iteration_count+=1
        return origin_numbers, origin_mapping
    
    except Exception as e:
        print("Something went wrong:", e)

def extract_results():
    """
    Extracts search results
    """
    results = driver.find_elements(By.CSS_SELECTOR, "div.stipdb-results ul.resultlist li.entry.clearfix h2 a")

    for index, result in enumerate(results):
        print(f"Result {index + 1}:")
        print("Text:", result.text)
        print("Link:", result.get_attribute("href"))
    return len(results)

def extract_result_links():
    """
    Extracts links of each search result
    """
    result_links = []
    results = driver.find_elements(By.CSS_SELECTOR, "div.stipdb-results ul.resultlist li.entry.clearfix h2 a")
    for result in results:
        result_links.append(result.get_attribute("href"))
    return result_links

def extract_data_from_page():
    """
    Extracts data from each result link
    """
    data = {}
    try:
        # Example: Extract the title
        data["Name"] = driver.find_element(By.CSS_SELECTOR, "div#content div#ifa-stipendien-detail div.sub-navi.clearfix h2.title").text
    except NoSuchElementException:
        data["Name"] = "N/A"

    try:
        # Example: Extract the description
        data["Overview"] = driver.find_element(By.CSS_SELECTOR, "#ueberblick").text
    except NoSuchElementException:
        data["Overview"] = "N/A"

    try:
        application_requirements = driver.find_element(By.CSS_SELECTOR, "#voraussetzungen")
        
        # Example: Extract the requirements
        data["Application Requirements"] = application_requirements.get_attribute("innerText")
    except NoSuchElementException:
        data["Application Requirements"] = "N/A"

    try:
        application_procedure = driver.find_element(By.CSS_SELECTOR, "#prozess")
        # Example: Extract the procedure
        data["Application Procedure"] = application_procedure.get_attribute("innerText")
    except TimeoutException:
        print(f"Timeout while processing {link}. Skipping...")
    except NoSuchElementException:
        data["Application Procedure"] = "N/A"
    except Exception as e:
        print(f"Unexpected error processing {link}: {e}")

    try:
        application_instructions = driver.find_element(By.CSS_SELECTOR, "#bewerbung")
        
        # Example: Extract the instructions
        data["Application Instructions"] = application_instructions.get_attribute("innerText")
    except TimeoutException:
        print(f"Timeout while processing {link}. Skipping...")
    except NoSuchElementException:
        data["Application Instructions"] = "N/A"
    except Exception as e:
        print(f"Unexpected error processing {link}: {e}")

    try:
        contact = driver.find_element(By.CSS_SELECTOR, "#kontaktberatung")

        # Example: Extract the contact information
        data["Contact Information"] = contact.get_attribute("innerText")
    except TimeoutException:
        print(f"Timeout while processing {link}. Skipping...")
    except NoSuchElementException:
        data["Contact Information"] = "N/A"
    except Exception as e:
        print(f"Unexpected error processing {link}: {e}")
        

    return data

def scrape_callable():
    base_url="https://www2.daad.de/deutschland/stipendium/datenbank/en/21148-scholarship-database/"
    status_range = range(3,4)
    status_mapping = {
        1: "Undergraduate",
        2: "Postdoctoral researchers",
        3: "Graduates",
        4: "PhD",
        5: "Faculty"
    }
    #Connect to the website
    driver.get(base_url)
    print(driver.title) #Check

    #Accept Cookies
    try:
        accept_all_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.qa-cookie-consent-accept-all"))
        )
        accept_all_button.click()
        print("Cookies accepted.")
    except Exception as e:
        print("No cookies screen found or could not accept cookies:", e)

    #Extract all possible countries of origin
    origins, origin_mapping=extract_origin_numbers()
    print(origins)

    scholarships_data=[]

    for status in status_range:
        for origin in origins:
            # Construct the URL with the current status and origin
            url = f"{base_url}?status={status}&origin={origin}&subjectGrps=&daad=&intention=1&q=&back=1"
            
            # Access the URL
            driver.get(url)

            links=set()
            count=0
            while True:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.stipdb-results"))
                )
                count+=extract_results()

                links.update(extract_result_links())
                try:
                    next_button = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.clearfix.pagination-wrapper ul.pagination li:last-child a"))
                        )
                    driver.execute_script("arguments[0].scrollIntoView(true);", next_button)

                    logging.info(f"Next button text: {next_button.text}")
                    logging.info(f"Next button href: {next_button.get_attribute('href')}")
                    logging.info(f"Next button class: {next_button.get_attribute('class')}")
                    # Check if the "Next" button is enabled and clickable
                    if "disabled" in next_button.get_attribute("class"):
                        print("Reached the last page.")
                        break
                    driver.execute_script("arguments[0].click();", next_button)
                except TimeoutException:
                    # Handle the case where the "Next" button is not found
                    print("No pagination found. Only one page of results.")
                    break
                except ElementClickInterceptedException:
                    # If the click is intercepted, hide the chat toggle button and try again
                    driver.execute_script("document.querySelector('button.chat-toggle-button.u-circle.u-text-structure-fix.qa-chat-toggle-button.u-text-center.btn--theme-none').style.display = 'none';")
                    driver.execute_script("arguments[0].click();", next_button)
            print(f"Total count {count} for {origin} and {status}")   
            print(len(links))

            # Iterate through each result link
            for link in links:
                time.sleep(2)
                try:
                    # Open the result page in a new tab
                    driver.execute_script("window.open(arguments[0]);", link)
                    driver.switch_to.window(driver.window_handles[1])  # Switch to the new tab

                    # Wait for the page to load
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div#content"))
                    )

                    # Extract data from the result page
                    data = extract_data_from_page()

                    data["Country of Origin"]=origin_mapping.get(origin, "Unknown Origin")
                    data["Status"]=status_mapping.get(status, "Unknown Status")

                    scholarships_data.append(data)

                    # Close the current tab and switch back to the main tab
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])

                except Exception as e:
                    print(f"Error processing {link}: {e}")
                    if len(driver.window_handles) > 1:
                        driver.close()  # Close the tab if it was opened
                        driver.switch_to.window(driver.window_handles[0])

    # Save the data to a JSON file
    # save_data_to_json(scholarships_data)
    send_data_to_aws(scholarships_data, "scholarship-data-bdm", f"{datetime.now().strftime("%Y-%m-%d_%H-%M")}_german_scholarships_data.json")

    # Close the WebDriver
    driver.quit()
