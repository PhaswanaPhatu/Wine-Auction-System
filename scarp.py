from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time


def setup_driver():
    """Setup Firefox driver with automatic driver management"""
    options = Options()
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/csv")

    service = Service(GeckoDriverManager().install())
    driver = webdriver.Firefox(service=service, options=options)
    return driver


def scrape_data_after_click(driver):
    """Scrape data after clicking a button"""
    # Get the page source and parse it with BeautifulSoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Example: Assuming you want to extract winery names and descriptions
    winery_elements = soup.find_all("div", class_="col-xs-12 col-sm-12 col-md-12")  # Adjust this selector as needed
    wineries_data = []

    for element in winery_elements:
        name = element.find("a",class_='h2').text.strip()
        location = element.findNext("a",class_='subheading').text.strip() if element else "N/A"
        contact_info =  element.findNext("a",class_='icon icon_phone').text.strip()
        link = element.findNext("a", class_='icon icon_url').get("href")
        print(element)
        wineries_data.append({
            'name': name,
            'location': location,
            'contact_info' : contact_info,
            'link': link
        })
        print(name)


    return wineries_data


def scrape_wineries():
    driver = setup_driver()
    all_wineries = []

    try:
        # Navigate to the initial URL
        url = "https://wine.co.za/winery/"
        driver.get(url)

        # Wait for the page to load
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "white")))  # Wait until buttons are present

        # Click each button A-Z
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ": #DEFGHIJKLMNOPQRSTUVWXYZ
            button_id = f"btn_{letter}"
            print(f"Clicking button {letter}...")

            # Find the button and click it
            button = wait.until(EC.element_to_be_clickable((By.ID, button_id)))
            button.click()

            # Wait for the page to load new content
            time.sleep(2)  # Adjust as necessary for the page to load

            # Scrape data after clicking
            wineries_data = scrape_data_after_click(driver)
            all_wineries.extend(wineries_data)

            # Optionally go back to the original page
            driver.back()
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "white")))  # Wait until buttons are present again

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Save data to CSV
        if all_wineries:
            df = pd.DataFrame(all_wineries)
            filename = 'C:\\Users\\kwind\\PycharmProjects\\Big_Data_Project_2\\wine_producers.csv'  # Ensure this path exists
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"Successfully saved {len(all_wineries)} wineries to {filename}")

        driver.quit()


if __name__ == "__main__":
    scrape_wineries()
