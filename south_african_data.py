from selenium import webdriver
from bs4 import BeautifulSoup
import csv
import time
import requests

# List of URLs to scrape
urls = [
    "https://www.restaurants.co.za/gauteng/all",
    "https://www.restaurants.co.za/eastern-cape/all",
    "https://www.restaurants.co.za/freestate/all",
    "https://www.restaurants.co.za/kwazulu-natal/all",
    "https://www.restaurants.co.za/limpopo/all",
    "https://www.restaurants.co.za/mpumalanga/all",
    "https://www.restaurants.co.za/northern-cape/all",
    "https://www.restaurants.co.za/western-cape/all"
]

# Set up Selenium with Firefox
options = webdriver.FirefoxOptions()
options.add_argument('--headless')  # Run in headless mode if you don't need to see the browser
driver = webdriver.Firefox(options=options)

# Function to scroll to the footer
def scroll_to_footer():
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        # Scroll down to the bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # Wait for new content to load
        time.sleep(2)
        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:  # If no new height, we have reached the footer
            break
        last_height = new_height

# Function to scrape the restaurant's address from its individual page
def get_location(restaurant_url):
    driver.get(restaurant_url)
    time.sleep(2)  # Adjust this wait time if needed
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    address = soup.find('p', class_='address')
    if address:
        # Join the address lines into a single line
        return " ".join(line.strip() for line in address.text.splitlines())
    else:
        return 'N/A'

# Function to scrape each URL
def scrape_data(url):
    driver.get(url)
    time.sleep(3)  # Initial wait time
    scroll_to_footer()  # Scroll until the footer is reached
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    restaurants = soup.find('div', class_='items').find_all("div", class_="laout_5 direcotry_listing")
    data = []

    for restaurant in restaurants:
        name = restaurant.find('h2').text.strip()
        print(name)
        url = restaurant.find("div", class_="img_left").find('a', class_="thumbnails").get("href")

        # Visit the restaurant's page to get the full address
        r = requests.get(url)
        address = 'N/A'
        if r.status_code == 200:
            res = BeautifulSoup(r.text, 'html.parser')
            address_tag = res.find("li", class_="address")
            if address_tag:
                # Join the address lines into a single line
                address = " ".join(line.strip() for line in address_tag.text.splitlines())

        data.append({'name': name, 'location': address})

    return data

# CSV file setup
csv_filename = r'/resturants.csv'
with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Name', 'Location'])  # Write CSV header

    # Loop through all URLs, scrape data, and write to CSV
    for url in urls:
        print(f"Scraping {url}...")
        data = scrape_data(url)
        for item in data:
            writer.writerow([item['name'], item['location']])

# Close the browser
driver.quit()

print(f"Data has been saved to {csv_filename}")
