input_path = '/Users/emmy/Desktop/scraper/blocked_urls.xlsx'
output_path = '/Users/emmy/Desktop/scraper/unblocked.csv'
output_blocked_path = '/Users/emmy/Desktop/scraper/blocked.csv'


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pandas as pd
import multiprocessing

def scrape_region(region):
    try:
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--headless")

        service = Service()
        driver = webdriver.Chrome(service=service, options=options)

        # Replace 'YOUR_BASE_URL' with the actual base URL
        base_url = 'https://thinkhazard.org/en/'
        driver.get(base_url)
        driver.find_element(By.XPATH, '//*[@id="myModal"]/div/div/div[2]/button[2]').click()
        driver.find_element(By.XPATH, '/html/body/div[2]/div/form/span[2]/input[2]').send_keys(region)
        driver.implicitly_wait(5)
        driver.find_element(By.XPATH, '//*[@id="search"]/span[2]/div/div/div[1]').click()
        driver.implicitly_wait(10)
        #URL = driver.current_url

        # Your scraping logic here
        # For demonstration, let's just get the current URL
        scraped_url = driver.current_url

        driver.quit()

        return {'region': region, 'url': scraped_url}

    except Exception as e:
        print('Blocked', region)
        return None

if __name__ == '__main__':
    df = pd.read_excel(input_path)
    df = df[:10]
    #regions_to_scrape = df['Region (Granular)'].tolist()
    regions_to_scrape = df['blocked'].tolist()

    num_processes = multiprocessing.cpu_count()  # Use the number of available CPU cores
    pool = multiprocessing.Pool(processes=num_processes)

    output = pool.map(scrape_region, regions_to_scrape)
    pool.close()
    pool.join()
    #print('output',output)

    # Process your output list and blocked list here
    result_output = [item for item in output if item is not None]
    blocked_output = [item for item in output if item is None]
    #print('blocked_output', blocked_output)

    # Create DataFrames from the processed output
    output_df = pd.DataFrame(result_output, columns=['region', 'url'])
    blocked_df = pd.DataFrame(blocked_output, columns=['blocked'])

    # Save DataFrames to CSV files
    output_df.to_csv(output_path, index=False)
    blocked_df.to_csv(output_blocked_path, index=False)
