import sys

import streamlit as st
import pandas as pd
import numpy as np
#import xlsxwriter

st.title('Project Crawler!')

user_input = st.text_input("name file")

with st.form("my-form", clear_on_submit=True):
    uploaded_file = st.file_uploader("upload file")
    submitted = st.form_submit_button("submit")


#Change this values accordingly
window_size_x = 800 #Enable only when to see the crawler
window_size_y = 800
batch_size = 20
#file_path_source = r'/Users/emmy/Desktop/scraper/webscraping output - climate data (2).xlsx'
#path_to_save_output = r"/Users/emmy/Desktop/scraper/final_output_3.xlsx"
#path_to_blocked_values = r'/Users/emmy/Desktop/scraper/blocked_batch_3.xlsx'
#ChromeDrive_path = r"/Users/emmy/Desktop/scraper/chromedriver"

#Step 1: Import Libraries
import requests
import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import time
# from tqdm import tqdm
import os
import csv

#Step 2: Import Data and data transformation
data = pd.read_excel(uploaded_file)
#data = data.drop(labels=0, axis = 0)
#data.columns = data.iloc[0]
#data = data.drop(labels=1, axis = 0)
dataset = data[:60]
st.write(dataset)
#print(data)
#data = data[['Country', 'Region (HL)', 'Region (Granular)', 'River flood', 'Urban flood', 'Earthquake', 'Landslide', 'Wildfires', 'Water scarcity', 'Cyclone', 'Extreme heat', 'Coastal flood', 'Tsunami', 'Volcano']]
#print(data)

output_data = pd.DataFrame()
blocked_data = pd.DataFrame()

total_iterations = len(dataset)
#pbar = tqdm(total=total_iterations, desc='Progress', unit='iteration')

#Step 3: Crawler
#service = Service(ChromeDrive_path)
#chrome_options = Options()
#chrome_options.add_argument('--headless')

@st.cache_data
def clean_and_split(value):
    cleaned = value.strip()
    split = cleaned.split("\n")
    return [split.strip() for split in split if split.strip()]

@st.cache_data
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

start = 0
end = batch_size
batch = 1
total_batches = int(len(dataset)/batch_size)
st.write('total batches', total_batches)


with open(f'{user_input}.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(dataset.columns)


while start < end:
    data_cop = dataset[start:end]
    #data_to_merge = data_cop[['Country','Region (HL)','Region (Granular)']]
    #print('data_to_merge', data_to_merge)
    granular = data_cop['Region (Granular)']

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")

    # driver = get_driver()
    service = Service()
    # options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=service, options=options)
    # driver = webdriver.Chrome(options=chrome_options)
    # driver.set_window_size(window_size_x, window_size_y)

    res = []
    val = []
    blocked = []
    st.write('batch:', batch)

    for i in granular:
        
        driver.get('https://thinkhazard.org/en/')
        driver.find_element(By.XPATH,'//*[@id="myModal"]/div/div/div[2]/button[2]').click()
        driver.find_element(By.XPATH,'/html/body/div[2]/div/form/span[2]/input[2]').send_keys(i)
        driver.implicitly_wait(60)
        driver.find_element(By.XPATH, '//*[@id="search"]/span[2]/div/div/div[1]').click()
        driver.implicitly_wait(60)
        URL = driver.current_url
        resp = requests.get(URL)
        soup = BeautifulSoup(resp.text, features='html.parser')
        #print('soup', soup)

        for j in soup.find_all('h2', {'class': 'page-header'}):
            val.append(i)
            res.append(j.get_text())
        #print('res', res)
    
        print(i, 'blocked')
        blocked.append(i)

        start += 1
        st.write(start)

    processed = [clean_and_split(value) for value in res]
    #print('processed', processed)

    df = pd.DataFrame(processed, columns=['Col', 'level'])
    #print('df', df)
    #df['Country'] = data_cop['Country']
    #df['Region HL'] = data_cop['Region (HL)']
    df['Region (Granular)'] = val
    block = pd.DataFrame(blocked, columns=['blocked'])
    table = pd.pivot_table(df, index=['Region (Granular)'], columns='Col', values='level', aggfunc=lambda x: ' '.join(x), sort = False)
    table = table.reset_index()
    #table = table[['Region (Granular)', 'River flood', 'Urban flood', 'Earthquake', 'Landslide', 'Wildfire', 'Water scarcity', 'Cyclone', 'Extreme heat', 'Coastal flood', 'Tsunami', 'Volcano']]
    #st.write('table', table)
    #print('table', table)
    #final = pd.concat([table, data_to_merge], axis=1)
    #print(final)
    #final = pd.merge(table, data_to_merge, on='Region (Granular)')
    #final = final[['Country', 'Region (HL)', 'Region (Granular)', 'River flood', 'Coastal flood', 'Wildfire', 'Urban flood', 'Landslide', 'Tsunami', 'Water scarcity', 'Extreme heat', 'Cyclone', 'Volcano', 'Earthquake']]
    #st.write('final', final)
    #print('final', final)

    #output_data = output_data._append(final)
    blocked_data = blocked_data._append(block)

    with open(f'{user_input}.csv', mode='a', newline='') as file:
        csv_writer = csv.writer(file)
        for _, row in table.iterrows():
            csv_writer.writerow(row)

    if batch <= total_batches:
        batch += 1
    
    if end < len(dataset):
        end += batch_size

st.write('blocked', blocked_data)

st.download_button(
    "Press to Download output",
    open(f'{user_input}.csv', 'rb').read(),
    f"{user_input}.csv",
    "text/csv",
    key='download-csv'
)
