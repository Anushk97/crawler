import scrapy
import pandas as pd
import streamlit as st
import subprocess

st.title('Project Crawler!')

with st.form("my-form", clear_on_submit=True):
    uploaded_file = st.file_uploader("upload file")
    submitted = st.form_submit_button("submit")

@st.cache_data
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')


class DisasterItem(scrapy.Item):
    region_granular = scrapy.Field()
    start_url = scrapy.Field()
    table_data = scrapy.Field()

class MySpider(scrapy.Spider):
    name = "my_spider_2"

    columns = ['Earthquake', 'Landslide', 'Wildfire', 'Extreme heat', 'River flood', 'Urban flood',
               'Cyclone', 'Water scarcity', 'Coastal flood', 'Tsunami', 'Volcano', 'region_granular']
    df = pd.DataFrame(columns=columns)

    urls_df = pd.read_csv(uploaded_file)
    urls_df = urls_df[:100]
    urls_dict = urls_df.set_index("region")['url'].to_dict()

    def start_requests(self):
        #start_urls = {"Bali": "https://thinkhazard.org/en/report/1513-indonesia-bali", "Mumbai": "https://thinkhazard.org/en/report/70183-india-maharashtra-mumbai-suburban", 'Burka': "https://thinkhazard.org/en/report/3468-afghanistan-baghlan-burka"}
        for k, v in self.urls_dict.items():
            yield scrapy.Request(
                url=v,
                callback=self.parse_region,
                meta={'region': k, 'start_url': v}
            )

    def parse_region(self, response):
        item = DisasterItem()
        region = response.meta['region']
        start_url = response.meta['start_url']

        item["region_granular"] = region
        item["start_url"] = start_url

        # Extract other disaster-related data using selectors
        #item["river_flood"] = response.xpath("/html/body/div[2]/div[1]/div[1]/a[1]").get()

        placeholder = response.xpath("/html/body/div[2]/div[1]/div[1]").get()
        h2_elements = response.xpath("//h2[@class='page-header']")
        extracted_text = [h2.xpath("normalize-space(.)").get() for h2 in h2_elements]

        data_dict = {}
        for entry in extracted_text:
            disaster_type, risk_level = entry.split(" ", 1)
            data_dict[disaster_type] = risk_level

        item["table_data"] = extracted_text

        #print(data_dict)
        # Extracting relevant data from table_data
        keywords = ['Coastal', 'Cyclone', 'Extreme', 'Wildfire', 'Urban', 'Earthquake', 'Tsunami',
                    'Water', 'River', 'Volcano', 'Landslide', 'flood', 'heat', 'scarcity']

        relevant_data = []

        for entry in item['table_data']:
            words = entry.split(maxsplit=2)
            #print('words', words)
            if len(words) > 2:
                if words[0] and words[1] in keywords:
                    joined = " ".join([words[0],words[1]])
                    relevant_data.append([joined, words[-1]])
                else:
                    joined = " ".join([words[1], words[2]])
                    relevant_data.append([words[0], joined])
            else:
                relevant_data.append([words[0], words[-1]])


        #print('relevant_data', relevant_data)

        # keyword_column_map = {entry[0]: entry[0] for entry in relevant_data}

        for entry in relevant_data:
            keyword = entry[0]
            value = entry[1]
            self.df.loc[region, keyword] = value
            self.df.loc[region, 'region_granular'] = region
        
        self.download_csv()

    def download_csv(self):
    # Convert the DataFrame to CSV bytes
        csv_bytes = self.df.to_csv(index=False).encode("utf-8")
        return csv_bytes
    # Display the download button

def main():
    # Run the Scrapy spider
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()

    download_button = st.button("Download CSV")
    if download_button:
        csv_bytes = MySpider().download_csv()  # Call download_csv method
        st.download_button(
            "Press to Download output",
            csv_bytes,
            "file.csv",
            "text/csv",
            key="download-csv"
        )

if __name__ == "__main__":
    main()
