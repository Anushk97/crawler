import scrapy
import pandas as pd

input_path = '/Users/emmy/Desktop/scraper/input_for_scraper.xlsx'
output_path = '/Users/emmy/Desktop/scraper/output_by_scraper.csv'

class DisasterItem(scrapy.Item):
    region = scrapy.Field()
    start_url = scrapy.Field()
    table_data = scrapy.Field()

class MySpider(scrapy.Spider):
    name = "my_spider_2"

    columns = ['Earthquake', 'Landslide', 'Wildfire', 'Extreme heat', 'River flood', 'Urban flood',
               'Cyclone', 'Water scarcity', 'Coastal flood', 'Tsunami', 'Volcano', 'region']
    df = pd.DataFrame(columns=columns)
    final_df = pd.DataFrame(columns=columns)

    #urls_df = pd.read_csv('/Users/emmy/Desktop/scraper/urls_2.csv')
    urls_df = pd.read_excel(input_path)
    urls_df = urls_df[:100]
    urls_dict = urls_df.set_index("region")['url'].to_dict()
    #print('url_dict', urls_dict)


    def start_requests(self):
        #start_urls = {"Bali": "https://thinkhazard.org/en/report/1513-indonesia-bali", "Mumbai": "https://thinkhazard.org/en/report/70183-india-maharashtra-mumbai-suburban", 'Burka': "https://thinkhazard.org/en/report/3468-afghanistan-baghlan-burka"}
        for region, data in self.urls_dict.items():
            yield scrapy.Request(
                url=data,
                callback=self.parse_region,
                meta={'region': region, 'start_url': data}
            )

    def parse_region(self, response):
        item = DisasterItem()
        region = response.meta['region']
        start_url = response.meta['start_url']

        item["region"] = region
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
            self.df.loc[region, 'region'] = region
        # Adding the region_granular as a column
        #df['region_granular'] = item['region_granular']
        self.final_df = pd.merge(self.df, self.urls_df, on='region')

        #yield item

    def closed(self, reason):
        self.final_df.to_csv(output_path, index=False)
        self.log('DataFrame exported to CSV.')
