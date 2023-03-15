# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import json
import os
from datetime import datetime as dt



class JsonPipeline:
    def process_item(self, item, spider):
        filename = str(dt.now()).split('.')[0]
        filename = "data-" + \
            filename.replace(':', '-').replace(' ', '-') + '.json'
        # add output directory
        if not os.path.exists('output'):
            os.makedirs('output')
        # write data into a file
        with open(f'output/{filename}', 'w') as wrt:
            wrt.write(json.dumps(item, indent=4))
        # return meta data
        return {
            'data_scraped': True,
            'format': 'JSON',
            'filename': filename,
            'filepath': os.getcwd()+f'/output/{filename}',
        }
