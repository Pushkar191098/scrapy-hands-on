# Imports
import pandas as pd
import scrapy
from pprint import pprint
from creds import *

# scrapy runspider .\kronos.py -a "filter=DEPARTMENT" -o "data.json"

# Different filtering dict for refining user-data
filter_dict = {
    'DEPARTMENT': 'buName',
    'COUNTRY': 'countryName',
    'TEAM': 'teamName'
}

# Main Crawler Class
class KronosSpider(scrapy.Spider):
    # Name of the crawler (MANDATORY)
    name = 'Kronos'

    def __init__(self, filter='', *args, **kwargs):
        # Customized filter
        self.filter = filter.upper().strip()
        super().__init__(*args, **kwargs)

    def start_requests(self):
        datax = {
            "password" : PASSWORD,
            "username" : USERNAME
        }

        yield scrapy.http.JsonRequest("https://kronos.tarento.com/api/v1/user/login", data=datax)

    def parse(self, response):
        data = response.json()
        # pprint(data)
        # Cheacking Login status
        if data['statusCode'] == 200:
            print("------------LOGGED IN SUCESSFULLY------------")

            # Collect meta-data from response
            module_list = data['responseData']['moduleList']
            cert = data['responseData']['sessionId']
            logged_in_user = data['responseData']
            del logged_in_user['moduleList']
            cookie = f"loggedInUser={logged_in_user}; moduleList={module_list}; certificate={cert}"

            yield scrapy.Request(
                "https://kronos.tarento.com/api/v1/user/getAllUsersByOrg",
                method="POST",
                headers={
                    'Authorization': cert,
                    'Cookie': cookie
                    },
                callback=self.collect_data
                )
        else:
            raise Exception('------------------------LOGIN FAILED------------------------')

    def collect_data(self, response):
        user_data = response.json()['responseData']
        # pprint(user_data)

        # Deleting misscelaneous field which is not required
        for i in user_data:
            del i['imagePath']

        # If no filter is given by user then it will log all data as it it
        if self.filter == '':
            msg_string = f'Filter : {None}'
            output_data = user_data

        # else the data will be refined according to the filter
        else:
            msg_string = f'Filter : {self.filter}'
            if self.filter in filter_dict.keys():
                output_data = list()
                df = pd.DataFrame(user_data)
                for f_val in df[filter_dict[self.filter]].unique():
                    dfx = df[df[filter_dict[self.filter]] == f_val]
                    output_data.append({'filter': self.filter, self.filter.lower(): f_val, 'count': len(dfx), 'data': dfx.to_dict(orient='records')})
            else:
                output_data = None

        # log data
        yield {
            'status': 'Success',
            'message': msg_string,
            'data': output_data
        }