import asyncio
from aiohttp import web
from aiohttp.web import Response
from elasticsearch_async import AsyncElasticsearch
import csv
from datetime import datetime
from collections import defaultdict


class DataConsumerService:
    def __init__(self):
        # TODO: Set up connection with elasticsearch
        # TODO: Read and store product master into a dict
        self.product_master = {}
        self.prepare_master()
        self.client = AsyncElasticsearch(hosts=['localhost'])
        # Create index if it does not exist already
        asyncio.ensure_future(self.client.indices.create(index='data-stream', ignore=400))
        print('ready')

    def _valid_barcode(self, barcode):
        if len(barcode)==13:
            try:
                int(barcode)
                return True
            except:
                return False
        return False

    def prepare_master(self):
        f = open('../../../Data/Products.csv', 'r')
        all_products = [{k: v for k, v in row.items()}
            for row in csv.DictReader(f)]
        for product in all_products:
            if self._valid_barcode(product['BARCODE']):
                self.product_master[product['BARCODE']] = product

    def process_line(self, input_text):
        # Processing data and converting it into a dict for easy storage in ES
        line = input_text.strip().split('|')
        values = line[:7] + ['|'.join(line[7:-1]), line[-1]]
        keys = ["POS_Application_Name","STOREID","MACID","BILLNO","BARCODE","GUID","CREATED_STAMP",
            "CAPTURED_WINDOW","UPDATE_STAMP"]
        processed_dict = dict(zip(keys, values))
        processed_dict["CREATED_STAMP"] = datetime.strptime(processed_dict["CREATED_STAMP"], '%Y-%m-%d %H:%M:%S.%f')
        processed_dict["UPDATE_STAMP"] = datetime.strptime(processed_dict["UPDATE_STAMP"], '%Y-%m-%d %H:%M:%S.%f')
        return processed_dict
    def combine_with_master(self, clean_dict):
        barcode = clean_dict['BARCODE']
        if barcode not in self.product_master:
            return {}
        clean_dict.update(self.product_master[barcode])
        return clean_dict

    async def insert_event(self, enriched_dict):
        await self.client.index(index='data-stream', body=enriched_dict, doc_type='data_line')
        pass

    async def consumer(self, request):
        data_line = await request.text()
        # print(data_line)
        clean_dict = self.process_line(data_line)
        enriched_dict = self.combine_with_master(clean_dict)
        if enriched_dict:
            await self.insert_event(enriched_dict)
        return Response(status=200)

    async def bulk(self, request):
        asyncio.ensure_future(self.bulk_insert())
        return Response(status=200)

    async def bulk_insert(self):
        print('Starting bulk inserts into ES')
        csv_files = ["BUSY7246fb6.csv","EASYSOL326dc49.csv","MARGfc72cd7.csv",
                "RETAIL_EXPERe490dde.csv","SOFT_GENd6661f5.csv"]
        prog = 0
        for filename in csv_files:
            f = open('../../../Data/' + filename, 'r')
            f.readline()
            for data_line in f:
                prog+=1
                if prog%10000==0:
                    print(prog)
                try:
                    clean_dict = self.process_line(data_line)
                    enriched_dict = self.combine_with_master(clean_dict)
                    if enriched_dict:
                        await self.insert_event(enriched_dict)
                except:
                    pass


app = web.Application()
loop = asyncio.get_event_loop()
http_service = DataConsumerService()
app.router.add_route('POST', '/consume', http_service.consumer)
app.router.add_route('GET', '/bulk', http_service.bulk)

serve = loop.create_server(app.make_handler(), '0.0.0.0', 8000)
loop.run_until_complete(serve)

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass
