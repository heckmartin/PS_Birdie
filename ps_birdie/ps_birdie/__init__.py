import json
import scrapy

class BirdieSpider(scrapy.Spider):
    name= 'BirdieExtra'
    start_urls = ['https://www.extra.com.br/c/tv-e-video/televisores/?filtro=c1_c2']
    