import json
import scrapy
import pandas as pd
 
class BirdieSpider(scrapy.Spider):
    name = 'ExtraSpider'
    base_url = 'https://www.extra.com.br/api/catalogo-ssr/products/?Filtro=c1_c2&PaginaAtual=%d'
    pagina_atual = 1
    pag_rev_atual = 0
    start_urls = [base_url % pagina_atual]
    crawled = 0
    next_product = None
    produtos_ids = []
    crawleando_products = True
    Data = pd.DataFrame()

    def parse(self,response):
        if self.crawleando_products:
            json_data = json.loads(response.text)
            for quote in json_data['products']:
                self.produtos_ids.append(quote['id'])
                product_elem = {'name': quote['name'], 'brand_name': quote['brand']['name'], 'url':quote['urls'], 'sku':quote['urls'].split('/')[-1], 'id':quote['id'],'rating_media': quote['rating']['ratingsMedia'], 'reviews':[]}
                self.Data = self.Data.append(product_elem, ignore_index = True)
                self.crawled += 1
            
            if json_data['products']:
                self.pagina_atual += 1
                next_page = self.base_url%self.pagina_atual
                yield scrapy.Request(url = next_page, callback=self.parse)

            else:
                self.crawleando_products = False
                self.next_product = self.produtos_ids.pop()
                next_page = 'https://pdp-api.extra.com.br/api/v2/reviews/product/' +str(self.next_product)+'/source/EX?page=' +str(self.pag_rev_atual)+'&size=50'
                yield scrapy.Request(url = next_page, callback=self.parse)
        
        elif not self.crawleando_products:
            print('pegandoreviews')

            json_data = json.loads(response.text)
            if json_data['review']['userReviews']:
                for review in json_data['review']['userReviews']:
                    print(review['text'])
                    print(self.Data.loc[(self.Data['id']) == self.next_product,['reviews']])
                    print(type(self.Data.loc[(self.Data['id']) == self.next_product,['reviews']]))
                    print(self.Data.loc[(self.Data['id']) == self.next_product]['reviews'])
                    print(type(self.Data.loc[(self.Data['id']) == self.next_product]['reviews']))
                    self.Data.loc[(self.Data['id']) == self.next_product]['reviews'].append(review['text'])
                self.pag_rev_atual += 1
                next_page = 'https://pdp-api.extra.com.br/api/v2/reviews/product/' +str(self.next_product)+'/source/EX?page=' +str(self.pag_rev_atual)+'&size=50'
                yield scrapy.Request(url = next_page, callback=self.parse)

            else:
                if self.produtos_ids: 
                    self.pag_rev_atual = 0
                    self.next_product = self.produtos_ids.pop()
                    next_page = 'https://pdp-api.extra.com.br/api/v2/reviews/product/' +str(self.next_product)+'/source/EX?page=' +str(self.pag_rev_atual)+'&size=50'
                    yield scrapy.Request(url = next_page, callback=self.parse)
                else:
                    print(self.crawled)
                    self.Data.to_csv('Birdie_PS.csv',index = False)

            


