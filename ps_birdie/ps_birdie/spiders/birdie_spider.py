import json
import scrapy
import pandas as pd
import ipdb
class BirdieSpider(scrapy.Spider):
    name = 'ExtraSpider'
    base_url = 'https://www.extra.com.br/api/catalogo-ssr/products/?Filtro=c1_c2&PaginaAtual=%d' # Link para api que retorna a informaçāo dos produtos em forma de json
    # Link para refrigeradores: 'https://www.extra.com.br/api/catalogo-ssr/products/?Filtro=d47457_c13_c14&PaginaAtual=%d'
    # Link para impressoras: 'https://www.extra.com.br/api/catalogo-ssr/products/?Filtro=c56_c61&PaginaAtual=%d'
    # Link para Televisores: 'https://www.extra.com.br/api/catalogo-ssr/products/?Filtro=c1_c2&PaginaAtual=%d'
    pagina_atual = 1
    pag_rev_atual = 0
    start_urls = [base_url % pagina_atual]
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
            
            if json_data['products']:
                self.pagina_atual += 1
                next_page = self.base_url%self.pagina_atual
                yield scrapy.Request(url = next_page, callback=self.parse)

            else:
                self.crawleando_products = False
                self.next_product = self.produtos_ids.pop()
                next_page = 'https://pdp-api.extra.com.br/api/v2/reviews/product/' +str(self.next_product)+'/source/EX?page=' +str(self.pag_rev_atual)+'&size=50'
                yield scrapy.Request(url = next_page, callback=self.parse)
        
        elif not self.crawleando_products: # Flag utilizada para coletar as reviews de usuarios após a coleta dos outros dados
            json_data = json.loads(response.text)
            if json_data['review']['userReviews']:
                for review in json_data['review']['userReviews']:
                    self.Data.loc[self.Data[self.Data['id'] == self.next_product].index[0],'reviews'].append(review['text'])
                self.pag_rev_atual += 1
                next_page = 'https://pdp-api.extra.com.br/api/v2/reviews/product/' +str(self.next_product)+'/source/EX?page=' +str(self.pag_rev_atual)+'&size=50'
                yield scrapy.Request(url = next_page, callback=self.parse)

            else:
                if self.produtos_ids: # Caso o json venha com o campo de reviews vazio, procura as reviews do próximo produto
                    self.pag_rev_atual = 0  
                    self.next_product = self.produtos_ids.pop()
                    next_page = 'https://pdp-api.extra.com.br/api/v2/reviews/product/' +str(self.next_product)+'/source/EX?page=' +str(self.pag_rev_atual)+'&size=50'
                    yield scrapy.Request(url = next_page, callback=self.parse)
            self.Data.to_csv('Birdie_PS.csv',index = False)

            


