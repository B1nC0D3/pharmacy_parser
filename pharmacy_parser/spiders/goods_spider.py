from datetime import datetime
from unicodedata import normalize
from urllib.parse import urljoin

from scrapy import spiders
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor
from pharmacy_parser.items import GoodsItem
from pharmacy_parser.settings import (COOKIES, START_URLS, PORTIONS_SYMBOLS, PRODUCTS_XPATH, PAGINATION_XPATH,
                                      HEADER_XPATH, TITLE_XPATH, MANUFACTURER_INFO_XPATH, SECTION_XPATH, TAGS_XPATH,
                                      PRICES_XPATH, DESCRIPTION_XPATH, IMAGES_XPATH)


class GoodsSpider(spiders.CrawlSpider):
    name = 'goods_spider'
    allowed_domains = ('apteka-ot-sklada.ru',)

    start_urls = START_URLS

    rules = (
        spiders.Rule(
            LinkExtractor(
                deny='.*/pharmacies',
                restrict_xpaths=(PRODUCTS_XPATH,),
                unique=True),
            callback='parse_item',),
        spiders.Rule(
            LinkExtractor(
                restrict_xpaths=(PAGINATION_XPATH,),
                unique=True),
            follow=True)
        )

    # Переорпеделение скрытого метода для добавления кук.
    # Причина почему не переопределен метод start_requests:
    # https://docs.scrapy.org/en/latest/topics/request-response.html?highlight=request#scrapy.http.Request секция куки
    def _build_request(self, rule_index, link):
        return Request(
            url=link.url,
            callback=self._callback,
            errback=self._errback,
            meta=dict(rule=rule_index, link_text=link.text),
            cookies=COOKIES
        )

    def parse_item(self, response):
        assets = {}
        metadata = {}
        price_data = {
            'cur_price': None,
            'original_price': None,
            'sale_tag': 0,
        }
        stock = {
            'in_stock': False,
            # Счетчик всгеда ноль, либо я не понял как это необходимо реализовать на текущем сайте,
            # не нашел упоминания количества
            'count': 0,
        }
        ts = datetime.now().timestamp()

        rpc = response.url.split('_')[-1]

        # Достаем хэдер с помощью индека потому что xpath всегда возвращается список
        item_header = response.xpath(HEADER_XPATH)[0]

        raw_title_text = item_header.xpath(TITLE_XPATH).get()

        splitted_title = raw_title_text.split()
        # Достаем количество порций если есть
        if 'N' in splitted_title:
            amount_symbol_index = splitted_title.index('N')
            metadata['portions_amount'] = splitted_title[amount_symbol_index+1]
            splitted_title = splitted_title[:amount_symbol_index]

        # И проверяем размер товара. Мл, мг и т.п.
        words_with_digit_indexes = []
        for i, word in enumerate(splitted_title):
            if any(map(str.isdigit, word)):
                if any([symbol in word for symbol in PORTIONS_SYMBOLS]):
                    metadata['portion_size'] = word
                    # Добавление индкекса в условии обуслаовлено тем,
                    # что мы можем найти не только те числа которые нам нужны, но и допустим указание возраста и т. д.
                    words_with_digit_indexes.append(i)
                elif '%' in word:
                    metadata['meds_percentage'] = word
                    words_with_digit_indexes.append(i)

        # Вытаскиваем из разбитого тайтла все элементы из прошлого этапа и соединяем обратно.
        # После добавляем размер в конец
        list(map(splitted_title.pop, words_with_digit_indexes))
        title = ' '.join(splitted_title)
        if metadata.get('portion_size'):
            title = f'{title}, {metadata["portion_size"]}'

        tags = item_header.xpath(TAGS_XPATH).getall()
        country, brand = item_header.xpath(MANUFACTURER_INFO_XPATH).getall()
        metadata['country'] = country
        # Вытаскиваем всю иерахию и отсекаем с помощью срезов первые два общих для всех предка и последнего ребенка,
        # то есть наш товар
        section = item_header.xpath(SECTION_XPATH).getall()[2:-1]

        raw_description = response.xpath(DESCRIPTION_XPATH).getall()
        # Убираем все косяки с кодировкой и пробелами, xpath не справляется с пробелами и переносами строки
        normalized_description = [normalize('NFKC', i.strip()) for i in raw_description]
        description = ' '.join(normalized_description)
        metadata['_description'] = description

        raw_prices = response.xpath(PRICES_XPATH).getall()

        # Если цены есть, то товар в наличии
        if raw_prices:
            # Исправляю с помощью Python потому что normalize-space в xpath очень странно себя ведет
            prices = [price.strip().split()[0] for price in raw_prices]
            # Проверка на присутствие акционной цены
            if len(prices) == 2:
                cur_price, original_price = prices
                # И вычисление скидки
                sale_tag = (original_price - cur_price) / cur_price * 100
                price_data['sale_tag'] = sale_tag
            else:
                cur_price = original_price = prices[0]

            price_data['cur_price'] = cur_price
            price_data['original_price'] = original_price
            stock['in_stock'] = True

        raw_images_links = response.xpath(IMAGES_XPATH)
        if raw_images_links:
            # Если картинки есть, то достаем все относительные ссылки из элементов и превращаем их в абсолютные
            images_links = [urljoin(response.url, link.attrib['src']) for link in raw_images_links]
            # И назначаем первую как главную
            assets['main_image'] = images_links[0]
            assets['set_images'] = images_links[1:]

        yield GoodsItem(
            timestamp=ts,
            RPC=rpc,
            url=response.url,
            title=title,
            marketing_tags=tags,
            brand=brand,
            section=section,
            price_data=price_data,
            stock=stock,
            assets=assets,
            metadata=metadata,
            # Вариант всегда равен одному из-за устройства сайта, другие варианты вынесены в отдельную карточку
            variants=1)
