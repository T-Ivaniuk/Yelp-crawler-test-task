import os
from urllib.parse import urlparse, parse_qs
import aiohttp
import requests
from bs4 import BeautifulSoup
import random
import json
import asyncio
import time

retries = 3
headers = \
    {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'authority': 'www.yelp.com'
    }


def get_proxys():
    with open('proxys.txt') as f:
        proxys = f.readlines()
        return proxys


def post_ads_filter(post):
    try:
        return post['searchResultBusiness']['isAd']
    except KeyError:
        return True


def extract_review_fileds(review: json):
    reviewer_name = review['user']['markupDisplayName']
    reviewer_location = review['user']['displayLocation']
    review_date = review['localizedDate']
    return {
        'Reviewer name': reviewer_name,
        'Reviewer location': reviewer_location,
        'Review date': review_date
    }


def extract_url_from_redirection(redir_url):
    parsed_url = urlparse(redir_url)
    query = parse_qs(parsed_url.query)['url'][0]
    return urlparse(query).netloc


def convert_business_url(url):
    return url if "://" in url else "http://" + url


def save_and_return_file(category, location, data):
    filename = f'{category}, {location}.json'
    path = os.path.join(os.getcwd(), 'created json files')
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, filename)
    with open(filepath, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, indent=4, ensure_ascii=False)
        print(f'{filename} saved successfully')
    return outfile


def transform_incoming_value(pages):
    if pages == 'all':
        return 100
    return pages


class Category:
    def __init__(self, category: str, location: str, pages: str or int = 10):
        self._category = category
        self._location = location
        self._pages = transform_incoming_value(pages)
        self._proxy = get_proxys()
        self._retries = 3
        self._page_multiplier = 10

    def collect_business_for_all_pages_in_category(self):
        businesses_in_category = []
        for page in range(0, self._pages):
            attempt, page_response = self.parse_page(page=page * self._page_multiplier)
            if 'mainContentComponentsListProps' not in page_response['searchPageProps']:
                print(f'Found {len(businesses_in_category)} business from {page} page(s) in {self._category}')
                break

            businesses_in_page = page_response['searchPageProps']['mainContentComponentsListProps']
            no_ads_businesses_in_page = [x for x in businesses_in_page if not post_ads_filter(x)]
            businesses_in_category.extend(no_ads_businesses_in_page)
            print(f'Attempt: {attempt} | Found {len(no_ads_businesses_in_page)} businesses in page:{page}, category: {self._category}')

            if len(no_ads_businesses_in_page) < self._page_multiplier:
                print(f'Found {len(businesses_in_category)} business from {page + 1} page(s) in {self._category}')
                break

        return businesses_in_category

    def parse_page(self, page):
        page_url = f"https://www.yelp.com/search/snippet?find_desc={self._category}&find_loc={self._location}&start={page}"
        for attempt in range(self._retries + 1):
            try:
                proxy = {'https': random.choice(self._proxy),
                         'http': random.choice(self._proxy)}
                response = requests.get(url=page_url, headers=headers, proxies=proxy, timeout=10).json()
                return attempt, response
            except ValueError as exc:
                print(f'Attempt: {attempt} | Website response: request denied. ERROR : {exc} ')
            if attempt < self._retries:
                sleepTime = 1.0 * 2 ** attempt  # sleep 1 second after first attempt, 2 after second, 4 after third, etc.
                time.sleep(sleepTime)


async def get_profile_json(biz_id):
    for attempt in range(retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                url = f'https://www.yelp.com/biz/{biz_id}/props'
                response = await session.get(url=url, headers=headers, proxy=random.choice(get_proxys()))
                return await response.json()
        except (ValueError, aiohttp.ContentTypeError):
            print('Website response: request denied')
        if attempt < retries:
            sleepTime = 1.0 * 2 ** attempt  # exponential backoff: sleep 1 second after first attempt, 2 after second, 4 after third, etc.
            await asyncio.sleep(sleepTime)


async def parse_url_from_html(yelp_url):
    for attempt in range(retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                response = await session.get(url=yelp_url, headers=headers, proxy=random.choice(get_proxys()))
                soup = BeautifulSoup(await response.text(), 'lxml')
                redirection_url = soup.find('p', text='Business website').next_sibling.a['href']
                return extract_url_from_redirection(redirection_url)
        except AttributeError:
            return None
        except ValueError:
            print('Website response: request denied')
        if attempt < retries:
            sleepTime = 1.0 * 2 ** attempt  # exponential backoff: sleep 1 second after first attempt, 2 after second, 4 after third, etc.
            await asyncio.sleep(sleepTime)


async def get_business_page(business_obj):
    website_url_via_json = business_obj.parse_url_from_json_obj()
    if website_url_via_json is not None:
        return convert_business_url(website_url_via_json)
    website_url_via_html = await parse_url_from_html(yelp_url=business_obj.yelp_url)
    if website_url_via_html is not None:
        return convert_business_url(website_url_via_html)
    return 'Not exist on webpage'


class Business:
    def __init__(self, search_data):
        self.reviews_data = None
        self.business_url = None
        self.reviews_count = None
        self.profile_data = None
        self.reviews_to_extract = 5
        self.domain_url = 'http://www.yelp.com'
        self.search_data = search_data
        self.bizId = self.get_id()
        self.name = self.get_name()
        self.rating = self.get_rating()
        self.yelp_url = self.get_yelp_url()

    def get_id(self):
        return self.search_data.get('bizId')

    def get_name(self):
        return self.search_data['searchResultBusiness'].get('name')

    def get_rating(self):
        return self.search_data['searchResultBusiness'].get('rating')

    def get_yelp_url(self):
        return self.domain_url + self.search_data['searchResultBusiness'].get('businessUrl')

    def get_reviews_count(self):
        try:
            return self.profile_data['bizDetailsPageProps']['reviewFeedQueryProps']['pagination']['totalResults']
        except TypeError:
            return None

    def get_review_data(self):
        try:
            first_n_reviews = self.profile_data['bizDetailsPageProps']['reviewFeedQueryProps']['reviews'][
                              0:self.reviews_to_extract]
            return [extract_review_fileds(x) for x in first_n_reviews]
        except TypeError:
            print("get_review_data NoneType ERROR")
            return None

    def create_json(self):
        return {
            'Business name': self.name,
            'Business rating': self.rating,
            'Number of reviews': self.reviews_count,
            'Business yelp url': self.yelp_url,
            'Business website': self.business_url,
            'Reviews': self.reviews_data
        }

    def parse_url_from_json_obj(self):
        try:
            redirection_url = self.profile_data['bizDetailsPageProps']['bizPortfolioProps']['ctaProps']['website']
            return extract_url_from_redirection(redirection_url)
        except (TypeError, KeyError):
            return None


async def scrape_business(business):
    business_obj = Business(search_data=business)
    business_obj.profile_data = await get_profile_json(biz_id=business_obj.bizId)
    business_obj.business_url = await get_business_page(business_obj)
    business_obj.reviews_count = business_obj.get_reviews_count()
    business_obj.reviews_data = business_obj.get_review_data()
    res = business_obj.create_json()
    print(f'{business_obj.name} scraped', res)
    return res


async def create_business_scraping_task(business: list, chunk=25):
    result = []
    composite_list = [business[x:x + chunk] for x in range(0, len(business), chunk)]
    for each_chunk in composite_list:
        asyncio_tasks = [asyncio.create_task(scrape_business(x)) for x in each_chunk]
        result.extend(await asyncio.gather(*asyncio_tasks))
    return result


def main(category, location, pages=10):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    cat = Category(category='Mexican', location=location, pages=pages)
    category_business_data = cat.collect_business_for_all_pages_in_category()

    category_result = asyncio.new_event_loop().run_until_complete(create_business_scraping_task(category_business_data))

    return save_and_return_file(category=category, location=location, data=category_result)


main(category=input('category: '), location=input('location: '), pages='all')  #main('Mexican', 'Ohio, IL, United States')
