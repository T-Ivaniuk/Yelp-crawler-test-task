from urllib.parse import urlparse, parse_qs
from aiohttp import ContentTypeError
from bs4 import BeautifulSoup
import random
import json
import asyncio
import aiohttp

proxys = []

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                  ' Chrome/109.0.0.0 Safari/537.36'}


def extract_url_from_redirection(redir_url):
    parsed_url = urlparse(redir_url)
    query = parse_qs(parsed_url.query)['url'][0]
    return urlparse(query).netloc


def extract_review_fileds(review: json):
    reviewer_name = review['user']['markupDisplayName']
    reviewer_location = review['user']['displayLocation']
    review_date = review['localizedDate']
    return {'Reviewer name': reviewer_name,
            'Reviewer location': reviewer_location,
            'Review date': review_date}


def save_and_return_file(filename, data):
    with open(filename, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, indent=4, ensure_ascii=False)
        print(f'{filename} saved successfully')
    return outfile


async def get_profile_json(biz_id):
    try:
        async with aiohttp.ClientSession() as session:
            url = f'https://www.yelp.com/biz/{biz_id}/props'
            response = await session.get(url=url, headers=headers, proxy=random.choice(proxys))
            return await response.json()
    except ValueError:
        asyncio.get_event_loop().close()
        raise Exception('Website response: request denied')
    except ContentTypeError as E:
        data = await response.read()
        print(url)
        print(data)


async def parse_url_from_html(yelp_url):
    try:
        async with aiohttp.ClientSession() as session:
            response = await session.get(url=yelp_url, headers=headers, proxy=random.choice(proxys))
            soup = BeautifulSoup(await response.text(), 'lxml')
            redirection_url = soup.find('p', text='Business website').next_sibling.a['href']
            return extract_url_from_redirection(redirection_url)
    except AttributeError:
        return None
    except ValueError:
        print('Website response: request denied')
        return None
    except Exception as E:
        print(E)
        return None


def convert_business_url(url):
    return url if "://" in url else "http://" + url


async def get_business_page(business_obj):
    website_url_via_json = business_obj._parse_url_from_json_obj()
    if website_url_via_json is not None:
        return convert_business_url(website_url_via_json)
    website_url_via_html = await parse_url_from_html(yelp_url=business_obj.yelp_url)
    if website_url_via_html is not None:
        return convert_business_url(website_url_via_html)
    return 'Not exist on webpage'


class ABusiness:
    def __init__(self, search_data):
        self.reviews_to_extract = 5
        self._domain_url = 'http://www.yelp.com'
        self._search_data = search_data
        self._bizId = self.get_id()
        self.name = self.get_name()
        self.rating = self.get_rating()
        self.yelp_url = self.get_yelp_url()

    def get_id(self):
        return self._search_data.get('bizId')

    def get_name(self):
        return self._search_data['searchResultBusiness'].get('name')

    def get_rating(self):
        return self._search_data['searchResultBusiness'].get('rating')

    def get_yelp_url(self):
        return self._domain_url + self._search_data['searchResultBusiness'].get('businessUrl')

    def get_reviews_count(self):
        return self._profile_data['bizDetailsPageProps']['reviewFeedQueryProps']['pagination']['totalResults']

    def get_review_data(self):
        if self.reviews_count < self.reviews_to_extract:
            self.reviews_to_extract = self.reviews_count
        first_n_reviews = self._profile_data['bizDetailsPageProps']['reviewFeedQueryProps']['reviews'][
                          0:self.reviews_to_extract]
        return [extract_review_fileds(x) for x in first_n_reviews]

    def create_json(self):
        return {
            'Business name': self.name,
            'Business rating': self.rating,
            'Number of reviews': self.reviews_count,
            'Business yelp url': self.yelp_url,
            'Business website': self.business_url,
            'Reviews': self.get_review_data()
        }

    def _parse_url_from_json_obj(self):
        try:
            redirection_url = self._profile_data['bizDetailsPageProps']['bizPortfolioProps']['ctaProps']['website']
            return extract_url_from_redirection(redirection_url)
        except (TypeError, KeyError):
            return None


async def scrape_business(business):
    business_obj = ABusiness(search_data=business)
    business_obj._profile_data = await get_profile_json(biz_id=business_obj._bizId)
    business_obj.reviews_count = business_obj.get_reviews_count()
    business_obj.business_url = await get_business_page(business_obj)
    res = business_obj.create_json()
    print(f'{business_obj.name} scraped')
    return res


async def create_business_scraping_task(business: list):
    asyncio_tasks = [asyncio.create_task(scrape_business(x)) for x in business]
    return await asyncio.gather(*asyncio_tasks)


async def collect_business_in_page(page_url):
    try:
        async with aiohttp.ClientSession() as session:
            page_response = await session.get(url=page_url, headers=headers, proxy=random.choice(proxys))
            result = await page_response.json()
            print(f'{page_url} scraped successfully')
            return result
    except (ValueError, ContentTypeError):
        raise Exception(f'Website response: request denied')


async def create_category_scraping_task(pages, category, location):
    asyncio_tasks = []
    for page in range(0, pages * 10, 10):
        page_url = f"https://www.yelp.com/search/snippet?find_desc={category}&find_loc={location}&start={page}"
        asyncio_tasks.append(asyncio.create_task(collect_business_in_page(page_url)))
    return await asyncio.gather(*asyncio_tasks)


def filter_post_by_ad_value(post):
    try:
        return post['searchResultBusiness']['isAd']
    except:
        return True


def scrape_category(category, location, pages=1):
    all_busines_in_category = []
    result = {}
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    cat_scraping_result = asyncio.run(create_category_scraping_task(pages=pages, category=category, location=location))

    for each_page in cat_scraping_result:
        for each_business in each_page['searchPageProps']['mainContentComponentsListProps']:
            if not filter_post_by_ad_value(each_business):
                all_busines_in_category.append(each_business)
    page_result = asyncio.new_event_loop().run_until_complete(create_business_scraping_task(all_busines_in_category))

    for business_result in page_result:
        result[len(result)] = business_result

    return result


def main(category, location):
    category_data = scrape_category(category=category, location=location, pages=20)
    filename = f'{category}.json'
    return save_and_return_file(filename=filename, data=category_data)


main(category=input('category: '), location=input('location: '))  # 'Delivery', 'San Francisco, CA, United States'

