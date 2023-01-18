import time
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
import requests
import json

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 '
                  'Safari/537.36'}


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


class Business:
    def __init__(self, json_obj, reviews_to_extract=5):
        self._domain_url = 'http://www.yelp.com'
        self.reviews_to_extract = reviews_to_extract
        self._search_data = json_obj
        self._bizId = self._get_business_id()
        self._profile_data = self._get_profile_json()
        self.yelp_url = self.get_yelp_url()

    def _get_profile_json(self):
        return requests.get(url=f'https://www.yelp.com/biz/{self._bizId}/props', headers=headers).json()

    def _get_business_id(self):
        return self._search_data.get('bizId')

    def get_name(self):
        return self._search_data['searchResultBusiness'].get('name')

    def get_rating(self):
        return self._search_data['searchResultBusiness'].get('rating')

    def get_yelp_url(self):
        return self._domain_url + self._search_data['searchResultBusiness'].get('businessUrl')

    def get_review_count(self):
        self.reviews_count = self._profile_data['bizDetailsPageProps']['reviewFeedQueryProps']['pagination'][
            'totalResults']
        return self.reviews_count

    def get_review_data(self):
        if self.reviews_count < self.reviews_to_extract:
            self.reviews_to_extract = self.reviews_count
        first_n_reviews = self._profile_data['bizDetailsPageProps']['reviewFeedQueryProps']['reviews'][
                          0:self.reviews_to_extract]
        return [extract_review_fileds(x) for x in first_n_reviews]

    def _parse_url_from_json_obj(self):
        try:
            redirection_url = self._profile_data['bizDetailsPageProps']['bizPortfolioProps']['ctaProps']['website']
            return extract_url_from_redirection(redirection_url)
        except (TypeError, KeyError):
            return None

    def _parse_url_from_html(self):
        try:
            response = requests.get(url=self.yelp_url, headers=headers).text
            soup = BeautifulSoup(response, 'lxml')
            redirection_url = soup.find('p', text='Business website').next_sibling.a['href']
            return extract_url_from_redirection(redirection_url)
        except AttributeError:
            return None

    def get_business_page(self):
        website_url_via_json = self._parse_url_from_json_obj()
        if website_url_via_json is not None:
            return website_url_via_json
        website_url_via_html = self._parse_url_from_html()
        if website_url_via_html is not None:
            return website_url_via_html
        return 'Not exist on webpage'

    def collect_business_data(self):
        return {
            'Business name': self.get_name(),
            'Business rating': self.get_rating(),
            'Number of reviews': self.get_review_count(),
            'Business yelp url': self.yelp_url,
            'Business website': self.get_business_page(),
            'Reviews': self.get_review_data()
        }


class Category:
    def __init__(self, category, location, pages=1, pause=0):
        self.category = category
        self.location = location
        self.pages = pages
        self.pause = pause
        self.data_to_save = {}
        self.page_count = self.calculate_page_count()
        self.filename = self.get_filename()

    def calculate_page_count(self):
        return self.pages * 10

    def get_filename(self):
        return "".join(self.category) + ".json"

    def iterate_over_pages(self):
        for page in range(0, self.page_count, 10):
            print(f"Page: {int(page / 10)}")
            page_url = f"https://www.yelp.com/search/snippet?find_desc={self.category}&find_loc={self.location}&" \
                       f"start={page}"  # &parent_request_id=5d2c0313dcc86ea8&request_origin=user
            try:
                response = requests.get(url=page_url).json()
            except ValueError:
                raise ValueError('Website response: request denied')
            self.collect_data(response=response)

    def collect_data(self, response):
        props = response['searchPageProps']['mainContentComponentsListProps']
        business_in_page = [x for x in props if x.get('bizId') is not None]
        for index, business in enumerate(business_in_page):
            business = Business(json_obj=business)
            business_data = business.collect_business_data()
            self.data_to_save[len(self.data_to_save)] = business_data
            print(index, business_data)
            time.sleep(self.pause)

    def save_to_file(self):
        with open(self.filename, 'w') as outfile:
            json.dump(self.data_to_save, outfile, indent=4)

    def return_file(self):
        return open(self.filename, mode='r')


def main(category, location):
    scraping_category = Category(category=category, location=location, pages=1)
    scraping_category.iterate_over_pages()
    scraping_category.save_to_file()
    return scraping_category.return_file()


main(category=input('category: '), location=input('location: '))
