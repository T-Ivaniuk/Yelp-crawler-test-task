import scrapy
import random
import json

def get_proxy():
    with open('proxy_file.txt') as f:
        proxys = f.readlines()
        return proxys


def post_ads_filter(post):
    try:
        return post['searchResultBusiness']['isAd']
    except KeyError:
        return True


def extract_page_index(value, step):
    if value == 0:
        return 0
    else:
        return value / step


def extract_review_fileds(review: json):
    reviewer_name = review['user']['markupDisplayName']
    reviewer_location = review['user']['displayLocation']
    review_date = review['localizedDate']
    return {
        'Reviewer name': reviewer_name,
        'Reviewer location': reviewer_location,
        'Review date': review_date
    }


def get_next_page_url(response):
    current_page_url = response.url
    pagination_value_index = current_page_url.index('&start=') + 7
    unformatted_page_index = int(float(current_page_url[pagination_value_index:]))
    formatted_page_index = extract_page_index(value=unformatted_page_index, step=10)
    next_page_index = (formatted_page_index + 1) * 10
    next_page = current_page_url[:pagination_value_index] + str(next_page_index)
    return next_page


class CategorySpider(scrapy.Spider):
    domain_url = 'http://www.yelp.com'
    name = 'category'
    category = 'Electricians'
    location = 'Ohio, NY, United States'
    reviews_to_extract = 5
    start_urls = [f'https://www.yelp.com/search/snippet?find_desc={category}&find_loc={location}&start={0}']

    def parse(self, response, **kwargs):
        json_search_response = json.loads(response.text)
        raw_businesses_data = json_search_response['searchPageProps']['mainContentComponentsListProps']
        filtered_businesses_data = [x for x in raw_businesses_data if post_ads_filter(x) is False]
        for business in filtered_businesses_data:

            business_name = business['searchResultBusiness']['name']
            business_rating = business['searchResultBusiness']['rating']
            business_reviews = business['searchResultBusiness']['reviewCount']
            yelp_url = self.domain_url + business['searchResultBusiness']['businessUrl']
            item = {
                'Business name': business_name,
                'Business rating': business_rating,
                'Number of reviews': business_reviews,
                'Business yelp url': yelp_url,
            }

            biz_id = business['bizId']
            url = f'https://www.yelp.com/biz/{biz_id}/props'

            yield scrapy.Request(url=url, meta={"proxy": random.choice(get_proxy())},
                                 callback=self.parse_profile_json_response,
                                 cb_kwargs=dict(item=item, yelp_url=yelp_url))

        if len(filtered_businesses_data) == 10:
            next_page = get_next_page_url(response)
            yield response.follow(next_page, callback=self.parse)

    def parse_profile_json_response(self, response, item):
        profile_json = json.loads(response.text)
        reviews = [extract_review_fileds(x) for x in
                   profile_json['bizDetailsPageProps']['reviewFeedQueryProps']['reviews'][0:self.reviews_to_extract]]
        item['Reviews']: reviews
        yield item


