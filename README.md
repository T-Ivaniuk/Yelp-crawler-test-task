# Yelp-crawler-test-task

`Yelp-crawler-test-task` is a simple, lightweight Python webscraping script, which allows you to scrape Yelp.com categories and save collected data to json file.

## Deploy Locally

- Clone the repo. 
`git clone https://github.com/T-Ivaniuk/Yelp-crawler-test-task.git`

- Open сloned folder
`cd Yelp-crawler-test-task`

- Create virtualenv
`python -m venv venv`

- Activate virtualenv
`.\venv\Scripts\activate`

- Install requirements
`pip install -r requirements.txt`

- add a proxy by filling in the proxies.txt file to bypass blocking due to an excessive number of requests

- Start Yelp crawler by
`python "asynchronous yelp.com scraper.py"`
