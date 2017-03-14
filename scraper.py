import os
# hack to override sqlite database filename
# see: https://help.morph.io/t/using-python-3-with-morph-scraperwiki-fork/148
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

import requests
import scraperwiki
import time
from datetime import datetime

MORPH_API_KEY = os.environ['MORPH_MORPH_API_KEY']
SLACK_WEBHOOK_URL = os.environ['MORPH_SLACK_WEBHOOK_URL']
BASE_URL = 'https://api.morph.io/wdiv-scrapers/'
STATIONS_QUERY = '/data.json?query=select%20timestamp%2Ccontent_hash%2Ccouncil_id%20from%20%27history%27%20WHERE%20%60table%60%3D%27stations%27%20ORDER%20BY%20timestamp%3B'
DISTRICTS_QUERY = '/data.json?query=select%20timestamp%2Ccontent_hash%2Ccouncil_id%20from%20%27history%27%20WHERE%20%60table%60%3D%27districts%27%20ORDER%20BY%20timestamp%3B'
RESOURCES_QUERY = '/data.json?query=select%20*%20from%20%27resources%27%3B'
IGNORE_LIST = ['dc-base-scrapers', 'dc-meta-scraper']


class GitHubWrapper:

    def get_repo_list(self):
        res = requests.get('https://api.github.com/users/wdiv-scrapers/repos?per_page=1000')
        if res.status_code != 200:
            res.raise_for_status()

        res_json = res.json()
        repositories = []
        for repo in res_json:
            if repo['name'] not in IGNORE_LIST:
                repositories.append(repo['name'])

        return repositories


class MorphWrapper:

    def query(self, repo, query):
        url = "%s%s%s&key=%s" % (BASE_URL, repo, query, MORPH_API_KEY)
        res = requests.get(url)
        if res.status_code != 200:
            res.raise_for_status()
        return res.json()


class TimeHelper:

    @staticmethod
    def parse_timestamp(timestamp, tz=True):
        if tz:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f+00:00')
        else:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')


class MorphReport:

    def __init__(self, github_wrapper, morph_wrapper):
        self.github = github_wrapper
        self.morph = morph_wrapper
        self.report = []
        self.slack_messages = []

    def summarise_history(self, history, repo_name, entity):
        # Summarise some info from the history table
        result = {}
        first_hash = history[0]['content_hash']
        last_changed = TimeHelper.parse_timestamp(history[0]['timestamp'])
        last_hash = first_hash

        result['started_polling'] = last_changed

        changes = 0
        for record in history:
            if record['content_hash'] != last_hash:
                last_changed = TimeHelper.parse_timestamp(record['timestamp'])
                last_hash = record['content_hash']
                changes = changes + 1
        result['council_id'] = history[-1]['council_id']

        result['changes'] = changes
        result['last_changed'] = last_changed

        if len(history) == 1:
            self.slack_messages.append('New scraper found: <https://morph.io/wdiv-scrapers/%s/> (%s)' % (repo_name, entity))
        else:
            if history[-2]['content_hash'] != history[-1]['content_hash']:
                self.slack_messages.append(
                    'New content hash for scraper <https://morph.io/wdiv-scrapers/%s/> (%s) at %s - check your import script' %
                    (repo_name, entity, str(last_changed))
                )

        return result

    def report_history_query(self, repo_name, query, entity):
        # Query history table and report some summary info
        try:
            history = self.morph.query(repo_name, query)
            if len(history) > 0:
                record = self.summarise_history(history, repo_name, entity)
                record['scraper'] = repo_name
                record['entity'] = entity
                return record
        except requests.exceptions.HTTPError:
            return None

    def full_report(self):
        """
        Generate a report about data we are scraping on morph
        Has any of it changed recently? Is any of it stale?
        """
        repositories = gh.get_repo_list()
        for repo in repositories:
            print('Scraper: %s' % (repo))

            record = self.report_history_query(
                repo, STATIONS_QUERY, 'stations')
            if record is not None:
                self.report.append(record)
            time.sleep(2)

            record = self.report_history_query(
                repo, DISTRICTS_QUERY, 'districts')
            if record is not None:
                self.report.append(record)
            time.sleep(2)

        return self.report


class PollingBot:

    def post_slack_messages(self, messages):
        for message in messages:
            r = requests.post(SLACK_WEBHOOK_URL, json={ "text": message })


gh = GitHubWrapper()
morph = MorphWrapper()
mr = MorphReport(gh, morph)
data = mr.full_report()

# save data to DB
scraperwiki.sqlite.execute("DROP TABLE IF EXISTS report;")
scraperwiki.sqlite.commit_transactions()

for row in data:
    scraperwiki.sqlite.save(
        unique_keys=['scraper', 'entity'],
        data=row,
        table_name='report')
    scraperwiki.sqlite.commit_transactions()

# post Slack updates
bot = PollingBot()
bot.post_slack_messages(mr.slack_messages)
