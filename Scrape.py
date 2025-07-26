import logging
import json
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters

logging.basicConfig(level=logging.INFO)

results = []

def on_data(data: EventData):
    # Removed date filtering because data.date_text is empty in your case
    job_dict = {
        "title": data.title,
        "company": data.company,
        "company_link": data.company_link,
        "date_posted": data.date,
        "date_text": data.date_text,  # will be empty as per your logs
        "job_link": data.link,
        "insights": data.insights,
        "description_length": len(data.description),
        "description": data.description
    }
    results.append(job_dict)

def on_metrics(metrics: EventMetrics):
    print('[ON_METRICS]', metrics)

def on_error(error):
    print('[ON_ERROR]', error)

def on_end():
    print(json.dumps(results, indent=2))

scraper = LinkedinScraper(
    headless=True,
    max_workers=1,
    slow_mo=1.0,
    page_load_timeout=60
)

scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

queries = [
    Query(
        query='Software Engineer',
        options=QueryOptions(
            locations=['Bangalore, India'],
            limit=50,
            filters=QueryFilters(
                relevance=RelevanceFilters.RECENT,
                time=TimeFilters.DAY,  # keep this to get recent jobs
                type=[TypeFilters.FULL_TIME, TypeFilters.INTERNSHIP],
                experience=[ExperienceLevelFilters.ENTRY_LEVEL, ExperienceLevelFilters.ASSOCIATE],
            )
        )
    ),
]

scraper.run(queries)
