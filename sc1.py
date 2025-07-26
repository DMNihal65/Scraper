#!/usr/bin/env python3
"""
scrape_and_filter_jobs.py

Scrapes “Software Engineer” jobs from LinkedIn (Bangalore, India, last 24h, entryassociate),
then filters them via Google Gemini API for backend skills (Python, Spark) and ~1–2 yr experience,
chunking requests to stay within token limits.

Requirements:
  - Python ≥3.9
  - Chrome/Chromium + matching Chromedriver in PATH
  - pip install linkedin-jobs-scraper google-genai

Environment variables:
  LI_AT_COOKIE   ← your LinkedIn li_at cookie
  GEMINI_API_KEY ← your Google Gemini API key
"""

import os
import json
import logging
from datetime import datetime, timedelta
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import TimeFilters, ExperienceLevelFilters
from google import genai

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("li:scraper")

# Configure Gemini client
genai_client = genai.Client(api_key="AIzaSyB0S0J-e72-xgLTy0wYFYrlbH2JpI4EGB8")

jobs = []

def on_data(data: EventData):
    # Normalize date
    date_val = data.date.isoformat() if hasattr(data.date, "isoformat") else data.date
    jobs.append({
        "job_id":      data.job_id,
        "title":       data.title,
        "company":     data.company,
        "location":    data.place,
        "date":        date_val,
        "link":        data.link,
        "description": data.description,
    })

def on_error(error):
    logger.error("Scraping error: %s", error)

def on_end():
    logger.info("Scraping completed: %d jobs collected", len(jobs))

def scrape_jobs():
    scraper = LinkedinScraper(
        chrome_executable_path=None,
        headless=True,
        max_workers=1,
        slow_mo=1.0,
        page_load_timeout=60,
        cookies={"li_at": os.getenv("LI_AT_COOKIE")}
    )
    scraper.on(Events.DATA, on_data)
    scraper.on(Events.ERROR, on_error)
    scraper.on(Events.END, on_end)

    query = Query(
        query="Software Engineer",
        options=QueryOptions(
            locations=["Bangalore, India"],
            limit=100,
            filters=QueryFilters(
                time=TimeFilters.DAY,
                experience=[
                    ExperienceLevelFilters.ENTRY_LEVEL,
                    ExperienceLevelFilters.ASSOCIATE
                ]
            )
        )
    )
    scraper.run([query])

def filter_with_gemini(input_path: str, output_path: str, chunk_size: int = 20):
    with open(input_path, "r") as f:
        all_jobs = json.load(f)

    filtered = []
    # Process in chunks
    for i in range(0, len(all_jobs), chunk_size):
        chunk = all_jobs[i : i + chunk_size]
        prompt = {
            "model": "gemini-2.5-flash",
            "contents": [
                {
                    "parts": [
                        {"text": (
                            "You receive a JSON array of LinkedIn job postings. "
                            "Return only those entries where:\n"
                            "- The job is backend-development oriented.\n"
                            "- The description mentions one or more skills: Python, Spark.\n"
                            "- The role targets candidates with ~1–2 years of experience.\n"
                            "Respond with a JSON array of the filtered jobs.\n"
                            "Jobs JSON:\n"
                        )},
                        {"text": json.dumps(chunk, indent=0)}
                    ]
                }
            ]
        }
        resp = genai_client.models.generate_content(**prompt)
        try:
            subset = json.loads(resp.text)
            filtered.extend(subset)
        except json.JSONDecodeError:
            logger.warning("Failed to parse chunk %d–%d", i, i+chunk_size)
    # Deduplicate by job_id
    unique = {job["job_id"]: job for job in filtered}.values()
    with open(output_path, "w") as f:
        json.dump(list(unique), f, indent=2)
    logger.info("Filtered %d jobs saved to %s", len(unique), output_path)

if __name__ == "__main__":
    scrape_jobs()
    with open("all_jobs.json", "w") as out:
        json.dump(jobs, out, indent=2)
    logger.info("Saved raw jobs to all_jobs.json")

    filter_with_gemini("all_jobs.json", "filtered_jobs.json")
    logger.info("Done. See filtered_jobs.json")
