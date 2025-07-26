#!/usr/bin/env python3
"""
scrape_and_filter_jobs.py

1) Scrape “Software Engineer” jobs from LinkedIn (Bangalore, India, last 24h, entry/associate).
2) Send exactly 10 jobs per Gemini call for backend‐skills filtering (Python, Spark).
3) Save raw (`all_jobs.json`) and filtered (`filtered_jobs.json`) outputs.

Requirements:
  • Python ≥3.9
  • Chrome/Chromium + matching Chromedriver in PATH
  • pip install linkedin-jobs-scraper google-genai

Environment variables:
  LI_AT_COOKIE     ← your LinkedIn li_at cookie
  GEMINI_API_KEY   ← your Google Gemini API key
"""

import os
import json
import logging
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import TimeFilters, ExperienceLevelFilters
from google import genai
from google.genai import types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Gemini client
# client = genai.Client(api_key="AIzaSyB0S0J-e72-xgLTy0wYFYrlbH2JpI4EGB8")

jobs = []

def on_data(data: EventData):
    # Normalize date
    date_val = getattr(data.date, "isoformat", lambda: data.date)()
    jobs.append({
        "job_id":      data.job_id,
        "title":       data.title,
        "company":     data.company,
        "location":    data.place,
        "date":        date_val,
        "link":        data.link,
        "description": data.description,
    })

def on_error(err):
    logger.error("Scraping error: %s", err)

def on_end():
    logger.info("Scraping completed: %d jobs collected", len(jobs))

def scrape_jobs():
    scraper = LinkedinScraper(
        chrome_executable_path=None,
        headless=True,
        max_workers=1,
        slow_mo=1.0,
        page_load_timeout=60,
        # cookies={"li_at": os.getenv("LI_AT_COOKIE")}
    )
    scraper.on(Events.DATA, on_data)
    scraper.on(Events.ERROR, on_error)
    scraper.on(Events.END, on_end)

    query = Query(
        query="Data Engineer",
        options=QueryOptions(
            locations=["Bangalore, India"],
            limit=20,
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

def filter_with_gemini(input_path: str, output_path: str):
    with open(input_path, "r") as f:
        all_jobs = json.load(f)

    # Initialize empty list for filtered results
    filtered = []

    # Define a minimal JSON schema: an array of objects with at least job_id
    schema = types.Schema(
        type=types.Type.ARRAY,
        items=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "job_id": types.Schema(type=types.Type.STRING)
            },
            required=["job_id"],
        )
    )

    # Prepare a config that forces JSON output
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=schema
    )

    # Create the Gemini client once
    client = genai.Client(api_key="AIzaSyCylLPmz1Kr2j_FVrhdTUn_yWcZiy1rcKg")

    # Send two fixed chunks of 10 jobs each
    for start in (0, 10):
        chunk = all_jobs[start : start + 10]
        if not chunk:
            continue

        prompt = f"""
        You receive a JSON array of LinkedIn job postings. Each posting includes fields: job_id, title, company, location, date, link, and description.

        Filter and return only those postings that meet ALL of the following criteria:

        1. Role Title: Contains one of:
           • “Data Engineer”
           • “Software Engineer”
           • “Associate Software Developer”
           • “Backend Developer”
           • “Full Stack Developer”
           or any software development roles dont stress to much on this not so imporant becasue titles may variy but focus on the skills and experince

        2. Skills Match: Mentions at least **two-three** of these skills (case-insensitive):
           Python, ReactJS, VueJS, Postgres, SQL, AWS, Azure, DevOps, Docker, Kubernetes, PySpark, ETL, ELT, Node.js, FastAPI

        3. Experience Level: Specifies or implies **0–2 years** of experience (e.g., “0–2 years,” “entry level,” “freshers,” “1 year,” “up to 2 years”)

        Respond with a JSON array containing only the filtered job objects. Do not include any extra text or explanation—only the JSON array.

        Jobs JSON:
        {json.dumps(chunk)}
        """

        try:
            resp = client.models.generate_content(
                model="gemini-2.5-pro",
                contents=prompt,
                config=config
            )
            # The SDK will validate against our schema and return JSON text
            subset = json.loads(resp.text)
            filtered.extend(subset)
            logger.info("Chunk %d–%d returned %d jobs", start, start+10, len(subset))
        except Exception as e:
            logger.warning("Chunk %d–%d failed: %s", start, start+10, e)

    # Deduplicate by job_id
    unique_jobs = {job["job_id"]: job for job in filtered}.values()

    with open(output_path, "w") as f:
        json.dump(list(unique_jobs), f, indent=2)

    logger.info("Filtered %d unique jobs saved to %s", len(unique_jobs), output_path)

if __name__ == "__main__":
    scrape_jobs()
    with open("all_jobs.json", "w") as out:
        json.dump(jobs, out, indent=2)
    logger.info("Saved raw jobs to all_jobs.json")

    filter_with_gemini("all_jobs.json", "filtered_jobs.json")
    logger.info("Done. See filtered_jobs.json")
