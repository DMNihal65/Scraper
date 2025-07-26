#!/usr/bin/env python3
"""
github_actions_job_scraper.py

Automated job scraper optimized for GitHub Actions that:
1) Scrapes "Data Engineer" jobs from LinkedIn (Bangalore, India, last 24h, entry/associate)
2) Filters with Gemini API for backend skills
3) Avoids duplicates using Git-tracked JSON files
4) Sends email notifications for new jobs
5) Runs via GitHub Actions cron schedule

Requirements:
  • Python ≥3.9
  • Chrome/Chromium + ChromeDriver (installed by GitHub Actions)
  • pip install linkedin-jobs-scraper google-genai

GitHub Secrets Required:
  GEMINI_API_KEY     ← your Google Gemini API key
  GMAIL_EMAIL        ← your Gmail address
  GMAIL_APP_PASSWORD ← your Gmail app password
"""

import os
import json
import logging
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import TimeFilters, ExperienceLevelFilters
from google import genai
from google.genai import types

# Configure logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# File paths for Git-tracked storage
PERSISTENT_DIR = Path("job_data")
PERSISTENT_DIR.mkdir(exist_ok=True)
ALL_JOBS_FILE = PERSISTENT_DIR / "all_jobs_history.json"
FILTERED_JOBS_FILE = PERSISTENT_DIR / "filtered_jobs_history.json"
LAST_RUN_FILE = PERSISTENT_DIR / "last_run.json"
DAILY_STATS_FILE = PERSISTENT_DIR / "daily_stats.json"


class GitHubActionsJobScraper:
    def __init__(self):
        self.jobs = []
        self.gemini_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        self.email_config = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'email': os.getenv("GMAIL_EMAIL"),
            'password': os.getenv("GMAIL_APP_PASSWORD")
        }

    def load_json_file(self, file_path):
        """Load JSON file, return empty list if file doesn't exist"""
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning(f"Could not load {file_path}: {e}")
                return []
        return []

    def save_json_file(self, data, file_path):
        """Save data to JSON file"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(data) if isinstance(data, list) else 'data'} items to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save {file_path}: {e}")

    def get_existing_job_ids(self, job_list):
        """Extract job IDs from existing job list"""
        return {job.get('job_id') for job in job_list if job.get('job_id')}

    def on_data(self, data: EventData):
        """Callback for scraped job data"""
        try:
            date_val = getattr(data.date, "isoformat", lambda: str(data.date))()
            self.jobs.append({
                "job_id": data.job_id,
                "title": data.title,
                "company": data.company,
                "location": data.place,
                "date": date_val,
                "link": data.link,
                "description": data.description,
                "scraped_at": datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error processing job data: {e}")

    def on_error(self, err):
        logger.error(f"Scraping error: {err}")

    def on_end(self):
        logger.info(f"Scraping completed: {len(self.jobs)} jobs collected")

    def scrape_jobs(self):
        """Scrape jobs from LinkedIn"""
        logger.info("Starting job scraping...")
        self.jobs = []

        try:
            scraper = LinkedinScraper(
                chrome_executable_path=None,
                headless=True,
                max_workers=1,
                slow_mo=2.0,  # Slower for stability
                page_load_timeout=90,
            )

            scraper.on(Events.DATA, self.on_data)
            scraper.on(Events.ERROR, self.on_error)
            scraper.on(Events.END, self.on_end)

            query = Query(
                query="Data Engineer",
                options=QueryOptions(
                    locations=["Bangalore, India"],
                    limit=50,
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
            logger.info(f"Successfully scraped {len(self.jobs)} jobs")
            return self.jobs

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            return []

    def filter_with_gemini(self, jobs_to_filter):
        """Filter jobs using Gemini API"""
        if not jobs_to_filter:
            logger.info("No jobs to filter")
            return []

        filtered = []

        # Define JSON schema
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

        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=schema
        )

        # Process jobs in smaller chunks for better reliability
        chunk_size = 8  # Smaller chunks for GitHub Actions
        for i in range(0, len(jobs_to_filter), chunk_size):
            chunk = jobs_to_filter[i:i + chunk_size]

            prompt = f"""
            You receive a JSON array of LinkedIn job postings. Filter and return only those that meet ALL criteria:

            1. Role Title: Contains "Data Engineer", "Software Engineer", "Backend Developer", "Full Stack Developer", or similar software development roles

            2. Skills Match: Mentions at least 2-3 of these skills (case-insensitive):
               Python, ReactJS, VueJS, Postgres, SQL, AWS, Azure, DevOps, Docker, Kubernetes, PySpark, ETL, ELT, Node.js, FastAPI, Django, Flask

            3. Experience Level: 0-2 years experience ("entry level", "freshers", "0-2 years", "1 year", etc.)

            Return only the JSON array of filtered job objects, no extra text.

            Jobs: {json.dumps(chunk)}
            """

            try:
                resp = self.gemini_client.models.generate_content(
                    model="gemini-2.5-pro",
                    contents=prompt,
                    config=config
                )
                subset = json.loads(resp.text)
                filtered.extend(subset)
                logger.info(f"Chunk {i // chunk_size + 1}: {len(subset)} jobs passed filter")

            except Exception as e:
                logger.warning(f"Gemini filtering failed for chunk {i // chunk_size + 1}: {e}")
                continue

        # Remove duplicates
        unique_filtered = {job["job_id"]: job for job in filtered}.values()
        logger.info(f"Total filtered jobs: {len(list(unique_filtered))}")
        return list(unique_filtered)

    def update_daily_stats(self, new_jobs_count, new_filtered_count):
        """Update daily statistics"""
        today = datetime.now().strftime('%Y-%m-%d')
        daily_stats = self.load_json_file(DAILY_STATS_FILE)

        # Find or create today's entry
        today_stats = None
        for stats in daily_stats:
            if stats.get('date') == today:
                today_stats = stats
                break

        if not today_stats:
            today_stats = {
                'date': today,
                'runs': 0,
                'total_scraped': 0,
                'total_filtered': 0,
                'runs_detail': []
            }
            daily_stats.append(today_stats)

        # Update stats
        today_stats['runs'] += 1
        today_stats['total_scraped'] += new_jobs_count
        today_stats['total_filtered'] += new_filtered_count
        today_stats['runs_detail'].append({
            'time': datetime.now().strftime('%H:%M'),
            'scraped': new_jobs_count,
            'filtered': new_filtered_count
        })

        # Keep only last 30 days
        daily_stats = daily_stats[-30:]

        self.save_json_file(daily_stats, DAILY_STATS_FILE)
        return today_stats

    def send_email_notification(self, new_jobs_count, new_filtered_count, new_filtered_jobs, today_stats):
        """Send email notification"""
        if not all([self.email_config['email'], self.email_config['password']]):
            logger.warning("Email credentials not configured")
            return

        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['email']
            msg['To'] = self.email_config['email']
            msg['Subject'] = f"🤖 Job Scraper Report - {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC"

            # Load total counts
            all_jobs_total = len(self.load_json_file(ALL_JOBS_FILE))
            filtered_jobs_total = len(self.load_json_file(FILTERED_JOBS_FILE))

            body = f"""
🤖 GitHub Actions Job Scraper Report
{'=' * 50}

📅 Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC

📊 This Run Results:
• New Jobs Scraped: {new_jobs_count}
• New Filtered Jobs: {new_filtered_count}

📈 Today's Summary:
• Total Runs Today: {today_stats['runs']}
• Jobs Scraped Today: {today_stats['total_scraped']}
• Filtered Jobs Today: {today_stats['total_filtered']}

💾 Database Totals:
• All Jobs in Database: {all_jobs_total}
• Filtered Jobs in Database: {filtered_jobs_total}

"""

            if new_filtered_count > 0:
                body += f"\n🎯 New Filtered Jobs Found:\n{'-' * 40}\n"
                for i, job in enumerate(new_filtered_jobs[:3], 1):  # Show first 3
                    body += f"""
{i}. 💼 {job.get('title', 'N/A')}
   🏢 Company: {job.get('company', 'N/A')}
   📍 Location: {job.get('location', 'N/A')}
   🔗 Link: {job.get('link', 'N/A')}
   📅 Posted: {job.get('date', 'N/A')}

"""
                if len(new_filtered_jobs) > 3:
                    body += f"... and {len(new_filtered_jobs) - 3} more jobs!\n"
            else:
                body += "\n😔 No new filtered jobs found in this run.\n"

            body += f"""
⏰ Next scheduled run: In ~6 hours
🔧 Powered by: GitHub Actions + Gemini AI
📊 View full data: Check your GitHub repository's job_data/ folder

Happy job hunting! 🚀
"""

            msg.attach(MIMEText(body, 'plain'))

            # Attach new jobs if any
            if new_filtered_count > 0:
                attachment = MIMEBase('application', 'octet-stream')
                attachment_data = json.dumps(new_filtered_jobs, indent=2, ensure_ascii=False)
                attachment.set_payload(attachment_data.encode('utf-8'))
                encoders.encode_base64(attachment)
                attachment.add_header(
                    'Content-Disposition',
                    f'attachment; filename="new_jobs_{datetime.now().strftime("%Y%m%d_%H%M")}.json"'
                )
                msg.attach(attachment)

            # Send email
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['email'], self.email_config['password'])
            server.send_message(msg)
            server.quit()

            logger.info("✅ Email notification sent successfully")

        except Exception as e:
            logger.error(f"❌ Failed to send email: {e}")

    def run_scraping_cycle(self):
        """Run complete scraping cycle"""
        logger.info("=" * 60)
        logger.info("🚀 Starting GitHub Actions job scraping cycle")
        logger.info("=" * 60)

        try:
            # Load existing data
            all_jobs_history = self.load_json_file(ALL_JOBS_FILE)
            filtered_jobs_history = self.load_json_file(FILTERED_JOBS_FILE)

            existing_all_ids = self.get_existing_job_ids(all_jobs_history)
            existing_filtered_ids = self.get_existing_job_ids(filtered_jobs_history)

            logger.info(f"📚 Loaded {len(all_jobs_history)} existing jobs, {len(filtered_jobs_history)} filtered")

            # Scrape new jobs
            scraped_jobs = self.scrape_jobs()

            if not scraped_jobs:
                logger.warning("⚠️ No jobs scraped, ending cycle")
                # Still send notification
                today_stats = self.update_daily_stats(0, 0)
                self.send_email_notification(0, 0, [], today_stats)
                return

            # Filter out duplicates
            new_jobs = [job for job in scraped_jobs if job['job_id'] not in existing_all_ids]
            logger.info(f"🆕 Found {len(new_jobs)} new jobs out of {len(scraped_jobs)} scraped")

            if not new_jobs:
                logger.info("ℹ️ No new unique jobs found")
                today_stats = self.update_daily_stats(0, 0)
                self.send_email_notification(0, 0, [], today_stats)
                return

            # Filter with Gemini
            new_filtered_jobs = self.filter_with_gemini(new_jobs)

            # Remove already existing filtered jobs
            truly_new_filtered = [
                job for job in new_filtered_jobs
                if job['job_id'] not in existing_filtered_ids
            ]

            logger.info(f"✨ Found {len(truly_new_filtered)} new filtered jobs")

            # Update histories
            all_jobs_history.extend(new_jobs)
            filtered_jobs_history.extend(truly_new_filtered)

            # Save updated data
            self.save_json_file(all_jobs_history, ALL_JOBS_FILE)
            self.save_json_file(filtered_jobs_history, FILTERED_JOBS_FILE)

            # Update run info
            run_info = {
                "timestamp": datetime.now().isoformat(),
                "new_jobs": len(new_jobs),
                "new_filtered": len(truly_new_filtered),
                "total_jobs": len(all_jobs_history),
                "total_filtered": len(filtered_jobs_history),
                "github_action_run": True
            }
            self.save_json_file(run_info, LAST_RUN_FILE)

            # Update daily stats and send notification
            today_stats = self.update_daily_stats(len(new_jobs), len(truly_new_filtered))
            self.send_email_notification(len(new_jobs), len(truly_new_filtered), truly_new_filtered, today_stats)

            logger.info("✅ Scraping cycle completed successfully")
            logger.info(f"📊 Final counts - New: {len(new_jobs)}, Filtered: {len(truly_new_filtered)}")

        except Exception as e:
            logger.error(f"❌ Error in scraping cycle: {e}")
            self.send_error_notification(str(e))
            raise  # Re-raise to fail the GitHub Action

    def send_error_notification(self, error_msg):
        """Send error notification"""
        if not all([self.email_config['email'], self.email_config['password']]):
            return

        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['email']
            msg['To'] = self.email_config['email']
            msg['Subject'] = f"🚨 Job Scraper ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

            body = f"""
🚨 GitHub Actions Job Scraper Error
{'=' * 40}

⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC
❌ Error: {error_msg}

🔧 Action Required:
1. Check GitHub Actions logs
2. Verify environment variables
3. Check LinkedIn/Gemini API status

🔗 GitHub Actions: https://github.com/YOUR_USERNAME/YOUR_REPO/actions
"""

            msg.attach(MIMEText(body, 'plain'))

            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['email'], self.email_config['password'])
            server.send_message(msg)
            server.quit()

            logger.info("🚨 Error notification sent")

        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")


def main():
    """Main function for GitHub Actions"""
    logger.info("🎬 Starting GitHub Actions Job Scraper")

    # Validate environment variables
    required_vars = ['GEMINI_API_KEY', 'GMAIL_EMAIL', 'GMAIL_APP_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"❌ Missing environment variables: {missing_vars}")
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    # Create and run scraper
    scraper = GitHubActionsJobScraper()
    scraper.run_scraping_cycle()

    logger.info("🎉 GitHub Actions job scraper completed successfully!")


if __name__ == "__main__":
    main()