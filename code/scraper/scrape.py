#!/usr/bin/env python3

import sys
import os
import itertools
import re
import lzma
import base64
from time import sleep
from tempfile import TemporaryDirectory
from shutil import copyfile
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By

from lib.storage import Database, Account
from lib.jobs import jobs
from lib.util import is_memory_pressure_high, clean_url, scroll, click, ensure_not_banned

DATABASE = Database()
SCRAPING_ACCOUNT = Account(DATABASE)

BASE_URL = "https://www.facebook.com/"



def scrape_outlet(driver, account_name, search_query=None):
	driver.get("about:blank")
	# Load page, and wait for previous page to garbage collect.
	sleep(30)
	if search_query:
		driver.get(BASE_URL + 'page/' + account_name + '/search/?q=' + search_query)
	else:
		driver.get(BASE_URL + account_name)
	sleep(10)
	ensure_not_banned(driver)

	# When looking at an account page, the entire page is also a "main" element, so get the last one.
	posts_container = driver.find_elements(By.CSS_SELECTOR, '[role="main"]')[-1]
	
	scraped_posts = set()
	
	unfruitful_count = 0
	for i in itertools.count():
		scroll(driver, 5)
		elements = posts_container.find_elements(By.CSS_SELECTOR, f'a[href^="{BASE_URL}{account_name}/posts/"]')
		posts = set(clean_url(element.get_attribute("href")) for element in elements)
		if posts <= scraped_posts:
			# No new posts found
			unfruitful_count += 1
			if unfruitful_count > 20:
				# Give up
				break
		else:
			scraped_posts |= posts
			unfruitful_count = 0
		print(len(scraped_posts))

		if is_memory_pressure_high():
			print("Memory pressure is high. Finishing job.")
			break

	if search_query:
		tag = {
			"path": "search_query",
			"value": search_query,
		}
	else:
		tag = None
	for scraped_post in scraped_posts:
		DATABASE.touch_item(
			'scraper-posts',
			{
				"account": { "S": account_name },
				"post_id": { "S": re.sub(f'^{BASE_URL}{account_name}/posts/', '', scraped_post) },
			},
			tag=tag,
		)

def scrape_post(driver, account_name, post_id):
	driver.get(BASE_URL + account_name + '/posts/' + post_id)
	sleep(10)
	ensure_not_banned(driver)

	# Open the "Most relevant" menu
	click(driver, "button", startswith="Most relevant")
	# Switch it to "All comments"
	sleep(1)
	click(driver, "menuitem", startswith="All comments")

	# Get the target post container, if there are other posts shown below it.
	# feed_posts = driver.find_elements(By.CSS_SELECTOR, '[role="feed"]')
	# if len(feed_posts) > 0:
		# container = feed_posts[0].parent.parent.find_elements(By.CSS_SELECTOR, ':first-child')
	# Assumes the first post link is in the first post (the timestamp).
	container_link = driver.find_elements(By.CSS_SELECTOR, f'a[href^="{BASE_URL}{account_name}/posts/"]')[0]
	container = container_link.find_element(By.XPATH, '.' + '/..'*14)
	if len(container.find_elements(By.CSS_SELECTOR, '[role="feed"]')) != 0:
		raise Exception("container is too high-level: it contains [role=\"feed\"] elements (other suggested posts).")

	while click(container, "button", regex="View( [0-9]+)? more comments", might_not_exist=True):
		sleep(1)
	while click(container, "button", regex="[0-9]+ Repl(y|ies)|View( all)?( [0-9]+)?( more| previous)? repl(y|ies)", multiple=True, might_not_exist=True):
		sleep(1)
	while click(container, "button", startswith="See more", multiple=True, might_not_exist=True):
		sleep(1)

	# note: main has been replaced by 'container'
	# mains = driver.find_elements(By.CSS_SELECTOR, '[role="main"]')
	# assert len(mains) == 1
	# main = mains[0]

	DATABASE.set_attribute(
		'scraper-posts',
		{
			"account": { "S": account_name },
			"post_id": { "S": post_id },
		},
		'outerHTML',
		{'M': {
			'compression': {'S': 'xz'},
			'content': {'B': base64.b64encode(lzma.compress(container.get_attribute('outerHTML').encode('utf-8')))},
		}},
	)



def execute(driver, job):
	match job.type:
		case "scrape_outlet":
			scrape_outlet(driver, job.target)
		case "scrape_outlet_search":
			scrape_outlet(driver, job.target["account"], search_query=job.target["search_query"])
		case "scrape_post":
			scrape_post(driver, job.target["account"], job.target["post_id"])
		case _:
			# Unknown job type, cannot execute it!
			job.reject()
			return
	# Job done! Remove it from the queue.
	job.delete()

def main():
	with TemporaryDirectory() as profile_dir:
		# copyfile(f"data/accounts/cookies/{ACCOUNT_UUID}.cookies.sqlite", profile_dir + "/cookies.sqlite")
		with open(profile_dir + "/cookies.sqlite", 'wb') as cookiefile:
			cookiefile.write(SCRAPING_ACCOUNT.cookiefile)
		options = Options()
		options.add_argument("--headless")
		options.add_argument("--profile")
		options.add_argument(profile_dir)
		with webdriver.Firefox(options=options) as driver:
			for job in jobs():
				print(f"Got job: [{job.type}] [{job.target}]")
				execute(driver, job)
			print("Finished all jobs. Exiting.")

if __name__ == "__main__":
	main()
