from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from time import sleep
from sys import stderr
from os import environ
import re

def is_memory_pressure_high():
	# The Javascript VM crashes when it reaches its memory limit (4GB in Firefox/Chrome)
	# During testing, the total "anon" value would reach ~6GB, when the JS VM crashes and Selenium hangs.
	# So, this function checks if we're approaching that limit.
	MEMORY_STAT_FILENAME = "/sys/fs/cgroup/memory.stat"
	MEMORY_PRESSURE_LIMIT = 4_000_000_000

	with open(MEMORY_STAT_FILENAME, 'r') as memory_stat_file:
		memory_usage = next((line[5:] for line in memory_stat_file if line[:5] == 'anon '), None)
		if memory_usage is None:
			# Somehow couldn't read the file. This should never happen.
			raise Exception(f"Could not read 'anon' value from {MEMORY_STAT_FILE} for memory pressure check.")
		return int(memory_usage) > MEMORY_PRESSURE_LIMIT



def clean_url(url):
	# Remove any query parameters
	end = url.find('?')
	if end > 0:
		url = url[:end]
	return url



def scroll(driver, repeat=1, delay=1):
	# body = driver.find_element(By.TAG_NAME, 'body')
	# body.send_keys(Keys.PAGE_DOWN)
	for x in range(repeat):
		ActionChains(driver).key_down(Keys.PAGE_DOWN).perform()
		sleep(delay)

def click(search_anchor, role, startswith=None, regex=None, multiple=False, interval=0.5, might_not_exist=False):
	# 'search_anchor' can be the driver or a container element.
	elements = search_anchor.find_elements(By.CSS_SELECTOR, f'[role="{role}"]')
	if startswith:
		elements = tuple(el for el in elements if el.text.startswith(startswith))
	if regex:
		prog = re.compile(regex)
		elements = tuple(el for el in elements if prog.match(el.text))

	if might_not_exist and len(elements) == 0:
		return False
	if not multiple and len(elements) != 1:
		raise Exception(f'Expected 1 element, but got {len(elements)}')

	for element in elements:
		element.click()
		sleep(interval)
	return True

# Assumes a target page has been loaded
def ensure_not_banned(driver):
	elements = driver.find_elements(By.CSS_SELECTOR, '[role="button"]')
	text_contents = set(element.text for element in elements)
	# Note: the "Other options you may have" isn't always there I think, it seems to only show for some jurisdictions.
	if {"Read more about this rule", "How we made this decision", "Disagree with decision"}.issubset(text_contents):
		# Log the ban, and stop scraping
		print(f"[ERROR] Account has been banned! ({environ['ACCOUNT_UUID']})", file=stderr)
		quit()
