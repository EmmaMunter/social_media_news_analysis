#!/usr/bin/env python3

import os
from time import sleep
from tempfile import TemporaryDirectory
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.common.keys import Keys
import subprocess
from hashlib import sha1
from datetime import datetime, timezone, timedelta
import lzma
import json
from collections import OrderedDict
import re

from lib.storage import Database, ChannelReference
from lib.jobs import jobs
from lib.util import is_memory_pressure_high, bubble_outer_html_to_plain_text
from lib.driver_util import scroll, expect_visible_message_bubble, load_page_by_single_reference, wait_for_bubbles_to_load, get_outer_html

DATABASE = Database()

BASE_URL = "https://web.telegram.org/k/"



# Note that target_id can be a numeric id OR an @-tag handle.
# But, do note that numeric IDs must have their tag_handles registered in the database already. (Because there is no way to navigate to channels that haven't been subscribed to by the scraper account).
def scrape_id(driver, target_id, current_job):
	if type(target_id) != str:
		raise TypeError(f"Expected target_id to be a string, instead got a '{type(target_id)}'.")

	print(f"Scraping account with id: '{target_id}'.")

	# Once 12 hours have passed, the job will again become visible in the queue and the receipt handle will time out.
	# So, in order to prevent that, we pre-delete the job after 11 hours.
	# If an error does occur, the job is re-added to the queue, but this is logged clearly.
	# Note: (Because this essentially refreshes the job, this will reset any dead letter queue timeouts! So, make sure to check that jobs don't keep failing and living on infinitely!)
	job_early_delete_time = datetime.now(timezone.utc) + timedelta(hours=11)
	# These job deletions are grandfathered in! TODO: remove this once both are gone.
	if target_id in ("@radovkanews", "@nevzorovtv"):
		print(f"This job has already been finished before! Instead of scraping target_id '{target_id}', make this job a no-op so it just gets deleted.")
		return

	# TO-DO: if the group hasn't been joined (yet), join it. (in util function probably)

	channel = load_page_by_single_reference(driver, BASE_URL, DATABASE, target_id)

	# Fetch message contents
	scraped_messages = {}
	scraped_message_ids = set()
	stale_results_count = 0
	scrape_repeat_count = 0
	while True:
		scroll(driver, repeat=1)

		skip_bubbles_with_stuck_preloaders, skip_bubbles_with_not_fully_loaded_thumbnails = wait_for_bubbles_to_load(driver)

		bubbles = driver.find_elements(By.CSS_SELECTOR, ".bubble:not(.is-date):not(.service)")
		if skip_bubbles_with_not_fully_loaded_thumbnails:
			old_bubbles = bubbles
			bubbles = [bubble for bubble in bubbles if len(bubble.find_elements(By.CSS_SELECTOR, ".reply-media .thumbnail")) == 0]
			print(f"Skipping bubbles with half loaded thumbnails. There are {len(old_bubbles)} bubbles total, but we're scraping only {len(bubbles)} of those.")
			# Note: turns out the below DOES happen. So, just allow this from now on. Note that this allows some messages to not get scraped at all.
			# if any(old_bubble not in bubbles and old_bubble.location['y'] > 0 for old_bubble in old_bubbles):
				# # This should never happen, as this is also ensured when enabling this filter, and isn't expected to change.
				# raise ValueError(f"While skipping bubbles with not fully loaded thumbnails, some bubbles not far up would apparently be skipped.")
		if skip_bubbles_with_stuck_preloaders:
			old_bubbles = bubbles
			bubbles = [bubble for bubble in bubbles if len(bubble.find_elements(By.CSS_SELECTOR, ".preloader-container")) == 0]
			print(f"Skipping bubbles with stuck preloader elements. There are {len(old_bubbles)} bubbles total, but we're scraping only {len(bubbles)} of those.")
			# Note: unlike skip_bubbles_with_not_fully_loaded_thumbnails, we allow skipping bubbles below y=0. This allows some messages to not get scraped.
		stale_results_count += 1
		for bubble in bubbles:
			try:
				outer_html = get_outer_html(bubble)
				message_id = bubble.get_attribute('data-mid')

				if outer_html not in scraped_messages:
					scraped_messages[outer_html] = message_id
					if message_id not in scraped_message_ids:
						# Note: this needs to checked separately because the outerHTML of the message can keep changing indefinitely (when people interact with it).
						# This was added before scraped_messages.values() contained the same data, but this is likely more efficient anyways, so no need to remove it.
						scraped_message_ids.add(message_id)
						stale_results_count = 0
			except StaleElementReferenceException as e:
			# except (StaleElementReferenceException, InvalidArgumentException) as e:
				# Note: This seems to happen quite a lot, weirdly. For now, just ignore this.
				# If this affects bubbles that are currently scrolled out of view (which seems likely), they should get scraped at some point anyways.

				# print(e)
				# print("This failed!")
				# print(bubble)
				# print()
				pass

		# Log the occasional status update
		scrape_repeat_count += 1
		if scrape_repeat_count % 100 == 0:
			print(f"Scraped {len(scraped_messages)} messages so far!")

		if len(scraped_messages) in range(30,100) and not any('replies-element' in outer_html for outer_html in scraped_messages):
			# Backoff from this scraping job if it doesn't appear to have comments. This will still store the scraping results so far in the databse.
			print(f"Scraped {len(scraped_messages)} messages, but '{target_id}' seems to not have comments.")
			break
		if is_memory_pressure_high():
			print("Memory pressure is high. Finishing job.")
			break
		if stale_results_count > 30:
			print("Not finding any new messages. Finishing job.")
			break
		# Note: Telegram WebK is MUCH less of a memory hog than Facebook. So, if there is much history, this scraping could potentially continue for a LONG time.

		if datetime.now(timezone.utc) > job_early_delete_time:
			# We're getting close to the job's visibility timeout!
			# Pre-delete it.
			if not job.has_been_deleted:
				print(f"IMPORTANT! We're pre-deleting job of type '{job.type}' with target '{job.target}'.")
				current_job.delete()
				print(f"Pre-deletion done!")
	print(f"Success! Scraped {len(scraped_messages)} messages.")

	MEMORY_STAT_FILENAME = "/sys/fs/cgroup/memory.stat"
	with open(MEMORY_STAT_FILENAME, 'r') as memory_stat_file:
		memory_usage = next((line[5:] for line in memory_stat_file if line[:5] == 'anon '), None)
		print(f"memory_usage: {memory_usage}")

	# # TO=DO: remove this, as I can just look at the main database
	# with open('/tmp/scraped_messages.json', 'w') as dump_file:
		# json.dump(scraped_messages, dump_file)

	for outer_html, message_id in scraped_messages.items():
		DATABASE.set_nested_attribute(
			'tg-scraper-posts',
			{
				'main_tag_handle': { 'S': channel.tag_handle },
				'message_id': { 'S': message_id },
			},
			'outerHTML_by_hash',
			'SHA1:' + sha1(outer_html.encode('utf-8', errors='surrogatepass')).hexdigest(),
			{'M': {
				'compression': {'S': 'xz'},
				'outerHTML': { 'B': lzma.compress(outer_html.encode('utf-8', errors='surrogatepass'))},
				'timestamp': { 'S': datetime.now(timezone.utc).isoformat() },
			}},
		)

	#TMP-Note: could use use CSS selector: .class[data-peer-id="{NUMERIC_ID}"]
	# WebDriverWait(driver, 60).until(EC.visibility_of_any_elements_located((By.CSS_SELECTOR, "")))

	# Selector '.person-avatar' seems to be the a nice unique thing
	# Get the main OR chat num_id with: document.querySelectorAll('.avatar-full')[0].attributes["data-peer-id"]
	# Get the main num_id (also in chat view) with: document.querySelectorAll('.person-avatar')[0].attributes["data-peer-id"]
	# Get the main OR chat @-tag (e.g. "https://t.me/yug_24_ru") with (replacing the index 4 by calculated -3 maybe? Or filtered by textContent?): document.querySelectorAll('.sidebar-left-section-content')[0].querySelectorAll('.row-title')[4].textContent


	# Quick test to see if any '.bubbles-group' contains multiple '.bubble' elements (turns out they do!): document.querySelectorAll('.bubbles-group').forEach(function(bg){console.log(bg.querySelectorAll('.bubble').length)})
	# Exclude the timestamp bubbles with the selector: '.bubble:not(.is-date)'



# Note: expects the channel_id to start with a minus sign
def scrape_comments(driver, channel_id, message_id, current_job):
	if type(channel_id) != str:
		raise TypeError(f"Expected channel_id to be a string, instead got a '{type(channel_id)}'.")
	if type(message_id) != str:
		raise TypeError(f"Expected message_id to be a string, instead got a '{type(message_id)}'.")

	if channel_id[0] not in ('-', '@'):
		raise ValueError(f"Expected the channel_id to start with a minus or @-sign, but got: '{channel_id}'. Other cases aren't explicitly handled yet. Exiting for safety.")

	job_early_delete_time = datetime.now(timezone.utc) + timedelta(hours=11)

	channel = load_page_by_single_reference(driver, BASE_URL, DATABASE, channel_id)

	# Note: the onus is now on load_page_by_single_reference() to load the correct channel. Still, remember that the script injection will SILENTLY FAIL if there is an issue.
	# # When a channel isn't joined (yet), it first needs to be loaded in some way, before t.me/c/ links with numeric IDs (instead of tag handles) work.
	# # So, first make sure the channel itself is loaded, before navigating to the specific message.
	# if not expect_numeric_id_to_be(channel_id)(driver):
		# # TO-DO: Fix this, because it has to use the tag handle instead of the numeric id
		# driver.get(BASE_URL + "#" + channel_id)
		# WebDriverWait(driver, 60).until(expect_numeric_id_to_be(channel_id))
		# # Note: if this doesn't wait long enough, the script that is injected later could silently fail.
		# # However, expect_numeric_id_to_be() should still be satisfied when already in the chat view of the target channel, so this safety margin should only occur when REALLY changing to a new target channel.
		# sleep(5)

	# Inject code to trigger navigation to a deep message link
	# injection_script = f"link = document.createElement('a'); link.href = 'https://t.me/c/{-channel_id}/{message_id}'; im(link);"
	injection_script = f"link = document.createElement('a'); link.href = 'https://t.me/{channel.tag_handle}/{message_id}'; im(link);"
	# print(f'Injecting script: "{injection_script}"')
	# Note that the injected script fails silently if it doesn't work
	driver.execute_script(injection_script)

	# TODO: gracefully handle this failing, because that'll happen if the injected script silently failed for some reason.
	for timeout_duration in (10,20):
		try:
			WebDriverWait(driver, timeout_duration).until(expect_visible_message_bubble(channel_id, message_id))
		except TimeoutException:
			driver.execute_script(injection_script)
	WebDriverWait(driver, 60).until(expect_visible_message_bubble(channel_id, message_id))

	# TO-DO: if the group hasn't been joined (yet), join it, maybe?

	# Navigate to the comments view
	channel_target_bubbles = driver.find_elements(By.CSS_SELECTOR, f'.bubble:not(.is-date)[data-peer-id="{channel.numeric_id}"][data-mid="{message_id}"]')
	if not len(channel_target_bubbles) == 1:
		raise ValueError(f"Expected to find exactly 1 bubble with peer-id '{channel.numeric_id}' and mid 'message_id', but found {len(channel_target_bubbles)} instead.")
	channel_replies_footers = channel_target_bubbles[0].find_elements(By.CSS_SELECTOR, 'replies-element')
	if not len(channel_replies_footers) == 1:
		raise ValueError(f"Expected to find exactly 1 'replies-element' in the bubble with peer-id '{channel.numeric_id}' and mid 'message_id', but found {len(channel_replies_footers)} instead.")
	channel_replies_footers[0].click()

	sleep(10) # TODO: maybe reduce this?

	no_chat_views_found_count = 0
	while len(target_chat_views := driver.find_elements(By.CSS_SELECTOR, '.chat[data-type="discussion"]')) == 0:
		no_chat_views_found_count += 1
		if no_chat_views_found_count > 30:
			raise ValueError(f"Expected eventually have a comments view appear. But, there are still only {len(target_chat_views)}.")
		sleep(1)
	sleep(3)
	target_chat_views = driver.find_elements(By.CSS_SELECTOR, '.chat[data-type="discussion"]')
	if not len(target_chat_views) == 1:
		raise ValueError(f"Expected to find only one comments view. But, got {len(target_chat_views)} instead.")
	target_chat_view = target_chat_views[0]

	pre_scrape_repeat_count = 0
	while True:
		scroll(target_chat_view)
		main_post_bubbles = target_chat_view.find_elements(By.CSS_SELECTOR, '.bubble:not(.is-date):not(.service).channel-post')
		if len(main_post_bubbles) > 1:
			raise ValueError(f"Expected at most 1 bubble that appears to be the channel post to which the comments belong, but found {len(main_post_bubbles)} instead.")
		if len(main_post_bubbles) == 1 and main_post_bubbles[0].location['y'] > 0:
			# We've reached the top! (The top of the main post).

			# Scroll the final bit to the very top of the view.
			scroll(target_chat_view)

			# Check if this bubble is actually the target post.
			main_post_bubble_saved_from = main_post_bubbles[0].get_attribute('data-saved-from')
			if not main_post_bubble_saved_from == f"{channel_id}_{message_id}":
				raise ValueError(f"Expected the 'data-saved-from' attribute of the main post bubble to be '{channel_id}_{message_id}', but got '{main_post_bubble_saved_from}' instead.")

			# Initialize the ordered list of scraped comments with it.
			scraped_messages = OrderedDict()
			scraped_messages[main_post_bubbles[0].get_attribute('data-mid')] = set([get_outer_html(main_post_bubbles[0])])

			break
		pre_scrape_repeat_count += 1
		if pre_scrape_repeat_count > 600:
			# We seem to be unable to reach the top of the page.
			raise TimeoutError(f"While trying to scrape the channel '{channel_id}' post with id '{message_id}', we were unable to scroll to the top of the comments view.")

	stale_results_count = 0
	scrape_repeat_count = 0
	while True:
		scroll(target_chat_view, key=Keys.PAGE_DOWN)

		# TODO: ditch in a util function
		target_chat_views = driver.find_elements(By.CSS_SELECTOR, '.chat[data-type="discussion"]')
		if not len(target_chat_views) == 1:
			raise ValueError(f"Expected to find exactly one comments view. But, got {len(target_chat_views)} instead.")
		target_chat_view = target_chat_views[0]

		skip_bubbles_with_stuck_preloaders, skip_bubbles_with_not_fully_loaded_thumbnails = wait_for_bubbles_to_load(driver)

		bubbles = target_chat_views[0].find_elements(By.CSS_SELECTOR, '.bubble:not(.is-date):not(.service)')

		tail_bubble_message_id = next(reversed(scraped_messages.keys()))
		uninterrupted_bubbles = OrderedDict()
		current_uninterrupted_bubbles_contain_tail_bubble = False
		uninterrupted_bubbles_is_at_end = False
		for bubble in bubbles:
			skip_this_bubble = False
			if 'outer_html' in globals() or 'message_id' in globals():
				raise Exception("The 'outer_html' or 'message_id' variables are set globally. This is unexpected.")
			if 'outer_html' in locals():
				del outer_html
			if 'message_id' in locals():
				del message_id

			got_message_id = False
			try:
				# print("Trying to get message_id.")
				message_id = bubble.get_attribute('data-mid')
				got_message_id = True
				# print(f"Got message_id: '{message_id}'.")
				outer_html = get_outer_html(bubble)
				# print(f"Got outer_html: '{bubble_outer_html_to_plain_text(outer_html)[:20]}'.")
			except StaleElementReferenceException as e:
			# except (StaleElementReferenceException, InvalidArgumentException) as e:
				skip_this_bubble = True
				# print('='*50)
				# print(e)
				# print('-'*50)
				# import traceback
				# traceback.print_exc(e)
				# print('-'*50)
				# print(e.__notes__)
				# print('+'*50)

			# Note: this is done this way just because it is similar to the way channels are scraped.
			# BUT: if this does encounter a message which completely refuses to fully load, then it will crash the scraping job. I don't expect that to be likely though? We'll see.
			if skip_bubbles_with_not_fully_loaded_thumbnails and len(bubble.find_elements(By.CSS_SELECTOR, ".reply-media .thumbnail")) != 0:
				skip_this_bubble = True
			if skip_bubbles_with_stuck_preloaders and len(bubble.find_elements(By.CSS_SELECTOR, ".preloader-container")) != 0:
				skip_this_bubble = True

			if skip_this_bubble:
				if current_uninterrupted_bubbles_contain_tail_bubble:
					# We've got the intended uninterrupted bubbles. So, continue to storing these.
					break
				else:
					# Reset the found bubbles
					uninterrupted_bubbles = OrderedDict()
					uninterrupted_bubbles_is_at_end = False
			else:
				uninterrupted_bubbles[message_id] = outer_html
				uninterrupted_bubbles_is_at_end = (bubble is bubbles[-1])

			if got_message_id and (message_id == tail_bubble_message_id):
				# We've now gotten to the position we were at, which means the current uninterrupted_bubbles are the ones that we want to store.
				# Note: if this bubble is skipped, that means it'll not actually be included in the uninterrupted_bubbles!
				current_uninterrupted_bubbles_contain_tail_bubble = True

		# TODO: make sure elsewhere that this can't happen
		tail_bubble_is_unloaded_but_presumably_ahead = False
		if not current_uninterrupted_bubbles_contain_tail_bubble:
			if len(uninterrupted_bubbles) > 0 and next(reversed(uninterrupted_bubbles)) in scraped_messages:
				# The tail bubble has been removed (with a delay), but that's because of how far DOWN it was.
				print("We lost track of the latest scraped bubble while scrolling down, but it should still be ahead.")
				tail_bubble_is_unloaded_but_presumably_ahead = True
				uninterrupted_bubbles_is_at_end = False
			else:
				print("We lost track of the latest scraped bubble while scrolling down.")
				print(f"len(uninterrupted_bubbles): {len(uninterrupted_bubbles)}")
				for message_id, outer_html in uninterrupted_bubbles.items():
					print(f"textContent {message_id}: {repr(bubble_outer_html_to_plain_text(outer_html)[:50])}")
				print("This happened while we had this tail bubble:")
				print(f"outer_htmls {tail_bubble_message_id}: {len(scraped_messages[tail_bubble_message_id])}")
				for outer_html in scraped_messages[tail_bubble_message_id]:
					print(f"textContent {tail_bubble_message_id}: {repr(bubble_outer_html_to_plain_text(outer_html)[:50])}")
				raise ValueError("We lost track of the latest scraped bubble while scrolling down.")

		stale_results_count += 1

		if any(message_id in scraped_messages for (message_id, outer_html) in uninterrupted_bubbles.items()):
			# The old tail bubble is contained in uninterrupted_bubbles. So, we must first insert some bubbles into existing scraped messages.
			scraped_messages_iterator = iter(scraped_messages.items())
			first_insertion = True
			while True:
				message_id, outer_html = uninterrupted_bubbles.popitem(last=False)

				(current_scraped_message_message_id, current_scraped_message_outer_html_set) = next(scraped_messages_iterator)
				while current_scraped_message_message_id != message_id:
					if first_insertion:
						(current_scraped_message_message_id, current_scraped_message_outer_html_set) = next(scraped_messages_iterator)
					else:
						# I don't expect this to ever actually happen. But it could potentially happen, if the order in TG Web itself changes (including insertions/deletions between already scraped messages).
						raise ValueError("The order between found messages is inconsistent.")
				first_insertion = False

				# Actually insert the data.
				current_scraped_message_outer_html_set.add(outer_html)

				if (message_id == tail_bubble_message_id) or (tail_bubble_is_unloaded_but_presumably_ahead and len(uninterrupted_bubbles) == 0):
					# We've now reached (and consumed) the old tail bubble. Any remaining messages are not (supposed to be) in scraped_messages yet.
					break
		for message_id, outer_html in uninterrupted_bubbles.items():
			if message_id in scraped_messages:
				raise ValueError(f"We seem to have encountered messages with id '{message_id}' at multiple points in the order of messages. This happened while scraping comments of channel '{channel_id}' message '{message_id}'.")
			scraped_messages[message_id] = set([outer_html])
			stale_results_count = 0

		# Log the occasional status update
		scrape_repeat_count += 1
		if scrape_repeat_count % 10 == 0:
			print(f"Scraped {len(scraped_messages)} comments so far!")

		if is_memory_pressure_high():
			print("Memory pressure is high. Finishing job.")
			break
		if stale_results_count > 30: # TODO: reduce this wait time, maybe? That might be more risky, but the impact of 30 seconds is pretty big.
			print("Not finding any new messages. Finishing job.")
			if not uninterrupted_bubbles_is_at_end:
				# This should only be possible if the final bubble has a stuck thumbnail. If it does happen, that case should be handled specially.
				raise ValueError("We don't seem to have been able to scrape to the end.")
			break

		if datetime.now(timezone.utc) > job_early_delete_time:
			# We're getting close to the job's visibility timeout!
			# Pre-delete it.
			if not job.has_been_deleted:
				print(f"IMPORTANT! We're pre-deleting job of type '{job.type}' with target '{job.target}'.")
				current_job.delete()
				print(f"Pre-deletion done!")

	# Note: we combine these results into a single string, because that makes the compression (much) more efficient.
	combined_results = OrderedDict()
	for message_id, outer_html_set in scraped_messages.items():
		combined_results[message_id] = list(outer_html_set)
	if not combined_results.keys() == scraped_messages.keys():
		# This can't happen. But just to be safe.
		raise Exception("There's a typo in the code that constructs combined_results.")
	combined_results_json = json.dumps(combined_results)

	DATABASE.set_nested_attribute(
		'tg-scraper-posts',
		{
			'main_tag_handle': { 'S': channel.tag_handle },
			'message_id': { 'S': message_id },
		},
		'scraped_comments',
		'SHA1:' + sha1(combined_results_json.encode('utf-8', errors='surrogatepass')).hexdigest(),
		{'M': {
			'compression': {'S': 'xz'},
			'contains_json': {'BOOL': True},
			'content': { 'B': lzma.compress(combined_results_json.encode('utf-8', errors='surrogatepass'))},
			'timestamp': { 'S': datetime.now(timezone.utc).isoformat() },
		}},
	)



def execute(driver, job):
	try:
		match job.type:
			case "scrape_id":
				scrape_id(driver, job.target, job)
			case "scrape_comments":
				scrape_comments(driver, job.target["channel_id"], job.target["message_id"], job)
			case _:
				# Unknown job type, cannot execute it!
				job.reject()
				return
	except Exception as e:
		if job.has_been_deleted:
			# Something has crashed. But the scraping function has pre-deleted the job, probably because it took very long to finish.
			# So, we should re-add it to the queue.
			# Note that this is essentially a new job, which means it will never end up in the dead-letter queue if this keeps happening.
			job.send_new_copy()
		# Don't change anything about the exception itself though.
		raise
	# Job done! Remove it from the queue.
	if job.has_been_deleted:
		# It has been pre-deleted by the scraping function, probably because it took very long to finish.
		# So, no need to actually send a deletion request.
		pass
	else:
		job.delete()

def main():
	# Several things depend on the XDG_RUNTIME_DIR.
	os.environ['XDG_RUNTIME_DIR'] = TemporaryDirectory().name

	# Run a headless xvnc server
	subprocess.Popen(["/usr/bin/Xvnc", ":0", "-geometry", "1440x1080", "-SecurityTypes", "None"])
	os.environ['DISPLAY'] = ":0"
	sleep(1)
	subprocess.Popen(["/usr/bin/openbox"])

	# TO-DO: fetch data.sqlite, instead of depending on it being manually mounted into the container.

	# Start the browser
	sleep(2)
	os.chdir("./tor-browser/")
	subprocess.Popen(["Browser/start-tor-browser", "--detach", "--marionette"])
	sleep(5)
	subprocess.Popen(["/usr/local/bin/geckodriver", "--connect-existing", "--marionette-port", "2828"])
	sleep(10)

	# Connect to the browser
	with webdriver.Remote(command_executor="http://127.0.0.1:4444") as driver:
		if os.environ.get('DEV_MODE') in ("1", "y", "Y", "yes", "true", "True"):
			global _DEV_MODE_DRIVER
			_DEV_MODE_DRIVER = driver
		# driver.get silently fails before the Tor proxy is up, so wait for it to work
		print("Waiting on the Tor proxy...")
		for i in range(60):
			driver.get(BASE_URL)
			sleep(1)
			current_url = driver.current_url
			if current_url.startswith("https://web.telegram.org/"):
				break
			elif current_url == "about:tor":
				pass
			else:
				print(f"The current URL is unexpected! The current URL is '{current_url}'")

		print("Loading Telegram...")
		sleep(2) # TO-DO: maybe make it wait in a smart way

		if os.environ.get('DROP_TO_DEBUG') in ("1", "y", "Y", "yes", "true", "True"):
			import pdb
			pdb.set_trace()
			print("Woops! Continued. Sleeping just in case.")
			sleep(86400*100)
			print("Went 100 days without being killed! Exiting anyways.")
			return

		try:
			for job in jobs():
				print(f"Got job: [{job.type}] [{job.target}]")
				execute(driver, job)
				if os.path.exists('/tmp/_FLAG_QUIT_AFTER_CURRENT_JOB') and not os.path.exists('/tmp/_FLAG_QUIT_AFTER_CURRENT_JOB_OVERRIDE'):
					print('Finished the current job. Exiting.')
					try:
						os.remove('/tmp/_FLAG_QUIT_AFTER_CURRENT_JOB')
					except:
						# We couldn't delete the flag. So, create the override flag instead.
						with open('/tmp/_FLAG_QUIT_AFTER_CURRENT_JOB_OVERRIDE') as file_handle:
							pass
					return
		except Exception as e:
			if not os.environ.get('DEV_MODE') in ("1", "y", "Y", "yes", "true", "True"):
				raise
			import traceback
			traceback.print_exc()
			# print("In dev mode. Sleeping.")
			print("Dropping to debugger.")
			import pdb
			pdb.set_trace()
			print("Woops! Continued. Sleeping just in case.")
			sleep(86400*100)
			print("Went 100 days without being killed! Exiting anyways.")
		print("Finished all jobs. Exiting.")

		if os.environ.get('DEV_MODE') in ("1", "y", "Y", "yes", "true", "True"):
			# Wait for 100 days, so I can manually look at the state/results
			sleep(86400*100)
			print("Went 100 days without being killed! Exiting anyways.")

if __name__ == "__main__":
	main()
