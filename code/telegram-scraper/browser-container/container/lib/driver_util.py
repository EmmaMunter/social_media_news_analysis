from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException
from time import sleep
from lib.storage import ChannelReference
from lib.util import is_valid_tag, assert_is_normal_numeric_id
from datetime import datetime, timezone



# Note: this expects only the tag itself, without the @.
def expect_tag_handle_to_be(tag):
	if not is_valid_tag(tag):
		# This isn't supposed to happen
		raise ValueError(f"Got a malformed tag: '{tag}'.")
	def _closure_expect_tag_handle_to_be(driver):
		details_containers = driver.find_elements(By.CSS_SELECTOR, ".profile-content .sidebar-left-section-content")
		match len(details_containers):
			case 0:
				# TODO: remove this again
				print("0 details containers found.")
				# The sidebar isn't in the DOM. As far as I'm aware, this occurs only upon first loading Telegram, before selecting a chat.
				return False
			case 1:
				# We presumably found it!
				pass
			case _:
				# This isn't supposed to happen. The selector can also match things like the Settings view, but we assume we don't go there.
				raise ValueError(f"Expected 0 or 1 elements maching '.sidebar-left-section-content'. Got {len(details_containers)} matches instead.")
		detail_items = details_containers[0].find_elements(By.CSS_SELECTOR, ".row-title")
		if len(detail_items) < 3:
			raise ValueError(f"Expected 7 sub-elements in the profile detaills view, but found only {len(detail_items)} instead. This probably violates the assumption that the t.me handle is at index [-3].")
		if len(detail_items) != 7:
			print(f"Warning: expected 7 sub-elements in the profile details view, but found {len(detail_items)}. Continuing under the assumption that the t.me handle is still at index [-3].")
		text_content = detail_items[-3].get_attribute('textContent')
		if text_content == f"https://t.me/{tag}":
			return True
		elif text_content[:13] == "https://t.me/":
			# This could be spammy, if navigation to a different chat is slow.
			print(f"Warning: waiting for chat with tag '{tag}', but the profile details URL is still '{text_content}'.")
			return False
		else:
			# TODO: remove this again
			print(f"Expected tag handle 'https://t.me/{tag}', but it was '{text_content}'.")
			# The element was found, but the text content isn't a t.me URL!
			# Note: this occurs (at least), on first initialization, when the textContent seems to initially be ' ' (a single space).
			# So, expectation isn't met yet. Return False.
			return False

			# Note: This could also happen if the assumption of it being at index [-3] is wrong? That case isn't handled (yet) though, and would eventually result in timeout of the WebDriverWait.
			# raise ValueError(f"The fetched element does not contain a t.me URL. Instead, got: '{text_content}'.")
	return _closure_expect_tag_handle_to_be

def expect_numeric_id_to_be(numeric_id):
	assert_is_normal_numeric_id(numeric_id)
	def _closure_expect_numeric_id_to_be(driver):
		person_avatars = driver.find_elements(By.CSS_SELECTOR, ".person-avatar")
		match len(person_avatars):
			case 0:
				# As far as I'm aware, this occurs only upon first loading Telegram, before selecting a chat.
				return False
			case 1:
				pass
			case _:
				# This isn't supposed to happen. No idea what the additional matches would be, so throw an error.
				raise ValueError(f"Expected 0 or 1 elements maching '.person-avatar'. Got {len(person_avatar)} matches instead.")
		peer_id = person_avatars[0].get_attribute("data-peer-id")
		assert_is_normal_numeric_id(peer_id)
		if peer_id == numeric_id:
			return True
		else:
			return False
	return _closure_expect_numeric_id_to_be

def expect_visible_message_bubble(channel_id, message_id):
	assert_is_normal_numeric_id(channel_id)
	assert_is_normal_numeric_id(message_id)
	def _closure_expect_visible_message_bubble(driver):
		bubbles = driver.find_elements(By.CSS_SELECTOR, ".bubble:not(.is-date)")
		for bubble in bubbles:
			try:
				if bubble.get_attribute('data-peer-id') == channel_id and bubble.get_attribute('data-mid') == message_id:
					return True
			except StaleElementReferenceException:
				pass
		return False
	return _closure_expect_visible_message_bubble



def get_main_numeric_id(driver):
	person_avatars = driver.find_elements(By.CSS_SELECTOR, ".person-avatar")
	if len(person_avatars) != 1:
		raise ValueError(f"Expected to find a single '.person-avatar' element, but got {len(person_avatars)} instead.")
	peer_id = person_avatars[0].get_attribute("data-peer-id")
	assert_is_normal_numeric_id(peer_id)
	return peer_id

# Note: this returns the handle *without* the @ prefixed
# Note: When in chat view, this fetches the chat @-tag, NOT the main @-tag!
def get_current_tag_handle(driver):
	details_containers = driver.find_elements(By.CSS_SELECTOR, ".sidebar-left-section-content")
	if len(details_containers) != 1:
		# This isn't supposed to happen. The selector can also match things like the Settings view, but we assume we don't go there.
		# And 0 results should only happen on initialization, when this function isn't supposed to be ran.
		raise ValueError(f"Expected exactly 1 element matching '.sidebar-left-section-content'. Got {len(details_containers)} matches instead.")
	detail_items = details_containers[0].find_elements(By.CSS_SELECTOR, ".row-title")
	if len(detail_items) < 3:
		raise ValueError(f"Expected 7 sub-elements in the profile detaills view, but found only {len(detail_items)} instead. This probably violates the assumption that the t.me handle is at index [-3].")
	if len(detail_items) != 7:
		print(f"Warning: expected 7 sub-elements in the profile details view, but found {len(detail_items)}. Continuing under the assumption that the t.me handle is still at index [-3].")
	text_content = detail_items[-3].text
	if text_content[:13] == "https://t.me/" and is_valid_tag(text_content[13:]):
		return text_content[13:]
	else:
		# The element was found, but the text content isn't a valid t.me URL! Perhaps the assumption of it being at index [-3] was wrong?
		raise ValueError(f"The fetched element does not contain a valid t.me URL. Instead, got: '{text_content}'.")



# Note: you can also pass a webelement as the "driver". As long as the object supports the .find_elements() method, it'll work.
def scroll(driver, repeat=1, delay=1, key=Keys.PAGE_UP):
	bubbles_containers = driver.find_elements(By.CSS_SELECTOR, '.bubbles-inner')
	no_bubbles_containers_found_count = 0
	while len(bubbles_containers) == 0:
		# Note: this didn't seem to ever happen, until a remote deployment, when scraping a channel, using an account that hadn't joined said channel (yet). This might imply some risk to other cade too.

		# Retry it for a while, to see if one pops up later.
		# Note that there might still be a risk of stuff not being loaded thoroughly enough, causing the scrolling to not actually do anything.
		# But, this isn't much of an issue because this function is usually called many times, so no big deal if the first ones don't do anything.
		no_bubbles_containers_found_count += 1
		if no_bubbles_containers_found_count > 60:
			raise ValueError(f"Expected 1 '.bubbles-inner' element to eventually show up, but there are {len(bubbles_containers)}.")
		sleep(1)
		bubbles_containers = driver.find_elements(By.CSS_SELECTOR, '.bubbles-inner')
	if len(bubbles_containers) > 1:
		raise ValueError(f"Expected at most 1 '.bubbles-inner' element, but found {len(bubbles_containers)} instead.")
	scrollable = bubbles_containers[0].find_element(By.XPATH, './..')
	for x in range(repeat):
		scrollable.send_keys(key)
		# ActionChains(driver).scroll_by_amount(0, -300).perform() // Note: none of the scroll_*() methods mentioned in the docs actually exist (yet)
		# ActionChains(driver).send_keys_to_element(scrollable, Keys.PAGE_UP).perform()
		# ActionChains(driver).key_down(Keys.PAGE_DOWN).perform()
		sleep(delay)



def load_page_by_single_reference(driver, base_url, database, target_id) -> ChannelReference:
	if target_id[0] == "@":
		# We have the target @-tag

		if not is_valid_tag(target_id[1:]):
			# This shouldn't ever happen.
			raise ValueError(f"The job contained a malformed @-tag: '{target_id}'.")
		channel = ChannelReference(database, tag_handle=target_id[1:])
	else:
		# We presumably have a numeric tag.
		assert_is_normal_numeric_id(target_id)

		# Fetch the target tag handle from the database
		channel = ChannelReference(database, numeric_id=target_id)
		channel.complete()

	# Load the page with the tag handle (which was fetched from the database if we had a numeric id as target id).
	print(f"Loading page: '{base_url + '#' + '@' + channel.tag_handle}'")
	driver.get(base_url + "#" + "@" + channel.tag_handle)

	# Wait until the profile detail view contains the correct t.me handle
	pre_wait_datetime = datetime.now(timezone.utc)
	WebDriverWait(driver, 300).until(expect_tag_handle_to_be(channel.tag_handle))
	print(f"Waited {(datetime.now(timezone.utc) - pre_wait_datetime).total_seconds()} seconds for the tag_handle to be '{channel.tag_handle}'.")
	sleep(5) # Wait some more just to be safe # TODO: make this wait more smart.

	if target_id[0] == "@":
		# Get the numeric id too, which we didn't know yet
		channel.numeric_id = get_main_numeric_id(driver)

		# Make sure to store this in the database. (Note that this will throw an error if the data differs from what's already in the database).
		channel.store()
	# else: # Note: commented out for now because it seems unneccessary to be extra careful when NOT storing a new numeric tag in the database.
		# # We presumably had a numeric tag.
		# # Just to be sure, also wait until the header avatar element references the correct numeric id.
		# WebDriverWait(driver, 60).until(expect_numeric_id_to_be(channel.numeric_id))
		# sleep(5) # Wait some more just to be safe
		# # Note: this only works because we aren't in a chat replies view yet.
		# tag_handle = get_current_tag_handle()

	return channel



# Note: "driver" can also be a webelement (or other) supporting the .find_elements() method.
def wait_for_bubbles_to_load(driver) -> (bool, bool):
	# Make sure we don't scrape partially-loaded stuff
	# TO-DO: deal with hover changes. (Addendum: nah whatever, I'll just filter them afterwards if needed).
	skip_bubbles_with_stuck_preloaders = False
	stuck_preloader_count = 0
	while len(stuck_preloaders := driver.find_elements(By.CSS_SELECTOR, ".preloader-container")) != 0:
		stuck_preloader_count += 1
		if stuck_preloader_count % 20 == 0 and all(stuck_preloader.location['y'] < 0 for stuck_preloader in stuck_preloaders):
			# There seem to VERY rarely be stuck preloaders even after 60 seconds of waiting. (Happened after scraping 1800 messages already once.)
			# So, treat it the same as the stuck reply thumbnails.
			skip_bubbles_with_stuck_preloaders = True
			break
		if stuck_preloader_count > 60:
			# This is a LOOONG timeout, because it seemed to be quite rare (even when stuck preloaders above y=0 were still disallowed), which means it can interrupt a lot of scraping, and I hope to never trigger it again.
			# If it DOES happen again, I should probably skip_bubbles_with_stuck_preloaders EVEN IF they're not above y=0.

			# Actually, even with a 5 minute timeout, this was reached. Guess I gotta just skip them even if they're below y=0.
			# Note though that this means that those messages might not get scraped at all.
			# raise TimeoutError(f"Expected all elements matching '.preloader-container' to eventually disappear, but {len(stuck_preloaders)} still remain.")
			skip_bubbles_with_stuck_preloaders = True
			break
		sleep(1)
	skip_bubbles_with_not_fully_loaded_thumbnails = False
	stuck_reply_thumbnail_count = 0
	while len(reply_thumbnails := driver.find_elements(By.CSS_SELECTOR, ".reply-media .thumbnail")) != 0:
		stuck_reply_thumbnail_count += 1
		if stuck_reply_thumbnail_count % 10 == 0 and all(reply_thumbnail.location['y'] < 0 for reply_thumbnail in reply_thumbnails):
			# It seems that occasionally these thumbnails don't yet load when they're still very far up (above the current scroll position).
			# So, just assume that these bubbles will be scraped later, and skip them for now.
			skip_bubbles_with_not_fully_loaded_thumbnails = True
			break
		if stuck_reply_thumbnail_count > 60:
			# Note: it turns out that these thumbnails DO occasionally get stuck indefinitely. Oh well. Just skip 'em then.
			# print("Some reply thumbnails seem to stay stuck, even below y=0. Skipping those.")
			# for el in driver.find_elements(By.CSS_SELECTOR, ".reply-media .thumbnail"):
				# print(el.is_displayed())
				# print(el.rect)
				# print(el.location)
			# raise TimeoutError(f"Expected all elements matchin '.reply-media .thumbnail' to eventually disappear, but {len(reply_thumbnails)} still remain.")
			skip_bubbles_with_not_fully_loaded_thumbnails = True
			break
		sleep(1)

	return (skip_bubbles_with_stuck_preloaders, skip_bubbles_with_not_fully_loaded_thumbnails)



# This function is required because Telegram Web K contains broken unicode. (It contains lone surrogates.)
# In order to not cause errors, we need to transport it in a "safe" format (essentially treating the outerHTML as a binary blob).
def get_outer_html(element) -> str:
	# TODO: make sure errors in the injected script are handled? Those are prone to failing silently.

	# # This does: str <- bytes (utf-8) <- b64 <- {transport} <- b64 <- String (codepoints representing bytes) <- UInt8Array (utf-8) <- String
	# # But, turns out it is lossy. Lone surrogates are replaced with the replacement character.
	# return base64.b64decode(element.parent.execute_script('return btoa(String.fromCodePoint(...(new TextEncoder().encode(arguments[0].outerHTML))))', element)).decode('utf-8')

	# This does: str <- generator (yields chr) <- list (int code points) <- {transport} <- Array (int code points) <- String
	return ''.join(chr(codepoint) for codepoint in element.parent.execute_script('return Array.from(arguments[0].outerHTML, character => character.codePointAt(0))', element))
