import string
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



# Note: this expects only the tag itself, and will return false if it the @ is included.
def is_valid_tag(tag):
	return len(tag) >= 5 and set(tag).issubset(is_valid_tag._ALLOWED_CHARS)
is_valid_tag._ALLOWED_CHARS = frozenset(string.ascii_lowercase + string.digits + '_')

def assert_is_valid_tag(tag):
	if not is_valid_tag(tag):
		raise ValueError(f"The tag '{tag}' is not a valid tag handle.")

# # Note: this expects a string representing the numeric id.
# def is_valid_numeric_id(numeric_id):
	# try:
		# int(numeric_id)
	# except:
		# return False
	# if int(numeric_id) in range(-1<<64,1<<64):
		# return True
	# else:
		# return False

def assert_is_normal_numeric_id(numeric_id):
	int(numeric_id)
	if int(numeric_id).bit_length() > 64:
		# Numeric IDs have at most 52 significant bits, so there's something wrong here.
		# Numeric IDs seem to often be (up to) 33 bits long though. (Presumably sign + 32 bits value).
		raise ValueError(f"The numeric ID '{numeric_id}' seems to be a weirdly large number. This is likely an incorrect value.")
	elif int(numeric_id).bit_length() < 10:
		# This seems to be a very small number. Perhaps it's a magic value?
		# The Numeric ID of (one?) Telegram Service Notifications account is already 777000.
		raise ValueError(f"The numeric ID '{numeric_id}' seems to be weirdly small. This likely does not refer to a target account.")



# Note: this is crude way to remove known stuff that isn't text content.
# And, there's no 1 true way to get plain text. For example, there's no way to tell that text is clickable URL, as the <a> tags get removed around it.
# TO-DO: for the analysis, I should probably use an actual html parser, because that'll allow me to just select the '.message' element. Any gunk outside of that (like the '.peer-title', '.bubble-name-rank', and the 1 stray '.tgico') are automatically excluded then.
def bubble_outer_html_to_plain_text(outer_html) -> str:
	if not type(outer_html) == str:
		raise TypeError(f"Expected outer_html to be a string, but got a '{type(outer_html)}'.")

	outer_html = bubble_outer_html_to_plain_text._PATTERN_PLAIN_EMOJI_CAPTURE_GROUP.sub('\\1', outer_html)
	outer_html = bubble_outer_html_to_plain_text._PATTERN_REACTIONS_ELEMENTS.sub('', outer_html)
	outer_html = bubble_outer_html_to_plain_text._PATTERN_PEER_TITLE.sub('', outer_html)
	outer_html = bubble_outer_html_to_plain_text._PATTERN_BUBBLE_NAME_RANK.sub('', outer_html)
	outer_html = bubble_outer_html_to_plain_text._PATTERN_TGICO.sub('', outer_html)
	outer_html = bubble_outer_html_to_plain_text._PATTERN_ALL_TAGS.sub('', outer_html)

	return outer_html
bubble_outer_html_to_plain_text._PATTERN_PLAIN_EMOJI_CAPTURE_GROUP = re.compile('<img [^>]* alt="([^"]*)"[^>]*>', flags=re.DOTALL) # The plaintext emojis are the alt-text fallback of the vendor emoji images. Be sure to EXTRACT, NOT REMOVE these!
bubble_outer_html_to_plain_text._PATTERN_REACTIONS_ELEMENTS = re.compile('<reactions-element .+?>.+?</reactions-element.*?>', flags=re.DOTALL) # These are the emoji replies, because those contain the reply counts.
bubble_outer_html_to_plain_text._PATTERN_PEER_TITLE = re.compile('<span class="peer-title"[^>]*>.+?</span.*?>', flags=re.DOTALL) # The name of the sender
bubble_outer_html_to_plain_text._PATTERN_BUBBLE_NAME_RANK = re.compile('<span class="bubble-name-rank"[^>]*>.+?</span.*?>', flags=re.DOTALL) # Says "channel" on (some?) '.channel-post's.
bubble_outer_html_to_plain_text._PATTERN_TGICO = re.compile('<span class="tgico"[^>]*>.+?</span.*?>', flags=re.DOTALL) # These are just icons of the web app. They seem to contain only a placeholder tho it seems?
bubble_outer_html_to_plain_text._PATTERN_ALL_TAGS = re.compile('<.+?>', flags=re.DOTALL) # Catch-all. This is just the 'textContent', essentially.
