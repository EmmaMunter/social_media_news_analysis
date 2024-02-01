#!/usr/bin/env python3

import boto3
import json
import sys
import base64
import binascii
import os
from glob import glob
import gzip
from datetime import datetime, timedelta, timezone
import lzma
import re
from collections import namedtuple, deque
from html.parser import HTMLParser
from typing import Iterable
from hashlib import sha1
from time import sleep

from lib.util import assert_is_normal_numeric_id, assert_is_valid_tag
from lib.jobs import send_job
from lib.storage import Database

DATABASE = Database()

IS_QUICK_RUN = os.environ.get('QUICK_RUN') in ("1", "y", "Y", "yes", "true", "True")
IS_EPHEMERAL_RUN = os.environ.get('EPHEMERAL_RUN') in ("1", "y", "Y", "yes", "true", "True")
IS_LESS_VERBOSE_RUN = os.environ.get('LESS_VERBOSE_RUN') in ("1", "y", "Y", "yes", "true", "True")
REALLY_PUSH_JOBS_TO_QUEUE = os.environ.get('REALLY_PUSH_JOBS_TO_QUEUE') in ("1", "y", "Y", "yes", "true", "True")

# TMP_IGNORED_MESSAGE_IDS = ("-1", "4294975096", "4294975097", "4294976526", "4294977264", "4294980266", "4294985644")
TMP_IGNORED_MESSAGE_IDS = ("-1", "4294975096", "4294975097", "4294976526", "4294977264", "4294980266", "4294985644") + ("4295012371", "4295016042")



class NonMatchingRepliesElementException(ValueError):
	pass
class NestedLinkException(ValueError):
	pass
class FormattingInLinkException(ValueError):
	pass



ChannelPostId = namedtuple('ChannelPostId', ['main_tag_handle', 'message_id'])
ChannelPostData = namedtuple('ChannelPostData', ['outer_html', 'latest_pushed_timestamp'])
class JobParameters(namedtuple('JobParameters', ['numeric_id', 'message_id', 'has_replies_element', 'number_of_comments', 'main_tag_handle', 'latest_pushed_timestamp'])):
	def assert_is_well_formed(self):
		assert_is_normal_numeric_id(self.numeric_id)
		assert_is_normal_numeric_id(self.message_id)
		assert_is_valid_tag(self.main_tag_handle)

BubbleContent = namedtuple('BubbleContent', ['textcontent', 'bubble_type', 'is_reply', 'was_edited', 'filter_reason'])

HTMLData = namedtuple('HTMLData', ['data'])
HTMLRoot = namedtuple('HTMLRoot', ['children'])
# HTMLStartTag = namedtuple('HTMLStartTag', ['tag', 'attrs', 'children'])
# HTMLStartTag.get_attrs = (lambda self, attr_name: [attr[1] for attr in self.attrs if attr[0] == attr_name])
# HTMLStartTag.get_attr = (lambda self, attr_name, must_be_defined=True:
	# found_attrs[0] if (len(found_attrs := self.get_attrs(attr_name)) == 1) else
	# None if (not must_be_defined and len(found_attrs) == 0) else
	# (_ for _ in ()).throw(ValueError(f"Was trying to get a unique attribute, but got {len(found_attrs[0])}."))
# )
# HTMLStartTag.assert_is_bubble = (lambda self:
	# (_ for _ in ()).throw(ValueError(f"Element is not a 'div' but a '{self.tag}'.")) if (self.tag != 'div') else
	# (_ for _ in ()).throw(ValueError(f"Element is not a '.bubble' but a '{self.get_attr('class')}'.")) if ('bubble' not in self.get_attr('class').split()) else
	# None
# )
class HTMLStartTag(namedtuple('HTMLStartTag', ['tag', 'attrs', 'children'])):
	def get_attrs(self, attr_name):
		return [attr[1] for attr in self.attrs if attr[0] == attr_name]
	def get_attr(self, attr_name, must_be_defined=True):
		return (
			found_attrs[0] if (len(found_attrs := self.get_attrs(attr_name)) == 1) else
			None if print(found_attrs) or (not must_be_defined and len(found_attrs) == 0) else
			(_ for _ in ()).throw(ValueError(f"Was trying to get a unique attribute, but got {len(found_attrs[0])} values."))
		)
	def assert_is_bubble(self):
		return (
			(_ for _ in ()).throw(ValueError(f"Element is not a 'div' but a '{self.tag}'.")) if (self.tag != 'div') else
			(_ for _ in ()).throw(ValueError(f"Element is not a '.bubble' but a '{self.get_attr('class')}'.")) if ('bubble' not in self.get_attr('class').split()) else
			None
		)
	def is_class(self, class_name):
		return class_name in self.get_attr('class').split()

class HTMLMatchCriterion(namedtuple('HTMLMatchCriterion', ['type', 'tag', 'regex', 'classes', 'at_any_depth'], defaults=[None, None, None, None, True])):
	def match(self, node):
		return (
			(self.type is None or type(node) == self.type)
			and (self.tag is None or node.tag == self.tag)
			and (self.regex is None or self.regex.match(node.data))
			and (self.classes is None or all(node.is_class(class_name) for class_name in self.classes))
		)
	def get_regex_match(self, node):
		return self.regex.match(node.data)



# Note: this class provides the framework for further subclassing. Subclasses should define .handle_unclosed_element() and .handle_closed_element()
class BubbleHTMLParser(HTMLParser):
	def __init__(self):
		super().__init__()
		self._stack = [HTMLRoot(deque())]
		self.is_closed = False

	def close(self):
		if len(self._stack) != 1:
			print(self._stack)
			raise ValueError(f"Did not recieve an end tag of the top-level element(s)!")
		if type(self._stack[0]) != HTMLRoot:
			# This shouldn't be possible.
			raise Exception("Root level element of wrong type. There's a typo somewhere in the HTML parser code.")
		top_level_elements = self._stack[0].children
		# print('tl elems', 'len: ', len(top_level_elements))
		# for el in top_level_elements:
			# print(' tl elem', el)
		# self.pretty_print(self._stack[0])
		if len(top_level_elements) != 1:
			raise ValueError(f"Expected only 1 top-level node(s), but there were {len(top_level_elements)}.")
		self.is_closed = True
		super().close()

	def reset(self):
		self._stack = [HTMLRoot(deque())]
		self.is_closed = False
		super().reset()

	def handle_starttag(self, tag, attrs):
		# print('  opened', tag, attrs)
		element = HTMLStartTag(tag, attrs, deque())
		if len(self._stack) != 0:
			self._stack[-1].children.append(element)
		self._stack.append(element)

	def handle_endtag(self, tag):
		# print(' closing', tag)
		while type(popped_element := self._stack.pop()) != HTMLStartTag or popped_element.tag != tag:
			# The element is unclosed, so it's "children" are actually children of it's parent instead.
			self._stack[-1].children.extend(popped_element.children)
			popped_element.children.clear() # Note, popped_element is NOT deep-copied, so this alters the value found at _stack[-1].children!

			# self.handle_unclosed_element(popped_element)
		# self.handle_closed_element(popped_element)

	def handle_data(self, data):
		self._stack[-1].children.append(HTMLData(data))

	@classmethod
	def pretty_print(cls, node, depth=0):
		deep_indent_format = '│   '
		child_indent_format = '┝   '
		print(deep_indent_format*(depth-1) + child_indent_format*min(depth, 1), end='')
		if type(node) == HTMLRoot:
			print('#root')
			for child in node.children:
				cls.pretty_print(child, depth=depth+1)
		elif type(node) == HTMLData:
			print('data:', repr(node.data))
		elif type(node) == HTMLStartTag:
			print(f"<{node.tag} attrs={node.attrs}>")
			for child in node.children:
				cls.pretty_print(child, depth=depth+1)
			print(deep_indent_format*depth, end='')
			print(f"</{node.tag}>")
		else:
			# This shouldn't be possible
			print(type(node))
			print(node)
			raise Exception("Got unknown type. There's a typo somewhere.")

	# Note: when the final match criterion includes a regex, the returned iterable will be a dict with the match objects as the .values()
	@classmethod
	def find_elements(cls, node, criteria, criteria_depth=0, collector=None, is_initial_invocation=True) -> Iterable[HTMLData | HTMLRoot | HTMLStartTag]:
		if collector is None:
			if criteria[-1].regex is not None:
				collector = {}
			else:
				collector = []

		if criteria[criteria_depth].match(node):
			if criteria_depth + 1 < len(criteria):
				# print(f"Match for {criteria[criteria_depth]} has been found:\n{node}")
				# Look for matches below this
				if type(node) in (HTMLRoot, HTMLStartTag):
					for child in node.children:
						cls.find_elements(child, criteria, criteria_depth + 1, collector=collector, is_initial_invocation=False)
				elif type(node) == HTMLData:
					pass
				else:
					# This shouldn't be possible
					raise Exception("Got unknown type. There's a typo somewhere.")
			else:
				# We found a match!
				if criteria[criteria_depth].regex is not None:
					if node in collector:
						# This can't happen
						raise ValueError('Got same match through 2 paths in the DOM.')
					collector[node] = criteria[criteria_depth].get_regex_match(node)
				else:
					collector.append(node)
		elif criteria[criteria_depth].at_any_depth and type(node) in (HTMLRoot, HTMLStartTag):
			for child in node.children:
				cls.find_elements(child, criteria, criteria_depth, collector=collector, is_initial_invocation=False)

		if is_initial_invocation:
			return collector

	@classmethod
	def find_element(cls, *args, **kwargs) -> HTMLData | HTMLRoot | HTMLStartTag:
		elements = cls.find_elements(*args, **kwargs)
		if not len(elements) == 1:
			cls.pretty_print(args[0])
			raise NonMatchingRepliesElementException(f"Expected exactly 1 match, but {len(elements)} matches were found.")
		return elements

	def _assert_is_closed(self):
		if not self.is_closed:
			raise ValueError("Expected the parser to have been closed already at this point.")

	def _assert_is_closed_bubble(self):
		self._assert_is_closed()
		if len(self._stack[0].children) != 1:
			# This can't happen
			raise ValueError(f"Validation failed. There's a typo somewhere.")
		if not self._stack[0].children[0].tag == 'div':
			raise ValueError("Top level element is not a 'div'.")
		if not self._stack[0].children[0].is_class('bubble'):
			raise ValueError("Top level element is not a '.bubble'.")

	def is_service_bubble(self):
		self._assert_is_closed_bubble()
		return self._stack[0].children[0].is_class('service')

	@classmethod
	def get_clean_html(cls, el, of_children_only=False):
		NESTED_TAGS = ('em', 'strong', 'u')
		TOP_LEVEL_TAGS = ('a', 'code') + NESTED_TAGS
		text_segment = ''
		if not of_children_only:
			text_segment += '<'+ el.tag + '>'
		for child in el.children:
			if type(child) is HTMLData:
				text_segment += child.data
			elif (type(child) is HTMLStartTag) and (child.tag == 'img') and child.is_class('emoji'):
				text_segment += child.get_attr('alt')
			elif (type(child) is HTMLStartTag) and (child.tag in NESTED_TAGS):
				text_segment += cls.get_clean_html(child)
			elif (type(child) is HTMLStartTag) and child.tag == 'img' and child.is_class('emoji'):
				text_segment += el.get_attr('alt')
			elif (type(child) is HTMLStartTag) and child.tag == 'a':
				raise NestedLinkException()
			elif (type(child) is HTMLStartTag) and child.tag == 'del':
				pass
			elif (type(child) is HTMLStartTag) and child.tag == 'custom-emoji-element' and child.is_class('custom-emoji'):
				text_segment += child.get_attr('data-sticker-emoji')
			elif (type(child) is HTMLStartTag) and child.tag == 'span' and child.is_class('spoiler'):
				raise FormattingInLinkException()
			else:
				# This shouldn't be able to happen
				print()
				print("Unknown child type!")
				cls.pretty_print(el)
				raise ValueError("Got unknown child node type while converting link to text")
		if not of_children_only:
			text_segment += '</'+ el.tag + '>'
		return text_segment

	# Extract the text content of the posted message
	def get_textcontent(self) -> BubbleContent:
		self._assert_is_closed_bubble()
		top_level_element = self._stack[0].children[0]
		if not top_level_element.is_class('bubble'):
			# This can't happen
			raise ValueError("Top level element is not of class '.bubble'")
		# if top_level_element.is_class('channel-post') and top_level_element.is_class('is-in'):
		# 	# This shouldn't ever happen I think
		# 	self.pretty_print(top_level_element)
		# 	raise ValueError("Top level element is of classes '.channel-post' and '.is-in'")
		if top_level_element.is_class('channel-post'):
			bubble_type = 'channel-post'
		elif top_level_element.is_class('is-in'):
			# Presumably! Channel posts are also of the '.is-in' class though!
			bubble_type = 'comment'
		else:
			raise ValueError("Top level element doesn't have the '.is-in' class, which is unexpected")
		
		message = self.find_element(
			self._stack[0],
			[
				HTMLMatchCriterion(
					type=HTMLStartTag,
					tag='div',
					classes=['message'],
				)
			]
		)[0]
		# print()
		# print()
		# self.pretty_print(self._stack[0])
		# print()
		# self.pretty_print(message)
		text_segments = []
		is_reply = False
		was_edited = False
		# filter_counts = {
		# 	'nested_link': 0,
		# 	'i18n': 0,
		# }
		for el in message.children:
			if type(el) is HTMLData:
				text_segments.append(el.data)
				continue
			if type(el) is not HTMLStartTag:
				raise Exception(f"Got an unknown node type: '{type(el)}'")

			# if el.tag == 'a' and len(el.children) == 1 and (type(el.children[0]) is HTMLData) and (el.children[0].data == '/report'):
			# 	pass
			if el.tag == 'a' and el.get_attr('href')[:25] == 'tg://bot_command?command=':
				pass
			elif el.tag == 'a' and el.get_attr('href')[:7] == 'mailto:':
				text_segments.append(self.get_clean_html(el))
			elif el.tag == 'a' and el.is_class('webpage') and el.is_class('quote-like'):
				pass
			elif el.tag == 'a' and el.is_class('btn-primary') and el.is_class('bubble-view-button'):
				pass
			elif el.tag == 'a':
				try:
					text_segment = self.get_clean_html(el)
					text_segments.append(text_segment)
				except NestedLinkException:
					# filter_counts['nested_link'] += 1
					return BubbleContent(None, None, None, None, filter_reason='nested_link')
				except FormattingInLinkException:
					return BubbleContent(None, None, None, None, filter_reason='complex_formatting_in_link')
			elif el.tag in ('em', 'strong', 'code', 'u'):
				try:
					text_segment = self.get_clean_html(el)
					text_segments.append(text_segment)
				except NestedLinkException:
					return BubbleContent(None, None, None, None, filter_reason='formatted_link')
			elif el.tag == 'span' and el.is_class('spoiler') and len(el.children) == 1 and el.children[0].tag == 'span' and el.children[0].is_class('spoiler-text'):
				text_segments.append(self.get_clean_html(el.children[0], of_children_only=True))
			elif el.tag == 'img' and el.is_class('emoji'):
				text_segments.append(el.get_attr('alt'))
			elif el.tag == 'div' and el.is_class('reply') and el.is_class('quote-like'):
				is_reply = True
			elif el.tag == 'del':
				was_edited = True
			elif el.tag == 'custom-emoji-renderer-element' and el.is_class('custom-emoji-renderer'):
				if len(el.get_attrs('data-sticker-emoji')) != 0:
					raise Exception("Didn't expect this sticker element to have an emoji alternative.")
				text_segments.append('❓')
			elif el.tag == 'custom-emoji-element' and (el.get_attr('data-sticker-emoji') is not None):
				text_segments.append(el.get_attr('data-sticker-emoji'))
			elif el.tag == 'span' and el.is_class('time'):
				pass
			elif el.tag == 'reactions-element':
				pass
			elif el.tag == 'div' and el.is_class('web') and len(el.children) == 1 and (type(el.children[0]) is HTMLStartTag) and (el.children[0].tag == 'div') and el.children[0].is_class('quote'):
				pass
			elif el.tag == 'div' and el.is_class('contact'):
				pass
			elif el.tag == 'span' and el.is_class('i18n'):
				# filter_counts['i18n'] += 1
				return BubbleContent(None, None, None, None, filter_reason='i18n')
			elif el.tag == 'blockquote':
				return BubbleContent(None, None, None, None, filter_reason='blockquote')
			elif el.tag == 'div' and el.is_class('document-container') and el.children[0].tag == 'div' and el.children[0].is_class('document-wrapper') and el.children[0].children[0].tag == 'audio-element':
				return BubbleContent(None, None, None, None, filter_reason='audio_document')
			elif el.tag == 'div' and el.is_class('document-container') and len(el.children) > 1 and el.children[1].tag == 'div' and el.children[1].is_class('document-wrapper') and el.children[1].children[0].tag == 'audio-element':
				return BubbleContent(None, None, None, None, filter_reason='audio_document')
			elif el.tag == 'div' and el.is_class('document-container') and el.children[0].tag == 'div' and el.children[0].children[0].tag == 'div' and el.children[0].children[0].is_class('document-message'):
				return BubbleContent(None, None, None, None, filter_reason='document_message')
			elif el.tag == 'div' and el.is_class('geo-footer'):
				return BubbleContent(None, None, None, None, filter_reason='geo_footer')
			elif el.tag == 'poll-element':
				return BubbleContent(None, None, None, None, filter_reason='poll')
			else:
				print("Encountered unknown structure!")
				self.pretty_print(el)
				raise Exception("Encountered unknown structure of message contents.")

		full_textcontent = ''.join(text_segments)
		if any(substring in full_textcontent for substring in ('\x90X࠼',)):
			return BubbleContent(None, None, None, None, filter_reason="probable_lone_surrogate")
		# print("Printing.")
		# print(full_textcontent)
		# print("Printed.")

		return BubbleContent(
			textcontent = ''.join(text_segments),
			bubble_type = bubble_type,
			is_reply = is_reply,
			was_edited = was_edited,
			filter_reason = None,
		)

	# def handle_unclosed_element(self, element):
		# raise NotImplementedError("This method should be overridden.")
	# def handle_closed_element(self, element):
		# raise NotImplementedError("This method should be overridden.")

# TODO: among other things, this class should extract whether this post is a potential target with comments (and the channel_id and message_id too, just for verification).
class ChannelPostHTMLParser(BubbleHTMLParser):
	# def handle_unclosed_element(self, element):
		# # print('unclosed', element)
		# pass
	# def handle_closed_element(self, element):
		# # print('  closed', element)
		# pass

	@staticmethod
	def is_replies_element(node):
		return (
			(type(node) == HTMLStartTag)
			# and ((class_attr := node.get_attr('class', must_be_defined=False)) is not None)
			# and ('replies-element' in class_attr.split())
			and node.tag == 'replies-element'
		)

	@classmethod
	def get_replies_element(cls, node, collector=None, is_initial_invocation=True):
		if collector is None:
			collector = []

		if cls.is_replies_element(node):
			collector.append(node)
		elif type(node) == HTMLRoot:
			for child in node.children:
				cls.get_replies_element(child, collector=collector, is_initial_invocation=False)
		elif type(node) == HTMLData:
			pass
		elif type(node) == HTMLStartTag:
			for child in node.children:
				cls.get_replies_element(child, collector=collector, is_initial_invocation=False)
		else:
			# This shouldn't be possible
			raise Exception("Got unknown type. There's a typo somewhere.")

		if is_initial_invocation:
			# Return the contents of the collector
			if len(collector) > 1:
				print(f"len(collector): {collector}")
				for element in collector:
					cls.pretty_print(element)
				raise ValueError(f"Expected at most 1 '.replies-element', but {len(collector)} were found!")
			elif len(collector) == 1:
				return collector[0]
			elif len(collector) == 0:
				# print("This has no '.replies-element'!", '='*100)
				return None

	@classmethod
	def get_number_of_comments(cls, node):
		# cls.pretty_print(node)
		found_match = cls.find_element(
			node,
			[
				HTMLMatchCriterion(
					type=HTMLStartTag,
					tag='span',
					classes=['replies-footer-text'],
				),
				HTMLMatchCriterion(
					type=HTMLData,
					regex=cls._PATTERN_COMMENT_COUNT,
				),
			]
		)
		captures = found_match.popitem()[1].groups()
		if len(captures) != 1:
			# This can't happen
			raise ValueError("There should have been only 1 capture group. There must be a typo somewhere.")
		if captures[0] is None:
			return 0
		else:
			return int(captures[0])
	_PATTERN_COMMENT_COUNT = re.compile('Leave a comment|([0-9]+) Comments?', flags=re.DOTALL)

	def get_job(self, main_tag_handle, latest_pushed_timestamp) -> JobParameters:
		self._assert_is_closed_bubble()
		replies_element = self.get_replies_element(self._stack[0])
		if replies_element is None:
			number_of_comments = None
		else:
			try:
				number_of_comments = self.get_number_of_comments(replies_element)
			except NonMatchingRepliesElementException:
				number_of_comments = None
		# if replies_element is None:
			# print("No replies element!")
			# self.pretty_print(self._stack[0])
		# else:
			# print("Found a replies element!", '-'*100)

		if len(self._stack[0].children) != 1:
			# This can't happen
			raise ValueError(f"Validation failed. There's a typo somewhere.")
		top_level_element = self._stack[0].children[0]
		top_level_element.assert_is_bubble()
		if not top_level_element.is_class('channel-post'):
			# if top_level_element.is_class('service'):
				# return JobParameters(numeric_id=None, message_id=None, has_replies_element=False)
			# else:
			print(top_level_element)
			if top_level_element.get_attr('data-mid') in TMP_IGNORED_MESSAGE_IDS:
				# Idk entirely why this happens. But this data clearly isn't sanity-chacked by the scraper workers before uploading
				# For now, just ignore these
				# Note: these are also filtered out later.
				pass
			else:
				raise ValueError(f"Expected top level bubble element to be a '.channel-post' (or a '.service'), but it is only: '{top_level_element.get_attr('class')}'.")

		return JobParameters(
			numeric_id = top_level_element.get_attr('data-peer-id'),
			message_id = top_level_element.get_attr('data-mid'),
			has_replies_element = (replies_element is not None),
			number_of_comments = number_of_comments,
			main_tag_handle = main_tag_handle,
			latest_pushed_timestamp = latest_pushed_timestamp,
		)



def extract_main_post_best_version(item, extract_scrape_results=False) -> bytes:
	if extract_scrape_results:
		target_map = 'scraped_comments'
		content_key = 'content'
	else:
		target_map = 'outerHTML_by_hash'
		content_key = 'outerHTML_by_hash'
	# TO-DO: maybe check out actual differences between versions
	chosen = max(item[target_map]['M'].items(), key=(lambda kv: datetime.fromisoformat(kv[1]['M']['timestamp']['S'])))
	outer_html_encoded = chosen[1]['M'][content_key]['B']
	outer_html_compressed = base64.b64decode(outer_html_encoded, validate=True)
	try:
		# TO-DO: remove this. The bug causing twice-encoded base64 data has been fixed, and I should probably just fix that in the database instead of handling it here.
		outer_html_compressed = base64.b64decode(outer_html_compressed, validate=True)
		if not IS_LESS_VERBOSE_RUN:
			print(f"Oh no! Still got twice-encoded base64 data from item with main_tag_handle '{item['main_tag_handle']}' and message_id '{item['message_id']}'.")
	except binascii.Error:
		pass
	outer_html = lzma.decompress(outer_html_compressed)
	if chosen[0] != 'SHA1:' + sha1(outer_html).hexdigest():
		# This can't happen.
		raise ValueError("outerHTML hash does not match the hash value it's stored at.")
	return outer_html

# This stores AND returns the extracted data!
# Note that extract_scrape_results is a clunky way to extract and store the scraped comments instead, for later use in measurement.
def extract_export(source_path, destination_path, extract_scrape_results=False) -> Iterable[tuple[ChannelPostId, ChannelPostData]]:
	if source_path[-1] != '/':
		source_path += '/'
	if destination_path[-1] != '/':
		destination_path += '/'
	if not os.path.exists(source_path):
		raise Exception(f"Error: location '{source_path}' does not exist.")
	if not os.path.exists(destination_path):
		if re.sub('(^|/)s3/', '\\1s3-extract/', source_path) == destination_path:
			# Note: this is for convenience, to not have to manually create these paths. Works for me, but would need to be improved for others.
			os.makedirs(destination_path)
		else:
			raise Exception(f"Error: location '{destination_path}' does not exist.")
	source_filenames = glob(source_path + '*.json.gz')
	if len(source_filenames) < 1:
		raise Exception(f"Could not find source files in directory '{source_path}'.")

	extracted_outer_htmls = {}
	for source_filename in source_filenames:
		print(f"Extracting '{source_filename}'.")
		with gzip.open(source_filename, 'r') as source_file:
			for line in source_file:
				# Get the data
				item = json.loads(line)['Item']
				if 'outerHTML_by_hash' not in item and not extract_scrape_results:
						print(f"Woops: empty item with main_tag_handle '{item['main_tag_handle']['S']}' and message_id '{item['message_id']['S']}'")
						continue
				if 'scraped_comments' not in item and extract_scrape_results:
						# print(f"Woops: empty item with main_tag_handle '{item['main_tag_handle']['S']}' and message_id '{item['message_id']['S']}'")
						continue
				content = extract_main_post_best_version(item, extract_scrape_results=extract_scrape_results)

				if not IS_EPHEMERAL_RUN:
					# Store it in a file for reference and/or further processing
					if extract_scrape_results:
						content_dest_filename = destination_path + item['main_tag_handle']['S'] + '/' + item['message_id']['S'] + '.comments.json'
					else:
						content_dest_filename = destination_path + item['main_tag_handle']['S'] + '/' + item['message_id']['S'] + '.outerHTML.html'
					if not os.path.exists(destination_path + item['main_tag_handle']['S'] + '/'):
						os.mkdir(destination_path + item['main_tag_handle']['S'] + '/')
					with open(content_dest_filename, 'wb') as content_dest_file:
						content_dest_file.write(content)

				if 'queue_push_timestamps' in item:
					latest_pushed_timestamp = max(datetime.fromisoformat(timestamp) for timestamp in item['queue_push_timestamps']['SS'])
				else:
					latest_pushed_timestamp = None

				# Store it in the return dict
				extracted_outer_htmls[ChannelPostId(main_tag_handle=item['main_tag_handle']['S'], message_id=item['message_id']['S'])] = ChannelPostData(
					outer_html = content,
					latest_pushed_timestamp = latest_pushed_timestamp,
				)

				if IS_QUICK_RUN and len(extracted_outer_htmls) >= 50:
					# Return after doing only 1 item.
					return extracted_outer_htmls

	return extracted_outer_htmls

# This returns good target jobs. (I.e., targets that are worth scraping.)
def extract_jobs(extracted_outer_htmls, minimum_number_of_comments) -> Iterable[JobParameters]:
	html_parser = ChannelPostHTMLParser()
	jobs = []
	main_tag_handle_versus_numeric_id_pairs = set()
	print(f"There are {len(extracted_outer_htmls)} items to parse.")
	item_parse_count = 0
	for (main_tag_handle, message_id), (outer_html, latest_pushed_timestamp) in extracted_outer_htmls.items():
		item_parse_count += 1
		if item_parse_count % 1000 == 0:
			print(f"item_parse_count: {item_parse_count}")
		outer_html_str = outer_html.decode('utf-8', errors='surrogatepass')
		html_parser.feed(outer_html_str)
		html_parser.close()
		if html_parser.is_service_bubble():
			if main_tag_handle == 'yug_24_ru' and message_id in ('4294967297', '4294967298'):
				# These two grandfathered in, but should probably just be removed from the database instead.
				# html_parser.pretty_print(html_parser._stack[0])
				pass
			else:
				# This can't happen
				raise ValueError(f"There's a service bubble stored for main_tag_handle '{main_tag_handle}' message_id '{message_id}'. There might be broken data in the database!")
		else:
			job = html_parser.get_job(main_tag_handle, latest_pushed_timestamp)
			if job.message_id == "-1":
				assert_is_normal_numeric_id(job.numeric_id)
				# assert_is_normal_numeric_id(job.message_id)
				assert_is_valid_tag(job.main_tag_handle)
			else:
				job.assert_is_well_formed()
			main_tag_handle_versus_numeric_id_pairs.add((main_tag_handle, job.numeric_id))
			if not message_id == job.message_id:
				# This can't happen, but the error might be in the scraper instead of here..
				raise ValueError(f"The message id in the outerHTML differs from that in the database key.")
			if job.has_replies_element:
				if job.message_id in TMP_IGNORED_MESSAGE_IDS:
					print(job)
					print("Woops! This was supposed to be ignored, and I didn't expect it to have a 'replies-element'!")
					# raise ValueError("Woops! This was supposed to be ignored, and I didn't expect it to have a 'replies-element'!")
				# html_parser.pretty_print(html_parser.get_replies_element(html_parser._stack[0]))
				else:
					jobs.append(job)
		html_parser.reset()

	if len(set(jobs)) != len(jobs):
		# This can't happen.
		raise ValueError("Validation failed. There's a typo somewhere.")
	unique_pair_count = len(main_tag_handle_versus_numeric_id_pairs)
	unique_main_tag_handle_count = len(set(pair[0] for pair in main_tag_handle_versus_numeric_id_pairs))
	unique_numeric_id_count = len(set(pair[1] for pair in main_tag_handle_versus_numeric_id_pairs))
	if unique_main_tag_handle_count != unique_pair_count or unique_numeric_id_count != unique_pair_count:
		# Some tag handles are associated with multiple numeric ids, or vice versa.
		# This shouldn't happen. I can happen if an account changes its tag handle while I'm scraping, but I don't expect that to happen.
		raise ValueError("Validation failed. There's a typo somewhere, or there are inconsistencies in the scraped data.")

	return jobs

def analyze_jobs(jobs):
	# Calculate the stats
	counts = {}
	for job in jobs:
		counts[job.number_of_comments] = counts.get(job.number_of_comments, 0) + 1
	total = len(jobs)
	comment_total = sum(count[0]*count[1] for count in counts.items())
	cumulative = 0
	comment_cumulative = 0
	for count in sorted(counts.items(), key=(lambda x: x[0])):
		cumulative += count[1]
		remaining = total - cumulative
		cumulative_frac = cumulative/total
		remaining_frac = remaining/total

		comment_cumulative += count[0] * count[1]
		comment_remaining = comment_total - comment_cumulative
		comment_cumulative_frac = comment_cumulative/comment_total
		comment_remaining_frac = comment_remaining/comment_total

		print(count[0], count[1], cumulative, '{:.1%}'.format(cumulative_frac), remaining, '{:.1%}'.format(remaining_frac), '', comment_cumulative, '{:.1%}'.format(comment_cumulative_frac), comment_remaining, '{:.1%}'.format(comment_remaining_frac), sep='\t')

def filter_jobs(jobs, minimum_number_of_comments, double_push_if_older_than=timedelta.max) -> Iterable[JobParameters]:
	now = datetime.now(timezone.utc)
	return [
		job for job in jobs if (
			(job.number_of_comments >= minimum_number_of_comments)
			and (job.latest_pushed_timestamp is None or (now - job.latest_pushed_timestamp) > double_push_if_older_than)
			and (job.message_id != TMP_IGNORED_MESSAGE_IDS) # Note: IDK really why this happens. Just filter these out for now. (Note, they're also ignored elsewhere).
		)
	]

def push_jobs(jobs):
	for job in jobs:
		send_job(job_type='scrape_comments', target={'channel_id': job.numeric_id, 'message_id': job.message_id})
		DATABASE.add_to_set(
			'tg-scraper-posts',
			{
				'main_tag_handle': { 'S': job.main_tag_handle },
				'message_id': { 'S': job.message_id },
			},
			'queue_push_timestamps',
			{ 'SS': [ datetime.now(timezone.utc).isoformat() ] },
		)
		sleep(0.1) # Mellow out the burst a bit.

def main():
	if not len(sys.argv) == 3:
		raise ValueError(f"Expected exactly 2 arguments (source and destination path), but got {len(sys.argv)} instead.")

	extracted_outer_htmls = extract_export(sys.argv[1], sys.argv[2])

	extracted_jobs = extract_jobs(extracted_outer_htmls, minimum_number_of_comments=1)

	print(f"Got len(extracted_jobs) extracted_jobs total.")
	extracted_jobs = [job for job in extracted_jobs if job.number_of_comments is not None] # TODO: replace this stop-gap pre-filter by a decent approach
	print(f"There are len(extracted_jobs) extracted_jobs left after removing those with broken comment counts.")

	if not IS_LESS_VERBOSE_RUN:
		analyze_jobs(extracted_jobs)

	chosen_jobs = filter_jobs(extracted_jobs, minimum_number_of_comments=500)

	print(f"Found {len(chosen_jobs)} target jobs!")

	if REALLY_PUSH_JOBS_TO_QUEUE:
		push_jobs(chosen_jobs)

if __name__ == '__main__':
	main()
