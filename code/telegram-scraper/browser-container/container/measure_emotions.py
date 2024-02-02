#!/usr/bin/env python3

# Note: to run this, FIRST run send_comment_jobs in non-ephemeral mode. That'll extract much of the data.
#       Actually, that's no longer necessary I think.

import os
import sys
from glob import glob
import json
from collections import namedtuple
import base64
import pickle
from urllib import request
from hashlib import sha1
from datetime import datetime, timezone
from itertools import product
import numpy as np
from math import inf

BERT_MODELS = ('bert-base-multilingual-cased','DeepPavlov/rubert-base-cased')
LLAMA_MODELS = ()

MODELS = BERT_MODELS + LLAMA_MODELS

HEURISTICS = (
	".\n" + "anger level: [MASK]",
	".\n" + "contempt level: [MASK]",
)

LABELS = ('high', 'mid', 'low', 'positive', 'neutral', 'negative', 'medium')

LABEL_GROUPS = (
	('high', 'mid', 'low'),
)

# For safety, ensure we don't accidentally trigger something in send_comment_jobs
if os.environ.get('REALLY_PUSH_JOBS_TO_QUEUE') in ("1", "y", "Y", "yes", "true", "True"):
	raise Exception('measurement code should not REALLY_PUSH_JOBS_TO_QUEUE')

from send_comment_jobs import DATABASE, IS_QUICK_RUN, IS_EPHEMERAL_RUN, IS_LESS_VERBOSE_RUN, REALLY_PUSH_JOBS_TO_QUEUE, extract_export, BubbleHTMLParser, ChannelPostHTMLParser

FULL_RUN = os.environ.get('FULL_RUN') in ("1", "y", "Y", "yes", "true", "True")
DONT_UNPICKLE = os.environ.get('DONT_UNPICKLE') in ("1", "y", "Y", "yes", "true", "True")
STORE_ANALYSIS_CACHE = os.environ.get('STORE_ANALYSIS_CACHE') in ("1", "y", "Y", "yes", "true", "True")
USE_ANALYSIS_CACHE = os.environ.get('USE_ANALYSIS_CACHE') in ("1", "y", "Y", "yes", "true", "True")


FEATURES = [
	lambda text: (
		'украин' in text.lower()
		and any(alt in text.lower() for alt in ('евреи', 'eврейск', 'иудаизм', 'еврей', 'иудей'))	
	),
	lambda text: (
		'украин' in text.lower()
		and any(alt in text.lower() for alt in ('евреи', 'eврейск', 'иудаизм', 'еврей', 'иудей'))
		and not any(alt in text.lower() for alt in ('израил', 'газа', 'палестин'))
	),
	lambda text: (
		'россия' in text.lower()
		and any(alt in text.lower() for alt in ('евреи', 'eврейск', 'иудаизм', 'еврей', 'иудей'))	
	),
	lambda text: (
		'россия' in text.lower()
		and any(alt in text.lower() for alt in ('евреи', 'eврейск', 'иудаизм', 'еврей', 'иудей'))
		and not any(alt in text.lower() for alt in ('израил', 'газа', 'палестин'))
	),
	lambda text: (
		'украин' in text.lower()
		and any(alt in text.lower() for alt in ('чеченцы', 'чеченская', 'чечня', 'чеченец'))
	),
	lambda text: (
		'россия' in text.lower()
		and any(alt in text.lower() for alt in ('чеченцы', 'чеченская', 'чечня', 'чеченец'))	
	),
]

ANALYSIS_FEATURES = [
	
]


QUICK_CACHE_PREFIX = '../../data/.QUICK_CACHE'


MINIMIZATION_STEPS_COUNT = 6
MINIMIZATION_STEPS = tuple(
	(np.exp(np.pi*1j/MINIMIZATION_STEPS_COUNT*STEP).real, np.exp(np.pi*1j/MINIMIZATION_STEPS_COUNT*STEP).imag) for STEP in range(MINIMIZATION_STEPS_COUNT)
)
STEP_SIZE_TARGET = 0.0001


RawMeasurement = namedtuple('RawMeasurement', ['textcontent', 'logits'])
UnifiedMeasurement = namedtuple('UnifiedMeasurement', ['textcontent', 'unified_logits'])


def glob_files(pattern):
	globbed_filenames = glob(pattern, root_dir=sys.argv[2])
	if len(globbed_filenames) < 1:
		raise Exception(f"Could not find cached files matching glob '{pattern}' in directory '{sys.argv[2]}'.")
	return globbed_filenames

def must_rebuild_cache(flag):
	filename_flag = sys.argv[2] + '/.CACHED_' + flag
	return (FULL_RUN or not os.path.exists(filename_flag))

def rebuild_cache_pre(flag):
	filename_flag = sys.argv[2] + '/.CACHED_' + flag
	FULL_RUN = True
	if IS_EPHEMERAL_RUN:
		raise Exception(f"Cannot rebuild '{flag}' cache in ephemeral mode.")
	try:
		os.remove(filename_flag)
	except FileNotFoundError:
		pass

def rebuild_cache_post(flag):
	filename_flag = sys.argv[2] + '/.CACHED_' + flag
	if not IS_EPHEMERAL_RUN and not IS_QUICK_RUN:
		# Create the file
		with open(filename_flag, 'a'):
			pass


def quick_cache_store(obj, suffix):
	with open(QUICK_CACHE_PREFIX + suffix, 'wb') as outfile:
		pickle.dump(obj, outfile)

def quick_cache_load(suffix):
	with open(QUICK_CACHE_PREFIX + suffix, 'rb') as infile:
		return pickle.load(infile)


def normalize(v):
	# print(f"normalizing {v} of norm {np.linalg.norm(v)} to {v/np.linalg.norm(v)}.")
	# print("pre normalization")
	# result = v/np.linalg.norm(v)
	# print("post normalization")
	# return result
	return v/np.linalg.norm(v)

def get_squared_errors(points, origin, direction, get_projected_values_instead=False):
	# origin = point_cloud.mean(axis=0)

	# raw_direction = np.array([1,1,1])
	# direction = normalize(raw_direction)

	if np.round(np.linalg.norm(direction), decimals=2) != 1.0:
		# This shouldn't happen
		raise ValueError('We should have gotten a unit vector as direction that was normalized already.')

	# print("POS_1")
	# Choose a random direction that isn't too aligned with direction.
	# if abs(direction[0]) < 0.8:
	# 	# It isn't too aligned with the first dimension. So, just mirror that one.
	# 	intermediate = normalize(direction*np.array([-1,1,1]))
	# else:
	# 	# Mirroring it would be kinda closely aligned. So, rotate that dimension elsewhere instead.
	# 	intermediate = normalize(np.matmul(direction,np.array([
	# 		[0,1,0],
	# 		[-1,0,0],
	# 		[0,0,1],
	# 	])))
	if np.abs(direction).argmax() == 2:
		# Make sure to rotate the 3rd dimension.
		intermediate = normalize(np.matmul(direction,np.array([
			[0,0,1],
			[0,1,0],
			[-1,0,0],
		])))
	else:
		# Mirroring it would be kinda closely aligned. So, rotate that dimension elsewhere instead.
		intermediate = normalize(np.matmul(direction,np.array([
			[0,1,0],
			[-1,0,0],
			[0,0,1],
		])))

	# print("POS_2")
	# print("direction", direction)
	# print("intermediate", intermediate)
	# Get a perpendicular direction
	# print('got perpendicular:', np.cross(direction, intermediate))
	perpendicular = normalize(np.cross(direction, intermediate))

	# print("POS_3")
	# Get the third part of a normal base
	second = np.cross(direction, perpendicular)
	# print(direction, perpendicular, second)
	# print(direction, perpendicular, second)
	if np.round(np.linalg.norm(second), decimals=2) != 1.0:
		# This can't happen
		raise ValueError('We should have had a unit vector.')

	# print("POS_4")
	# Construct te rotation matrix
	rotation_matrix = np.array([direction, perpendicular, second,])

	rotated_points = np.matmul(points,rotation_matrix.T)

	error_vectors = rotated_points*np.array([0,1,1])

	squared_errors = np.linalg.norm(error_vectors, axis=1)**2

	if get_projected_values_instead:
		# Note, we assume the first dimension is the "high" value. This is used to determine in which direction the measured values are "high".
		if direction[0] > 0:
			return rotated_points.T[0]
		else:
			return -rotated_points.T[0]

	return sum(squared_errors)/len(squared_errors), (perpendicular, second)

def minimize_squared_errors(points):
	origin = points.mean(axis=0)
	current_directions = np.array([
		[1,0,0],
		[-1,0,0],
		[0,1,0],
		[0,-1,0],
		[0,0,1],
		[0,0,-1],
	])
	# initial_direction = min(initial_directions, key=(lambda d: get_squared_errors(points, origin, d)[0]))

	# error, (p1, p2) = get_squared_errors(points, origin, d)

	step_size = 0.2
	current_error = inf

	while step_size > STEP_SIZE_TARGET:
		# old_direction = current_direction
		# old_error = current_error
		# old_p1 = current_p1
		# old_p2 = current_p2
		
		current_results = [(d, get_squared_errors(points, origin, d)) for d in current_directions]
		current_result = min(current_results, key=(lambda tup: tup[1][0]))

		# print(current_result)
		if current_result[1][0] < current_error:
			current_direction = current_result[0]
			current_error = current_result[1][0]
			current_p1 = current_result[1][1][0]
			current_p2 = current_result[1][1][1]
		else:
			step_size = step_size / 4

		current_directions = [normalize(current_direction + step1*current_p1 + step2*current_p2) for (step1, step2) in MINIMIZATION_STEPS]

	return get_squared_errors(points, origin, current_direction, get_projected_values_instead=True)


def get_feature_matches(text, include_analysis_features=False):
	if include_analysis_features:
		return [feature(text) for feature in FEATURES + ANALYSIS_FEATURES]
	else:
		return [feature(text) for feature in FEATURES]

def extract_outerhtmls():
	cache_flag = 'OUTERHTMLS'
	if must_rebuild_cache(cache_flag):
		rebuild_cache_pre(cache_flag)
		extracted_outer_htmls = extract_export(sys.argv[1], sys.argv[2], extract_scrape_results=True)
		rebuild_cache_post(cache_flag)

def extract_textcontent(html_parser, message_id, outerhtml) -> str:
	# outer_html_str = outerhtml.decode('utf-8', errors='surrogatepass')

	html_parser.feed(outerhtml)
	html_parser.close()

	# html_parser.pretty_print(html_parser._stack[0])
	bubble_content = html_parser.get_textcontent()

	html_parser.reset()

	return bubble_content

def extract_textcontents():
	cache_flag = 'TEXTCONTENTS'
	if must_rebuild_cache(cache_flag):
		rebuild_cache_pre(cache_flag)

		channel_post_parser = ChannelPostHTMLParser()
		comment_parser = BubbleHTMLParser()
		counter = 0
		total_filter_counts = {}
		for filename in glob_files('*/*.comments.json'):
			extracted_textcontents = {}
			counter += 1
			print(f"Parsing file number {counter}.")
			# if IS_QUICK_RUN and counter < 1333:
			# 	continue
			with open(sys.argv[2] + '/' + filename, 'r') as file:
				bubbles = json.load(file)
				bubbles_list = list(bubbles.items())
				for message_id, outerhtmls in bubbles.items():
					# TO-DO: actually choose a specific outerhtml
					bubble_content = extract_textcontent(comment_parser, message_id, outerhtmls[0])
					if bubble_content.filter_reason is not None:
						if bubble_content.filter_reason not in total_filter_counts:
							total_filter_counts[bubble_content.filter_reason] = 0
						total_filter_counts[bubble_content.filter_reason] += 1
					extracted_textcontents[message_id] = bubble_content._asdict()
					# print(bubble_content)

			with open(sys.argv[2] + '/' + filename[:-5] + '.textcontents.json', 'w') as outfile:
				json.dump(extracted_textcontents, outfile)
			print(total_filter_counts)

		with open(sys.argv[2] + '/' + 'total_filter_counts.json', 'w') as outfile:
			json.dump(total_filter_counts, outfile)
		print(total_filter_counts)

		rebuild_cache_post(cache_flag)

def get_raw_measurement(model, textcontent, heuristic) -> tuple[object, str]:
	# TODO: implement
	CACHE_PATH = '../../data/request_cache'
	if not os.path.exists(CACHE_PATH):
		raise Exception("The location of the cache at '{CACHE_PATH}' does not exist.")
	if model in BERT_MODELS:
		# Fetch some logits
		req_dict = {
			"target_tokens": "ALL",
			"model_name": model,
			"text": textcontent + heuristic,
		}
		req_data = json.dumps(req_dict).encode('utf-8')
		hash_id = sha1(req_data).hexdigest()

		cache_entry_filename = CACHE_PATH + '/' + hash_id[:2] + '/' + hash_id[2:] + '.json'
		if os.path.exists(cache_entry_filename):
			with open(cache_entry_filename, 'r') as infile:
				try:
					cache_entry = json.load(infile)
				except json.decoder.JSONDecodeError:
					print(f"\nGot an error while reading cache file at '{cache_entry_filename}'. The file is likely broken.")
					raise
			resp_body = cache_entry['response_body']
		else:
			req = request.Request(
				url='http://localhost:18002/generate',
				headers={
					"Content-Type": "application/json",
				},
				method='POST',
				data=req_data
			)
			resp = request.urlopen(req)
			if not resp.status == 200:
				raise ValueError("Got {resp.status} http response from model endpoint.")
			resp_body = json.loads(resp.read().decode('utf-8'))
			if not os.path.exists('/'.join(cache_entry_filename.split('/')[:-1])):
				os.mkdir('/'.join(cache_entry_filename.split('/')[:-1]))
			with open(cache_entry_filename, 'w') as outfile:
				json.dump({
					'request_data': base64.b64encode(req_data).decode('utf-8'),
					'request_json': req_dict,
					'response_body': resp_body,
					'timestamp': datetime.now(timezone.utc).isoformat(),
				}, outfile)

		if ('error' not in resp_body) or (resp_body['error'] is None):
			if DONT_UNPICKLE:
				return None, 'UNPICKLING_DISABLED'
			output = pickle.loads(base64.b64decode(resp_body['output']))
			print(output)
			print(output.shape)
			return output, None
		elif 'error' in resp_body and resp_body['error'] == 'TOO_LARGE':
			return None, 'TOO_LARGE'
		else:
			raise ValueError("Unknown structure of response body.")

		# # Log the human_readable part
		# print('┄'*80)
		# req = request.Request(
		# 	url='http://localhost:18002/tokenize',
		# 	headers={
		# 		"Content-Type": "application/json",
		# 	},
		# 	method='POST',
		# 	data=json.dumps(
		# 		{
		# 			"model_name": model,
		# 			"text": textcontent + heuristic,
		# 		}
		# 	).encode('utf-8')
		# )
		# resp = request.urlopen(req)
		# if not resp.status == 200:
		# 	raise ValueError("Got {resp.status} http response from model endpoint.")
		# resp_body = json.loads(resp.read().decode('utf-8'))
		# print(pickle.loads(base64.b64decode(resp_body['output'])))
		# print(resp_body['human_readable'])
		# quit("Alrighty!")
	elif model in LLAMA_MODELS:
		raise NotImplementedError("LLaMA models haven't been implemented yet")
	else:
		raise ValueError('This model is not recognized.')

def get_raw_measurements(heuristics):
	total_counter = 0
	filtered_counter = 0
	selected_bubbles = {}
	file_progress_counter = 0
	for filename in glob_files('*/*.comments.textcontents.json'):
	# for filename in ('readovkanews/4307796430.comments.textcontents.json',):
		file_progress_counter += 1
		if IS_QUICK_RUN and file_progress_counter < 102:
			continue
		with open(sys.argv[2] + '/' + filename, 'r') as file:
			bubbles = json.load(file)

		for heuristic in heuristics:
			for model in MODELS:
				for message_id, bubble in bubbles.items():
					if bubble['filter_reason'] is not None:
						filtered_counter +=1
						continue
					feature_matches = get_feature_matches(bubble['textcontent'])
					if sum(feature_matches) > 0:
						print('─'*80)
						print(message_id, bubble['bubble_type'])
						print(bubble['textcontent'])
						# # Note: there actually seem to be duplicates, so don't raise an error, just overwrite them with one of the versions.
						# #       Contents are the same anyways.
						# # Update: Actually I think I fixed this, I think this was caused by it trying to store the results of multiple models in the same place.
						# if message_id in selected_bubbles:
						# 	raise ValueError(f"Encountered duplicate message_id: '{message_id}'.")
						if message_id not in selected_bubbles:
							selected_bubbles[message_id] = {}
						output, error = get_raw_measurement(model, bubble['textcontent'], heuristic)
						if error is not None and output is not None:
							raise ValueError("Expected either an error OR output logits.")
						elif error is not None:
							selected_bubbles[message_id][(model, heuristic)] = RawMeasurement(bubble['textcontent'], None)
						elif output is not None:
							selected_bubbles[message_id][(model, heuristic)] = RawMeasurement(bubble['textcontent'], output)
						else:
							# This can't happen
							raise ValueError("There's probably a type somewhere.")
						print('files processed:', file_progress_counter)
					total_counter += 1
					if total_counter % 100 == 0:
						print(f"selected: {len(selected_bubbles)}\ttotal: {total_counter}\tfiltered_counter: {filtered_counter}")
					# if IS_QUICK_RUN and total_counter > 100000:
					# 	return selected_bubbles

		print('files processed:', file_progress_counter)
		print(f"selected: {len(selected_bubbles)}\ttotal: {total_counter}\tfiltered_counter: {filtered_counter}")
		if IS_QUICK_RUN and file_progress_counter > 103:
			return selected_bubbles

	return selected_bubbles

def get_model_vocab(model):
	VOCAB_CACHE_PATH = '../../data/vocab_request_cache'
	if not os.path.exists(VOCAB_CACHE_PATH):
		raise Exception("The location of the cache at '{VOCAB_CACHE_PATH}' does not exist.")
	if model in BERT_MODELS:
		cache_entry_filename = VOCAB_CACHE_PATH + '/' + model + '.json'
		if os.path.exists(cache_entry_filename):
			with open(cache_entry_filename, 'r') as infile:
				try:
					cache_entry = json.load(infile)
				except json.decoder.JSONDecodeError:
					print(f"\nGot an error while reading cache file at '{cache_entry_filename}'. The file is likely broken.")
					raise
			resp_body = cache_entry['response_body']
		else:
			req_dict = {
				"model_name": model,
			}
			req_data = json.dumps(req_dict).encode('utf-8')
			req = request.Request(
				url='http://localhost:18002/vocab',
				headers={
					"Content-Type": "application/json",
				},
				method='POST',
				data=req_data
			)
			resp = request.urlopen(req)
			if not resp.status == 200:
				raise ValueError("Got {resp.status} http response from model endpoint.")
			resp_body = json.loads(resp.read().decode('utf-8'))
			if not os.path.exists('/'.join(cache_entry_filename.split('/')[:-1])):
				os.mkdir('/'.join(cache_entry_filename.split('/')[:-1]))
			with open(cache_entry_filename, 'w') as outfile:
				json.dump({
					'request_data': base64.b64encode(req_data).decode('utf-8'),
					'request_json': req_dict,
					'response_body': resp_body,
					'timestamp': datetime.now(timezone.utc).isoformat(),
				}, outfile)

		if ('error' not in resp_body) or (resp_body['error'] is None):
			output = resp_body['output']
			return output, None
		else:
			raise ValueError("Unknown structure of response body.")
	elif model in LLAMA_MODELS:
		raise NotImplementedError()
	else:
		raise ValueError(f"unknown model name: '{model}'")

def get_model_vocabs():
	results = {}
	for model in MODELS:
		output, error = get_model_vocab(model)
		if output is None or error is not None:
			raise ValueError('There seems to have been an error getting the vocabs.')
		results[model] = output
	return results

def project_onto_labels(raw_measurements, vocabs):
	results = {}
	print(f"len(raw_measurements): {len(raw_measurements)}.")
	for model in MODELS:
		vocab = vocabs[model]
		# Note: the value of labels that aren't in the vocab is represented as None here.
		labels_zipped = tuple(zip(
			LABELS,
			[vocab.get(label) for label in LABELS],
		))
		for message_id, raw_measurement in raw_measurements.items():
			results[message_id] = {}
			for (model, heuristic), raw_measurement_content in raw_measurement.items():
				# print(logits)
				if raw_measurement_content.logits is None:
					results[message_id][(model, heuristic)] = UnifiedMeasurement(raw_measurement_content.textcontent, None)
				else:
					results[message_id][(model, heuristic)] = UnifiedMeasurement(raw_measurement_content.textcontent, [raw_measurement_content.logits[token_index] if token_index is not None else None for (label, token_index) in labels_zipped])

	# # Make all sets of logits None when part already are
	# for model in MODELS:
	# 	for message_id, raw_measurement in raw_measurements.items():
	# 		for (model, heuristic), raw_measurement_content in raw_measurement.items():
	# 			if raw_measurement_content.logits is None:
	# 				results[message_id][(model, heuristic)] = UnifiedMeasurement(raw_measurement_content.textcontent, None)

	print(f"len(results): {len(results)}.")
	return results

def fit_normalization_lines(unified_measurements, segmented_by):
	# Some validation
	segmentation_axes = {}
	for segmentation_axis_name in segmented_by:
		if segmentation_axis_name == 'model':
			segmentation_axes[segmentation_axis_name] = MODELS
		elif segmentation_axis_name == 'heuristic':
			segmentation_axes[segmentation_axis_name] = HEURISTICS
		elif segmentation_axis_name == 'label_group':
			segmentation_axes[segmentation_axis_name] = LABEL_GROUPS
		else:
			raise ValueError(f"Unknown segmentation axis: '{segmentation_axis_name}'.")

	segmented_values = {}
	for message_id, post_measurements in unified_measurements.items():
		for (model, heuristic), (textcontent, unified_logits) in post_measurements.items():
			if model not in MODELS:
				raise ValueError("Malformed model name.")
			if heuristic not in HEURISTICS:
				raise ValueError("Malformed heuristic.")
			for label_group in LABEL_GROUPS:
				# Ensure the storage position exists
				if not (model, heuristic, label_group) in segmented_values:
					segmented_values[(model, heuristic, label_group)] = []
				# Store the values in there
				# Note: this also converts to numpy arrays.
				if unified_logits is None:
					# Note: this depends on the knowledge that this is going to be None every time!
					#       if that assumption no longer holds, this will introduce bugs!
					# continue
					segmented_values[(model, heuristic, label_group)].append(
						(textcontent, None)
					)
				else:
					segmented_values[(model, heuristic, label_group)].append(
						(textcontent, [unified_logits[LABELS.index(label)].detach().numpy() for label in label_group])
					)

	print('═'*80)
	from pprint import pprint; pprint(segmented_values)

	# # Set None values to None across the board
	# segmented_nonevals = np.array([
	# 	[
	# 		(subval[1] is None) for subval in segmented_value
	# 	] for key, segmented_value in segmented_values.items()
	# ])
	# segmented_noneval_totals = np.sum(segmented_nonevals, axis=0)

	if not len(set(len(segmented_value) for segmented_value in segmented_values.values())) == 1:
		# There's segments of different lengths.
		# This shouldn't be able to happen
		raise ValueError('Unequal sizes of lists of unified logits')
	# TODO: set stuff to None
	# for index, segmented_noneval_total in enumerate(segmented_noneval_totals):
	segments = tuple(segmented_values.values())
	# It must either have no, or all, Nones.
	clean_none_counts = (0, len(segments))
	for index in range(len(segments[0])):
		if not int(sum(segment[index][1] is None for segment in segments)) in clean_none_counts:
			for segment in segments:
				segment[index] = (segment[index][0], None)
		# if segmented_noneval_total > 0 and segmented_noneval_total < len(segmented_nonevals):
	

	# Fit a direction
	fitted_measurements = {}
	for key, segmented_value in segmented_values.items():
		textcontents = np.array([subval[0] for subval in segmented_value if subval[1] is not None])
		datapoints_combined = np.array([subval[1] for subval in segmented_value if subval[1] is not None])
		print(key)
		print(datapoints_combined)
		values = minimize_squared_errors(datapoints_combined)
		sorted_values = sorted(values)
		sorted_indices = np.array([sorted_values.index(value) for value in values])
		percentiles = sorted_indices/len(sorted_indices) + 0.5/len(sorted_indices)
		print(percentiles)
		fitted_measurements[key] = (textcontents, percentiles)

	return fitted_measurements

def main():
	if not len(sys.argv) == 3:
		raise ValueError(f"Expected exactly 2 arguments (source and destination path), but got {len(sys.argv)} instead.")

	if USE_ANALYSIS_CACHE:
		unified_measurements = quick_cache_load('.unified_measurements')
		vocabs = quick_cache_load('.vocabs')
	else:
		extract_outerhtmls()

		extract_textcontents()

		raw_measurements = get_raw_measurements(HEURISTICS)

		vocabs = get_model_vocabs()

		unified_measurements = project_onto_labels(raw_measurements, vocabs)
		if STORE_ANALYSIS_CACHE:
			quick_cache_store(unified_measurements, '.unified_measurements')
			quick_cache_store(vocabs, '.vocabs')

	# from pprint import pprint; pprint(unified_measurements)
	lens = [len(measurements) for measurements in unified_measurements.values()]
	if not min(lens) == max(lens):
		raise ValueError('There are differing sizes')
	# # nones = [int(sum(print(measurement) is None for measurement in measurements)) for measurements in unified_measurements.values()]
	# # nones = [int(sum(print(measurement) is None for measurement in measurements[1])) for measurements in unified_measurements.values()]
	# # if not min(nones) == max(nones):
	# # 	quit("alright!")
	# # nones = [int(sum(print(measurement) is None for measurement in measurements[1])) for measurements in unified_measurements.values()]
	# for measurements in unified_measurements.values():
	# 	print()
	# 	print("measurements:")
	# 	print(measurements)
	# # nones = [int(sum(print(measurement) is None for measurement in measurements[1])) for measurements in unified_measurements.values()]
	# for keys, measurements in unified_measurements.items():
	# 	print()
	# 	print("keys:")
	# 	print(keys)
	# # for index in range(len(unified_measurements))
	# from pprint import pprint; pprint(unified_measurements)

	fitted_measurements = fit_normalization_lines(
		unified_measurements,
		segmented_by=('model', 'heuristic', 'label_group'),
	)

	# from pprint import pprint; pprint(fitted_measurements)
	feature_results_list = []
	# for message_id, post_measurements in unified_measurements.items():
	# 	(model, heuristic), (textcontent, unified_logits) = next(iter(post_measurements.items()))
	# 	if unified_logits is not None:
	# 		# print(model)
	# 		# print(heuristic)
	# 		# print(textcontent)
	# 		feature_results_list.append([feature(textcontent) for feature in FEATURES])
	# 		# for (model, heuristic), (textcontent, unified_logits) in post_measurements.items():
	# 		# 	# print(model)
	# 		# 	# print(heuristic)
	# 		# 	# print(textcontent)
	# 		# 	feature_results_list.append([feature(textcontent) for feature in FEATURES])
	# 		# 	break
	for (model, heuristic, label_group), (textcontents, percentiles) in fitted_measurements.items():
		for textcontent in textcontents:
			feature_results_list.append([feature(textcontent) for feature in FEATURES])
		break
	feature_results = np.array(feature_results_list)
	# print(feature_results)

	t = (True,)
	# f = (False,)
	a = (True,False,)

	feature_characterizations = (
		(a,a,a,a,a,a),
		(t,a,a,a,a,a),
		(a,t,a,a,a,a),
		(a,a,t,a,a,a),
		(a,a,a,t,a,a),
		(t,t,a,a,a,a),
		(a,a,t,t,a,a),
		(t,a,t,a,a,a),
		(a,t,a,t,a,a),
		(t,t,t,t,a,a),
		(a,a,a,a,t,a),
		(a,a,a,a,a,t),
		(a,a,a,a,t,t),
		(t,a,a,a,t,a),
		(a,t,a,a,t,a),
		(t,t,a,a,t,a),
		(a,a,t,a,a,t),
		(a,a,a,t,a,t),
		(a,a,t,t,a,t),
		(t,a,t,a,t,a),
		(a,t,a,t,t,a),
		(t,a,t,a,a,t),
		(a,t,a,t,a,t),
		(t,t,t,t,t,t),
	)

	def matches(pattern,result):
		return all((val in cond) for val, cond in zip(result,pattern))

	feature_matches = np.array([[matches(pattern,feature_result) for feature_result in feature_results] for pattern in feature_characterizations])
	# print(feature_matches)


	total_results = []
	for feature_match_base, feature_characterization in zip(feature_matches, feature_characterizations):
		for (model, heuristic, label_group), (textcontents, fitted_measurement) in fitted_measurements.items():
			for invert in (False, True):
				if invert:
					feature_match = np.logical_not(feature_match_base)
				else:
					feature_match = feature_match_base
				total_results.append({
					'feature_characterization': feature_characterization,
					'invert': invert,
					'model': model,
					'hueristic': heuristic,
					'label_group': label_group,
					'mean': np.mean(fitted_measurement[feature_match]),
					'count': int(sum(feature_match)),
				})

	with open('../../data/final_results.json', 'w') as outfile:
		json.dump(total_results, outfile)

	print("Measurements done!")

if __name__ == '__main__':
	main()
