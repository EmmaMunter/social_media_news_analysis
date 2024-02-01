#!/usr/bin/env python3

from flask import Flask, request
from transformers import BertTokenizer, BertForMaskedLM
import fire
import json
import pickle
import base64
from common.caching import AutoLoader
from torch import no_grad

KNOWN_MODELS = ('bert-base-multilingual-cased','DeepPavlov/rubert-base-cased')

app = Flask(__name__)

class ContextTooLargeException(ValueError):
	pass

# This class is intended to tranparently handle tokenization in a unified manner 
class Model:
	def __init__(self, model_name):
		self.tokenizer = BertTokenizer.from_pretrained(model_name)
		self.model = BertForMaskedLM.from_pretrained(model_name)

	def __call__(self, text):
		encoded_input = self.tokenizer(text, return_tensors='pt')
		if encoded_input['input_ids'].shape[1] > 512:
			raise ContextTooLargeException()
		raw_output = self.model(**encoded_input)
		# TODO: return the interesting parts of the result
		return raw_output

@app.route("/generate", methods=['POST'])
def generate():
	if not (
		type(request.json) == dict
		and 'text' in request.json
		and type(request.json['text']) == str
		and 'model_name' in request.json
		and type(request.json['model_name']) == str
		and 'target_tokens' in request.json
		and (
			request.json['target_tokens'] == 'ALL'
			or (
				type(request.json['target_tokens']) == list
				and all(type(target_token) == str for target_token in request.json['target_tokens'])
			)
		)
	):
		return {"error":"malformed request"}, 400
	print('Got request', request.json)

	text = request.json['text']
	model_name = request.json['model_name']

	warnings = []
	if model_name not in KNOWN_MODELS:
		warnings.append(f"Model '{model_name}' isn't known. Using anyways, but there might be issues.")
		print("WARNING:", warnings[-1])

	# Return an error if loading the requested model fails.
	if model_name not in MODELS:
		try:
			MODELS[model_name]
		except:
			return {"error": f"Could not load model '{model_name}'.", "warnings": warnings}

	tokens = MODELS[model_name].tokenizer.tokenize(text)
	mask_token_position = tokens.index('[MASK]')
	try:
		results = MODELS[model_name](text)
	except ContextTooLargeException:
		print("Context was too large. Returning 'TOO_LARGE' error.")
		return {
			'output': None,
			'error': 'TOO_LARGE',
			'tokens': tokens,
			'mask_token_position': mask_token_position,
		}

	print('Returning result', results)
	# TODO: have callers request only a subset of logits.
	return {
		# 'output': base64.b64encode(pickle.dumps(results.logits)).decode('utf-8'),
		'output': base64.b64encode(pickle.dumps(results.logits[0][mask_token_position])).decode('utf-8'),
		'error': None,
		'tokens': tokens,
		'mask_token_position': mask_token_position,
	}

@app.route("/tokenize", methods=['POST'])
def tokenize():
	if not (
		type(request.json) == dict
		and 'text' in request.json
		and type(request.json['text']) == str
		and 'model_name' in request.json
		and type(request.json['model_name']) == str
	):
		return {"error":"malformed request"}, 400
	print('Got request', request.json)

	text = request.json['text']
	model_name = request.json['model_name']

	warnings = []
	if model_name not in KNOWN_MODELS:
		warnings.append(f"Model '{model_name}' isn't known. Using anyways, but there might be issues.")
		print("WARNING:", warnings[-1])

	# Return an error if loading the requested model fails.
	if model_name not in MODELS:
		try:
			MODELS[model_name]
		except:
			return {"error": f"Could not load model '{model_name}'.", "warnings": warnings}

	tokenization = MODELS[model_name].tokenizer(text, return_tensors='pt')
	human_readable_tokenization = MODELS[model_name].tokenizer.tokenize(text)

	print('Returning tokenization', tokenization)
	return {
		'output': base64.b64encode(pickle.dumps(tokenization)).decode('utf-8'),
		'human_readable': human_readable_tokenization,
	}

@app.route("/vocab", methods=['POST'])
def vocab():
	if not (
		type(request.json) == dict
		and 'model_name' in request.json
		and type(request.json['model_name']) == str
	):
		return {"error":"malformed request"}, 400
	print('Got request', request.json)

	model_name = request.json['model_name']

	warnings = []
	if model_name not in KNOWN_MODELS:
		warnings.append(f"Model '{model_name}' isn't known. Using anyways, but there might be issues.")
		print("WARNING:", warnings[-1])

	# Return an error if loading the requested model fails.
	if model_name not in MODELS:
		try:
			MODELS[model_name]
		except:
			return {"error": f"Could not load model '{model_name}'.", "warnings": warnings}

	vocab = MODELS[model_name].tokenizer.vocab

	print('Returning vocab of', model_name)
	return {
		'output': vocab,
	}

def main(
	port: int
):
	global MODELS
	MODELS = AutoLoader(Model)

	# Preload BERT to speed up the first call after a reset
	MODELS['bert-base-multilingual-cased']

	app.run(port=port)

if __name__ == "__main__":
	with no_grad():
		fire.Fire(main)
