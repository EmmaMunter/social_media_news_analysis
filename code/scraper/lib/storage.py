import os
import base64
import bz2
import gzip
import lzma
import boto3
import json
from glob import glob
from boto3.dynamodb.types import TypeDeserializer

_STORAGE_DYNAMODB_ACCOUNT_TABLE_NAME = 'scraper-accounts'

_STORAGE_DYNAMODB_BACKEND = boto3.client('dynamodb')

_STORAGE_DYNAMODB_DESERIALIZER = TypeDeserializer()

class Account:
	def __init__(self, database):
		self._database = database
		self.uuid = os.environ["ACCOUNT_UUID"]

		response = self._database.get_item(_STORAGE_DYNAMODB_ACCOUNT_TABLE_NAME,{"UUID":{"S":self.uuid}})
		item = _STORAGE_DYNAMODB_DESERIALIZER._deserialize_m(response['Item'])

		if item['cookies']['format'] != 'cookies.sqlite':
			raise Exception(f"Got unexpected cookie format: {item['cookies']['format']}")
		if item['cookies']['encoding'] != 'base64':
			raise Exception(f"Got unexpected cookie encoding: {item['cookies']['encoding']}")

		self.cookiefile = base64.b64decode(item['cookies']['content'])
		
		if 'compression' in item['cookies']:
			if item['cookies']['compression'] != 'bz2':
				raise Exception(f"Got unexpected cookie compression type: {item['cookies']['compression']}")
			self.cookiefile = bz2.decompress(self.cookiefile)

class Database:
	def __init__(self):
		pass

	def get_item(self, table, key):
		return _STORAGE_DYNAMODB_BACKEND.get_item(
			TableName=table,
			Key=key,
		)
		
	def touch_item(self, table, key, tag=None):
		print(f"Writing to {table} item: {key}")
		if tag:
			print(f"Tagging with {tag['path']} as {tag['value']}")
			resp = _STORAGE_DYNAMODB_BACKEND.update_item(
				TableName=table,
				Key=key,
				ExpressionAttributeNames={
					"#A": tag['path'],
				},
				ExpressionAttributeValues={
					":A": { 'SS': [tag['value']] },
				},
				UpdateExpression="ADD #A :A",
			)
		else:
			resp = _STORAGE_DYNAMODB_BACKEND.update_item(
				TableName=table,
				Key=key,
			)
		print(f"response: {resp}")

	def set_attribute(self, table, key, attribute_name, attribute_value):
		print(f"Writing to {table} item: {key}.\nSetting {attribute_name} to:\n{attribute_value}\n")
		resp = _STORAGE_DYNAMODB_BACKEND.update_item(
			TableName=table,
			Key=key,
			ExpressionAttributeNames={
				"#A": attribute_name,
			},
			ExpressionAttributeValues={
				":A": attribute_value,
			},
			UpdateExpression="SET #A = :A",
		)
		print(f"response: {resp}")

def extract_export(source_path, destination_path):
	if source_path[-1] != '/':
		source_path += '/'
	if destination_path[-1] != '/':
		destination_path += '/'
	if not os.path.exists(destination_path):
		raise Exception(f"Error: location '{destination_path}' does not exist.")
	source_filenames = glob(source_path + '*.json.gz')
	if len(source_filenames) < 1:
		raise Exception(f"Could not find source files in directory '{source_path}'.")

	for source_filename in source_filenames:
		print(f"Extracting '{source_filename}'.")
		with gzip.open(source_filename, 'r') as source_file:
			for line in source_file:
				item = json.loads(line)
				if 'outerHTML' in item['Item']:
					# Extract the raw outerHTML
					content_encoded = item['Item']['outerHTML']['M']['content']['B']
					# Note: stuff is base64-encoded twice for some reason?
					content_compressed = base64.b64decode(base64.b64decode(content_encoded))
					content = lzma.decompress(content_compressed)
					content_dest_filename = destination_path + item['Item']['account']['S'] + '/' + item['Item']['post_id']['S'] + '.outerHTML.html'
					if not os.path.exists(destination_path + item['Item']['account']['S'] + '/'):
						os.mkdir(destination_path + item['Item']['account']['S'] + '/')
					with open(content_dest_filename, 'wb') as content_dest_file:
						content_dest_file.write(content)
