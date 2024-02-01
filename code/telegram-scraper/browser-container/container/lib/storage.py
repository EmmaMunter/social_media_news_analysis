import boto3
from boto3.dynamodb.types import TypeDeserializer


_STORAGE_DYNAMODB_HANDLES_TABLE_NAME = 'tg-scraper-handles'

_STORAGE_DYNAMODB_BACKEND = boto3.client('dynamodb')

_STORAGE_DYNAMODB_DESERIALIZER = TypeDeserializer()

class ItemNotFoundException(Exception):
	pass

class ChannelReference:
	def __init__(self, database, tag_handle=None, numeric_id=None):
		self._database = database
		self.tag_handle = tag_handle
		self.numeric_id = numeric_id

		self._fetched_tag_handle = None
		self._stored_tag_handle = None

		self._assert_not_empty()

	def _assert_not_empty(self):
		if self.tag_handle is None and self.numeric_id is None:
			raise ValueError("Expected at least one of tag_handle and numeric_id, but got neither.")

	def _assert_is_complete(self):
		if self.tag_handle is None or self.numeric_id is None:
			raise ValueError("Expected both tag_handle and numeric_id to be known, but one or both is missing.")
		if type(self.tag_handle) != str:
			raise TypeError(f"Expected tag_handle to be a string, but it was of type '{type(self.tag_handle)}', with value: '{self.tag_handle}'.")
		if type(self.numeric_id) != str:
			raise TypeError(f"Expected numeric_id to be a string, but it was of type '{type(self.numeric_id)}', with value: '{self.numeric_id}'.")

	# Note: I had added this empty function defiitionm but I think I added the intented routine inline to like the .store() method or something. Keeping this around as a comment just in case.
	# def _was_fetched_from_database(self):


	# Note: when both the tag handle and numeric id are already known, this function is a no-op.
	# Also note: this throws an exception if the data can't be found in the database.
	def complete(self):
		self._assert_not_empty()
		if self.tag_handle is None:
			# Need to fetch the tag_handle from the database

			if self._fetched_tag_handle is not None:
				# This isn't supposed to ever happen
				raise ValueError("For some reason the tag_handle is None, but the _fetched_tag_handle isn't. This is unexpected, so exiting for safety.")

			response = self._database.get_item(_STORAGE_DYNAMODB_HANDLES_TABLE_NAME, {"numeric_id":{"S":self.numeric_id}})
			if 'Item' not in response:
				raise ItemNotFoundException()
			item = _STORAGE_DYNAMODB_DESERIALIZER._deserialize_m(response['Item'])
			tag_handle = item['tag_handle']

			if not type(tag_handle) == str:
				raise ValueError(f"Expected the fetched item to be a string. Instead got: '{tag_handle}'.")

			self.tag_handle = tag_handle
			self._fetched_tag_handle = tag_handle
		if self.numeric_id is None:
			# Note: if this is implemented, it would impact the code that tracks whether the data was fetched from the database.
			raise NotImplementedError("I expected this to be used mainly for finding tag handles of known numeric IDs, not the other way around.")

	# Note: this does nothing if the tag_handle was originally fetched from the database, but it could overwrite
	def store(self):
		self._assert_is_complete()
		if self._fetched_tag_handle is None and self._stored_tag_handle is None:
			# The data wasn't sourced from the database.

			# Check if it's in the database already.
			fetched_channel = ChannelReference(self._database, numeric_id=self.numeric_id)
			try:
				fetched_channel.complete()
				if self.tag_handle == fetched_channel.tag_handle:
					# It's already stored! Register this fact.
					self._fetched_tag_handle = fetched_channel.tag_handle
					return
				else:
					# Woops! There's an inconsistency.
					raise ValueError(f"Numeric id '{self.numeric_id}' is already associated with '{fetched_channel.tag_handle}' in the database, but we got it from elsewhere for '{self.tag_handle}'. One of them is probably incorrect.")
			except ItemNotFoundException:
				# This numeric id isn't in the database yet. So, we can go ahead with storing it!
				pass

			self._database.set_attribute(
				_STORAGE_DYNAMODB_HANDLES_TABLE_NAME,
				{
					"numeric_id": { "S": self.numeric_id }
				},
				"tag_handle",
				{ "S": self.tag_handle },
			)
			self._stored_tag_handle = self.tag_handle
		if self._fetched_tag_handle is not None:
			# The data was already sourced from the database, so we don't actually need to do anything.
			if self.tag_handle != self._fetched_tag_handle:
				raise NotImplementedError(f"This would update the tag_handle in the database from '{self._fetched_tag_handle}' to '{self.tag_handle}'. But we assumed these relations were static, which isn't true, but such updates aren't handled (yet).")
		if self._stored_tag_handle is not None:
			# We've already stored this data, so we don't actually need to do anything.
			if self.tag_handle != self._stored_tag_handle:
				raise NotImplementedError(f"This would overwrite the tag_handle in the database from '{self._stored_tag_handle}' to '{self.tag_handle}'. Something probably went wrong here, if the data changed in-between.")


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

	# Note: Note that this could fail, if the second operation is performed before the first. This shouldn't happen for DynamoDB though, because the returned old attribute should be strongly consistent.
	def set_nested_attribute(self, table, key, main_attribute_name, nested_attribute_name, attribute_value):
		print(f"Writing to {table} item: {key}.\nSetting {nested_attribute_name} below {main_attribute_name} to:\n{attribute_value}\n")
		main_attribute_write_value = { "M": {nested_attribute_name: attribute_value} }
		resp = _STORAGE_DYNAMODB_BACKEND.update_item(
			TableName=table,
			Key=key,
			ExpressionAttributeNames={
				"#A": main_attribute_name,
			},
			ExpressionAttributeValues={
				":A": main_attribute_write_value,
			},
			UpdateExpression="SET #A = if_not_exists(#A, :A)",
			ReturnValues="UPDATED_NEW",
		)
		if resp['Attributes'][main_attribute_name] != main_attribute_write_value:
			# Setting the main attribute failed, because it already exists. So, set the nested_attribute below it.
			resp = _STORAGE_DYNAMODB_BACKEND.update_item(
				TableName=table,
				Key=key,
				ExpressionAttributeNames={
					"#A": main_attribute_name,
					"#B": nested_attribute_name,
				},
				ExpressionAttributeValues={
					":A": attribute_value,
				},
				UpdateExpression="SET #A.#B = :A",
			)
		# except botocore.exceptions.ClientError as e:
			# if not auto_create_main_attribute:
				# raise
			# if not e.response['Error']['Code'] == 'ValidationException':
				# raise
			# # Create the map if doesn't exist yet
		print(f"response: {resp}")

	# Note: the set attribute is automatically created if it doesn't exist yet.
	def add_to_set(self, table, key, attribute_name, set_value):
		print(f"Writing to {table} item: {key}.\nAdding to attribute {attribute_name} the values:\n{set_value}\n")
		if not len(set_value) == 1 or next(iter(set_value)) not in ("SS","NS","BS"):
			raise ValueError("Set value is malformed!")
		resp = _STORAGE_DYNAMODB_BACKEND.update_item(
			TableName=table,
			Key=key,
			ExpressionAttributeNames={
				"#A": attribute_name,
			},
			ExpressionAttributeValues={
				":A": set_value,
			},
			UpdateExpression="ADD #A :A",
		)
		print(f"response: {resp}")
