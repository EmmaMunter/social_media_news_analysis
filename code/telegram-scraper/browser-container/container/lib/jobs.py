import boto3
import json
import itertools
import os
from uuid import uuid4
from hashlib import sha1
from time import sleep

_JOBS_SQS_URL = "https://sqs.eu-central-1.amazonaws.com/182941705927/telegram-scraper-jobs.fifo"
_JOBS_SQS_MAX_RETRIES = 15
_JOBS_SQS_RETRY_DELAY = 60

# Allow overriding the defaults
if '_JOBS_SQS_URL' in os.environ:
	_JOBS_SQS_URL = os.environ['_JOBS_SQS_URL']
if '_JOBS_SQS_MAX_RETRIES' in os.environ:
	_JOBS_SQS_MAX_RETRIES = int(os.environ['_JOBS_SQS_MAX_RETRIES'])
if '_JOBS_SQS_RETRY_DELAY' in os.environ:
	_JOBS_SQS_RETRY_DELAY = int(os.environ['_JOBS_SQS_RETRY_DELAY'])

_JOBS_SQS_BACKEND = boto3.client('sqs')



def send_job(job_type, target):
	if not job_type in ("scrape_id","scrape_comments"):
		raise Exception(f"Cannot send jobs of type [{job_type}].")
	body = json.dumps({
		"type": job_type,
		"target": target,
	})
	print(f"sending job: '{body}'")
	_JOBS_SQS_BACKEND.send_message(
		QueueUrl=_JOBS_SQS_URL,
		MessageBody=body,
		MessageGroupId=sha1(body.encode()).hexdigest(),
	)

# A single job.
class Job:
	def __init__(self, job_type, target, receipt_handle, test_job=False):
		self.type = job_type
		self.target = target
		self.receipt_handle = receipt_handle
		self.is_test_job = test_job
		self.has_been_deleted = False
	def reject(self):
		if self.is_test_job:
			return
		_JOBS_SQS_BACKEND.change_message_visibility(
			QueueUrl=_JOBS_SQS_URL,
			ReceiptHandle=self.receipt_handle,
			VisibilityTimeout=0,
		)
	def delete(self):
		if self.is_test_job:
			return
		if self.has_been_deleted:
			raise ValueError(f"Trying to delete a job that has already been deleted earlier.")
		_JOBS_SQS_BACKEND.delete_message(
			QueueUrl=_JOBS_SQS_URL,
			ReceiptHandle=self.receipt_handle,
		)
		self.has_been_deleted = True
	def send_new_copy(self):
		print(f"IMPORTANT! We're re-sending job of type '{job.type}' with target '{job.target}' to the queue.")
		if not self.has_been_deleted:
			# This can't happen
			raise ValueError(f"Job has not actually been deleted? Job was of type '{job.type}' with target '{job.target}'.")
		send_job(self.type, self.target)
		print(f"Re-sending done!")


# Job generator
def jobs():
	if 'TEST_JOBS' in os.environ:
		# Run manual test jobs instead of jobs from SQS
		test_jobs = json.loads(os.environ['TEST_JOBS'])
		for message_content in test_jobs:
			yield Job(
				job_type=message_content['type'],
				target=message_content['target'],
				receipt_handle=None,
				test_job=True
			)
		return

	# Run jobs from SQS
	last_successful_retrieval = -1
	for retrieval_count in itertools.count():
		# Receive from SQS
		response = _JOBS_SQS_BACKEND.receive_message(QueueUrl=_JOBS_SQS_URL)

		if not 'Messages' in response:
			print("No jobs.")
			if retrieval_count-last_successful_retrieval > _JOBS_SQS_MAX_RETRIES:
				# The queue is empty. Exit.
				return
			# Wait a bit before trying again.
			sleep(_JOBS_SQS_RETRY_DELAY)
		elif len(response['Messages']) == 1:
			# Got a job!
			message_content = json.loads(response['Messages'][0]['Body'])
			last_successful_retrieval = retrieval_count
			yield Job(
				job_type=message_content['type'],
				target=message_content['target'],
				receipt_handle=response['Messages'][0]['ReceiptHandle'],
			)
		else:
			# This should never be reached.
			print(response)
			raise Exception("Unexpectedly received multiple messages from SQS. Panicking.")
