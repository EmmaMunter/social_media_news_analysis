import boto3
import json
import itertools
import os
from uuid import uuid4
from hashlib import sha1
from time import sleep

_JOBS_SQS_URL = "https://sqs.eu-central-1.amazonaws.com/182941705927/scraper-jobs.fifo"
_JOBS_SQS_QUICK_URL = "https://sqs.eu-central-1.amazonaws.com/182941705927/scraper-jobs-QUICK.fifo"
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
	if not job_type in ("scrape_outlet", "scrape_outlet_search", "scrape_post",):
		raise Exception(f"Cannot send jobs of type [{job_type}].")
	body = json.dumps({
		"type": job_type,
		"target": target,
	})
	_JOBS_SQS_BACKEND.send_message(
		QueueUrl=_JOBS_SQS_URL,
		MessageBody=body,
		MessageGroupId=sha1(body.encode()).hexdigest(),
	)

# A single job.
class Job:
	def __init__(self, job_type, target, receipt_handle):
		self.type = job_type
		self.target = target
		self.receipt_handle = receipt_handle
	def reject(self):
		_JOBS_SQS_BACKEND.change_message_visibility(
			QueueUrl=_JOBS_SQS_URL,
			ReceiptHandle=self.receipt_handle,
			VisibilityTimeout=0,
		)
	def delete(self):
		_JOBS_SQS_BACKEND.delete_message(
			QueueUrl=_JOBS_SQS_URL,
			ReceiptHandle=self.receipt_handle,
		)

# Job generator
def jobs():
	last_successful_retrieval = -1
	for retrieval_count in itertools.count():
		# Receive from SQS
		use_quick_queue_flag = os.environ.get("USE_QUICK_QUEUE")
		if use_quick_queue_flag in (None, "0", "no", "n", "false"):
			queue_url = _JOBS_SQS_URL
		elif use_quick_queue_flag in ("1", "yes", "y", "true"):
			queue_url = _JOBS_SQS_QUICK_URL
		else:
			raise Exception(f"Unrecognized value of environment variable 'USE_QUICK_QUEUE': {use_quick_queue_flag}")
		response = _JOBS_SQS_BACKEND.receive_message(QueueUrl=queue_url)

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
			yield Job(
				job_type=message_content['type'],
				target=message_content['target'],
				receipt_handle=response['Messages'][0]['ReceiptHandle'],
			)
		else:
			# This should never be reached.
			print(response)
			raise Exception("Unexpectedly received multiple messages from SQS. Panicking.")
