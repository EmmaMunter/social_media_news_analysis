#!/usr/bin/env python3

from flask import Flask, request, abort
import fire
import json
from common import load, cleanup

app = Flask(__name__)

@app.route("/generate", methods=['POST'])
def generate(
	temperature: float = 0.0,
	top_p: float = 0.95,  # use 0.95 or so for top_p sampler, and 0.0 for top_k sampler
	top_k: int = 40,
	repetition_penalty: float = (1.0 / 0.85),  # 1.0 to disable repetition_penalty
	sampler: str = 'top_p',  # top_p or top_k
):
	if not (
		type(request.json) == dict
		and 'prompt' in request.json
		and type(request.json['prompt']) == str
	):
		return {"error":"malformed request"}, 400
	print('Got request', request.json)

	max_seq_len = request.json.get('max_seq_len', MAX_SEQ_LEN)

	prompts = [request.json['prompt']]

	temperature = request.json.get('temperature', temperature)
	top_p = request.json.get('top_p', top_p)
	top_k = request.json.get('top_k', top_k)
	repetition_penalty = request.json.get('repetition_penalty', repetition_penalty)
	sampler = request.json.get('sampler', sampler)

	results = GENERATOR.generate_internals(
		prompts, max_gen_len=max_seq_len, temperature=temperature, top_p=top_p, top_k=top_k, repetition_penalty=repetition_penalty, sampler=sampler
	)
	print('Returning result', results[0])
	return {'output': results[0], 'internals': results[1]}

def main(
		port: int,
		ckpt_dir: str,
		tokenizer_path: str,
		max_seq_len: int = 2048,
		max_batch_size: int = 1,
):
	global GENERATOR, MAX_SEQ_LEN
	MAX_SEQ_LEN = max_seq_len
	GENERATOR = load(ckpt_dir, tokenizer_path, MAX_SEQ_LEN, max_batch_size)

	app.run(port=port)

if __name__ == "__main__":
	fire.Fire(main)
