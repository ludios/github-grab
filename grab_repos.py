#!/usr/bin/env python

__version__ = '0.3.0'

import os
import sys
import json
import time
import shutil
import socket
import random
import datetime
import requests
import traceback
import subprocess

def filter_repo_metadata(orig):
	data = orig.copy()
	for k, v in orig.iteritems():
		# All of the https://api.github.com/ "links" are a waste of space
		if k == "url" or (k.endswith("_url") and not k in ("avatar_url", "mirror_url")):
			del data[k]
	if u"owner" in data:
		data[u"owner"] = filter_repo_metadata(data[u"owner"])
	return data

class RepoNotFound(Exception):
	pass

class RepoAccessBlocked(Exception):
	pass

class MetadataError(Exception):
	pass

def get_repo_metadata(id, token=None):
	headers = {}
	if token is not None:
		headers['Authorization'] = 'token %s' % (token,)
	text = requests.get('https://api.github.com/repositories/%d' % (id,), headers=headers).text
	data = json.loads(text)
	if 'message' in data:
		if data['message'] == 'Not Found':
			raise RepoNotFound(data['message'])
		# e.g. https://api.github.com/repositories/738
		elif data['message'] == 'Repository access blocked':
			raise RepoAccessBlocked(data['message'])
		else:
			raise MetadataError(data['message'])
	else:
		return filter_repo_metadata(data)

def want_repo(data):
	# We'll get forks later
	if data['fork']:
		return False
	return True

def get_iso_time():
	return datetime.datetime.utcnow().isoformat().rsplit('.', 1)[0] + 'Z'

def get_git_version():
	return subprocess.check_output(['git', '--version']).strip()

def get_directory(id):
	assert isinstance(id, (int, long)), id
	# GitHub is at id ~47.5 million as of 2015-12-05; assume they
	# won't have more than 10 billion repos and pad to 10 digits.
	s = str(id).zfill(10)
	# Don't put more than 10,000 repos in the last leaf dir
	return s[:-4] + '/' + s + '.git'

def upload_terastash(directory):
	assert not "'" in directory, directory
	subprocess.check_call("find '%s' -type f -print0 | xargs -0 ts add" % (directory,), shell=True)
	shutil.rmtree(directory)

def upload_noop(directory):
	pass

class Decayer(object):
	def __init__(self, initial, multiplier, maximum):
		"""
		initial - initial number to return
		multiplier - multiply number by this value after each call to decay()
		maximum - cap number at this value
		"""
		self.initial = initial
		self.multiplier = multiplier
		self.maximum = maximum
		self.reset()

	def reset(self):
		# First call to .decay() will multiply, but we want to get the `intitial`
		# value on the first call to .decay(), so divide.
		self.current = self.initial / self.multiplier
		return self.current

	def decay(self):
		self.current = min(self.current * self.multiplier, self.maximum)
		return self.current

def has_unpacked_objects(out):
	return bool(list(c for c in os.listdir(out + "/objects") if c not in ('info', 'pack')))

def clone(data, out):
	url = "https://github.com/" + data['full_name']
	assert not os.path.exists(out), out
	# For finding repos cloned by a specific problematic server
	hostname = socket.gethostname()
	fetched_at = get_iso_time()
	git_version = get_git_version()
	subprocess.check_call(["git", "clone", "--quiet", "--mirror", url, out])
	if has_unpacked_objects(out):
		subprocess.check_call(["git", "repack", "-q", "-A", "-d"], cwd=out)
	assert os.path.isdir(out), out

	# Remove unneeded files
	os.unlink(out + "/description")
	shutil.rmtree(out + "/hooks")
	shutil.rmtree(out + "/info")

	# Write out metadata
	assert not os.path.exists(out + '/metadata.json'), out
	with open(out + '/metadata.json', 'wb') as f:
		json.dump({
			"api.github.com": data,
			"fetched_at": fetched_at,
			"fetched_by": hostname,
			"grab_version": __version__,
			"git_version": git_version,
		}, f)

def try_rmtree(p):
	try:
		shutil.rmtree(p)
	except OSError:
		pass
	assert not os.path.exists(p), p

def log(symbol, text=""):
	print get_iso_time(), symbol.ljust(8), text

def retry(func, retry_on=Exception):
	decayer = Decayer(2, 2, 300)
	for tries_left in reversed(xrange(10)):
		try:
			return func()
		except retry_on:
			if tries_left == 0:
				raise
			traceback.print_exc(file=sys.stdout)
			log("RETRY", tries_left)
			time.sleep(decayer.decay())
		else:
			break

def get_repo_id_and_api_key_from_stdin_and_env():
	line = sys.stdin.readline()
	if not line:
		return None, None
	id = int(line.rstrip())
	api_tokens = os.environ.get('GITHUB_API_TOKENS', '').split()
	if api_tokens == [""]:
		api_tokens = []
	token = random.choice(api_tokens) if api_tokens else None
	return id, token

def get_repo_id_and_api_key_from_distributor(distributor_url_next):
	def f():
		resp = retry(
			lambda: requests.post(
				distributor_url_next,
				data={'worker': os.environ['GRAB_REPOS_WORKER']}
			), requests.exceptions.ConnectionError)
		data = json.loads(resp.text)
		if data is None:
			return None, None
		return data['repo_id'], data['api_key']
	return f

def main():
	if os.environ.get('GRAB_REPOS_UPLOADER') == 'terastash':
		upload = upload_terastash
	else:
		upload = upload_noop

	if os.environ.get('GRAB_REPOS_SOURCE', '').startswith('http://'):
		distributor_url_next = os.environ['GRAB_REPOS_SOURCE']
		get_repo_id_and_api_key = get_repo_id_and_api_key_from_distributor(distributor_url_next)
	else:
		get_repo_id_and_api_key = get_repo_id_and_api_key_from_stdin_and_env

	log("UPLOADER", upload.__name__)

	while True:
		if os.path.exists('stop'):
			log("STOPPING", "because 'stop' file is present")
			break
		id, token = get_repo_id_and_api_key()
		if id is None:
			log("FINISHED")
			break
		try:
			data = retry(lambda: get_repo_metadata(id, token), requests.exceptions.ConnectionError)
		except RepoNotFound:
			log("404", id)
			continue
		except RepoAccessBlocked:
			log("403", id)
			continue

		if not want_repo(data):
			log("UNWANTED", "%d %s" % (id, data['full_name']))
			continue

		directory = get_directory(data["id"])
		# Assert the exact length since we're running a dangerous rmtree below
		assert len(directory) == 21, len(directory)
		log("CLONE", "%d %s" % (id, data['full_name']))
		def rm_and_clone():
			try_rmtree(directory)
			clone(data, directory)
		retry(rm_and_clone)
		log("UPLOAD", id)
		upload(directory)
		log("DONE", id)

if __name__ == '__main__':
	main()
