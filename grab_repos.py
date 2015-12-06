#!/usr/bin/env python

__version__ = '0.1.0'

import os
import sys
import json
import pprint
import shutil
import socket
import datetime
import requests
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

class NoSuchRepo(Exception):
	pass

class MetadataError(Exception):
	pass

def get_repo_metadata(id):
	text = requests.get('https://api.github.com/repositories/%d' % (id,)).text
	data = json.loads(text)
	if 'message' in data:
		if data['message'] == 'Not Found':
			raise NoSuchRepo(data['message'])
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

def clone(data, out):
	url = "https://github.com/" + data['full_name']
	assert not os.path.exists(out), out
	# For finding repos cloned by a specific problematic server
	hostname = socket.gethostname()
	fetched_at = get_iso_time()
	git_version = get_git_version()
	subprocess.check_call(["git", "clone", "--mirror", url, out])
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

def main():
	if os.environ.get('GRAB_REPOS_UPLOADER') == 'terastash':
		upload = upload_terastash
	else:
		upload = upload_noop
	print "Using uploader %s" % (upload.__name__,)

	for id in sys.stdin:
		id = int(id.rstrip())
		data = get_repo_metadata(id)
		if want_repo(data):
			print "%d %s..." % (id, data['full_name'])
			directory = get_directory(data["id"])
			clone(data, directory)
			upload(directory)
		else:
			print "%d %s unwanted" % (id, data['full_name'])

if __name__ == '__main__':
	main()
