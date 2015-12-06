#!/usr/bin/env python

import os
import sys
import json
import pprint
import shutil
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

def clone(data):
	url = "https://github.com/" + data['full_name']
	out = "%d.git" % (data['id'],)
	assert not os.path.exists(out), out
	subprocess.check_call(["git", "clone", "--mirror", url, out])
	assert os.path.isdir(out), out

	# Remove unneeded files
	os.unlink(out + "/description")
	shutil.rmtree(out + "/hooks")
	shutil.rmtree(out + "/info")

	# Write out metadata
	assert not os.path.exists(out + '/metadata.json'), out
	with open(out + '/metadata.json', 'wb') as f:
		json.dump({"api.github.com": data}, f)

def main():
	for id in sys.stdin:
		id = int(id.rstrip())
		data = get_repo_metadata(id)
		if want_repo(data):
			print "%d %s..." % (id, data['full_name'])
			clone(data)
		else:
			print "%d %s unwanted" % (id, data['full_name'])

if __name__ == '__main__':
	main()
