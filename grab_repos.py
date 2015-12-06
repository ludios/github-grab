#!/usr/bin/env python

import sys
import json
import pprint
import requests

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
	if data["fork"]:
		return False
	return True

def main():
	for id in sys.stdin:
		id = int(id.rstrip())
		pprint.pprint(get_repo_metadata(id))

if __name__ == '__main__':
	main()
