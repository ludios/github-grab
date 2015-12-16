"""
Use distributor --hooks github-grab/distributor_hooks.py
"""

# Add your worker->API keys here
API_KEYS = {
	 'worker1': 'GITHUB_API_KEY'
	,'__default__': 'GITHUB_API_KEY'
}

def wrap_value(worker, v, API_KEYS=API_KEYS):
	return {
		 "api_key": API_KEYS.get(worker, API_KEYS['__default__'])
		,"repo_id": int(v)
	}
