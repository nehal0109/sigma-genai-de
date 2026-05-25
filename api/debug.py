from http.server import BaseHTTPRequestHandler
import json
import os


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        token = os.environ.get('GITHUB_TOKEN', '')
        data = {
            "GITHUB_TOKEN_set": bool(token),
            "GITHUB_TOKEN_length": len(token),
            "TRAINER_REPO": os.environ.get('TRAINER_REPO', 'NOT SET'),
            "all_env_keys": [k for k in os.environ.keys()],
        }
        body = json.dumps(data, indent=2).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass
