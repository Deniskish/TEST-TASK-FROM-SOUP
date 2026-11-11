from http.server import BaseHTTPRequestHandler, HTTPServer
class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        n=int(self.headers.get('Content-Length','0'))
        if n>1048576:
            self.send_error(413);return
        print(self.rfile.read(n).decode(),flush=True)
        self.send_response(200);self.end_headers()
HTTPServer(('0.0.0.0',8080),Receiver).serve_forever()
