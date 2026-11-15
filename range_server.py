import threading,hashlib
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
class Source:
    def __init__(self,files,switch_after=None):self.files=[Path(p).read_bytes() for p in files];self.switch_after=switch_after;self.requests=[];self.sent=0
    def start(self):
        source=self
        class H(BaseHTTPRequestHandler):
            def log_message(self,*a):pass
            def do_GET(self):
                i=int(source.switch_after is not None and len(source.requests)>=source.switch_after); data=source.files[min(i,len(source.files)-1)];etag='"'+hashlib.sha256(data).hexdigest()+'"'
                source.requests.append({'range':self.headers.get('Range'),'if_match':self.headers.get('If-Match'),'etag':etag})
                if self.headers.get('If-Match') not in (None,etag):self.send_response(412);self.end_headers();return
                rg=self.headers.get('Range');lo,hi=(0,len(data)-1)
                if rg:
                    lo,hi=map(int,rg.removeprefix('bytes=').split('-'));hi=min(hi,len(data)-1)
                if lo<0 or lo>hi:self.send_response(416);self.end_headers();return
                body=data[lo:hi+1];source.sent+=len(body);self.send_response(206 if rg else 200);self.send_header('ETag',etag);self.send_header('Content-Length',str(len(body)));self.send_header('Content-Range',f'bytes {lo}-{hi}/{len(data)}');self.end_headers();self.wfile.write(body)
        self.server=ThreadingHTTPServer(('127.0.0.1',0),H);self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start();return f'http://127.0.0.1:{self.server.server_port}/table'
    def close(self):self.server.shutdown();self.server.server_close();self.thread.join()
