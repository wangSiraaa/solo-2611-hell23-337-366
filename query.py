import io,urllib.request
from astropy.io import fits
def query(url,rows,start=0,stop=8):
    with urllib.request.urlopen(url) as r:raw=r.read();etag=r.headers['ETag']
    with fits.open(io.BytesIO(raw)) as f: values=[{'row':i,'samples':list(map(int,f[1].data[i]['samples'][start:stop]))} for i in rows]
    return {'etag':etag,'rows':values}
