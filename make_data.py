from pathlib import Path
import numpy as np
from astropy.io import fits
def make(path,delta):
    arrays=np.empty(96,dtype=object)
    for i in range(96): arrays[i]=np.arange(1024,dtype=np.int32)+i*10000+delta
    h=fits.BinTableHDU.from_columns([fits.Column(name='id',format='J',array=np.arange(96)),fits.Column(name='samples',format='PJ()',array=arrays)])
    fits.HDUList([fits.PrimaryHDU(),h]).writeto(path,overwrite=True)
    # Two legal descriptors refer to the same heap object, not a duplicate heap copy.
    with fits.open(path) as f: start=f[1].fileinfo()['datLoc']; width=f[1].header['NAXIS1']
    b=bytearray(Path(path).read_bytes()); b[start+83*width+4:start+83*width+12]=b[start+7*width+4:start+7*width+12];Path(path).write_bytes(b)
if __name__=='__main__':
    Path('fixtures').mkdir(exist_ok=True);make('fixtures/a.fits',0);make('fixtures/b.fits',17)
