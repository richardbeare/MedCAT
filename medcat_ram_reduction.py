# The idea is to create sqlitedict versions of the
# vocab and concept database. It doesn't appear possible
# to include all of the cdb as some parts are written to.
# Fortunately they are small, so can be left in RAM/

import dill
from dill import Pickler, Unpickler

thecdb = '/slowdata/richardb/CogStackTests/cdb_mimic_md_21-April-2021.dat'
thevocab = '/slowdata/richardb/CogStackTests/vocab.dat'

sqlite_cdb = '/tmp/cdb.sqlite' 
nonsqlite_cdb = '/tmp/cdb_mimic_non_dict.dat'

sqlite_vocab = '/tmp/vocab.db'

with open(thecdb, 'rb') as f:
    data=dill.load(f)

shelvesroot=sqlite_cdb
import os.path
from sqlitedict import SqliteDict

keys_saved = ['cui2context_vectors', 'cui2count_train',
              'cui2names', 'cui2type_ids', 'name2cuis2status',
              'name2cuis', 'vocab']

for k in keys_saved:
    xx = SqliteDict(filename=shelvesroot, tablename=k, autocommit=False, journal_mode='OFF')
    for k1, v1 in data['cdb'][k].items():
        xx[k1] = v1
    xx.commit()
    xx.close()
        

# write snames, which is a set, to a dictionary
xx = SqliteDict(filename=shelvesroot, tablename='snames', autocommit=False, journal_mode='OFF')
for k1 in data['cdb']['snames']:
    xx[k1] = None

xx.commit()
xx.close()


# collect dictionaries to delete
for k in keys_saved:
    del data['cdb'][k]

# remove snames 
del data['cdb']['snames']

with open(nonsqlite_cdb, 'wb') as ff:
    dill.dump(data, ff)


## Convert vocab too?
with open(thevocab, 'rb') as f:
    v = pickle.load(f)

with SqliteDict(filename=sqlite_vocab, tablename='vocab', autocommit=False) as xx:
    for k,v in v.items():
        xx[k]=v
    xx.commit()
    xx.close()
