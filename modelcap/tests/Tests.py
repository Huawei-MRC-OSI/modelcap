from os import makedirs, replace, listdir
from os.path import join
from shutil import rmtree

from hypothesis import given, assume, example, note, settings
from hypothesis.strategies import text, decimals, integers

from modelcap import Config, Model, model_outpath, model_save, store_initialize, \
                     MODELCAP_STORE

from modelcap import assert_valid_ref, assert_store_initialized


def set_storage(tn:str)->str:
  storepath=f'/tmp/{tn}'
  rmtree(storepath, onerror=lambda a,b,c:())
  makedirs(storepath, exist_ok=False)
  store_initialize()
  assert 0==len(listdir(storepath))
  MODELCAP_STORE=storepath
  return storepath


def test_make_storege()->None:
  set_storage('a')
  set_storage('a')


@given(key=text(min_size=1,max_size=10),
       value=text(min_size=1,max_size=10))
def test_node_lifecycle(key,value)->None:
  set_storage('modelcap_store_lifecycle')

  c=Config({key:value})
  m=Model(c)
  o=model_outpath(m)
  with open(join(o,'artifact'),'w') as f:
    f.write('artifact')
  ref=model_save(m)
  assert_valid_ref(ref)
