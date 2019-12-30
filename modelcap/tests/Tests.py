from os import makedirs, replace, listdir
from os.path import join
from shutil import rmtree

from hypothesis import given, assume, example, note, settings
from hypothesis.strategies import text, decimals, integers

from modelcap import Config, Model, model_outpath, model_save, store_initialize, \
                     mknode, store_deps, MODELCAP_STORE

from modelcap import assert_valid_ref, assert_store_initialized


def set_storage(tn:str)->str:
  import modelcap
  storepath=f'/tmp/{tn}'
  rmtree(storepath, onerror=lambda a,b,c:())
  modelcap.Modelcap.MODELCAP_STORE=storepath
  modelcap.Modelcap.MODELCAP_TMP='/tmp'
  store_initialize(exist_ok=False)
  assert 0==len(listdir(storepath))
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


@given(key=text(min_size=1,max_size=10),
       value=text(min_size=1,max_size=10))
@settings(deadline=None)
def test_mknode(key,value)->None:
  set_storage('modelcap_mknode')

  n1=mknode({key:value})
  assert_valid_ref(n1)
  n2=mknode({key:value, 'parent':n1})
  assert_valid_ref(n1)
  n3=mknode({key:value, 'parent':n2})
  assert_valid_ref(n3)

  n2_deps=store_deps(n2)
  assert n2_deps == [n1]
  n3_deps=store_deps(n3)
  assert n3_deps == [n2]

