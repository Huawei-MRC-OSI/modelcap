import json
from time import strftime
from errno import EEXIST
from os import mkdir, makedirs, replace, listdir, rmdir, environ, symlink, \
               remove, environ, walk
from os.path import basename, join, isfile, isdir, islink, relpath, abspath
from hashlib import md5
from copy import deepcopy
from typing import Optional, Any, List, Tuple, Union


MODELCAP_ROOT:str = environ.get('MODELCAP_ROOT', join(environ.get('HOME','/var/run'),'_modelcap'))
MODELCAP_LOGDIR:str = environ.get('MODELCAP_LOGDIR', join(MODELCAP_ROOT,'log'))
MODELCAP_TMP:str = environ.get('MODELCAP_TMP', join(MODELCAP_ROOT,'tmp'))
MODELCAP_STORE:str = join(MODELCAP_ROOT,'store')


Ref=str
RefPath=List[str]
WHash=str
Hash=str
Protocol=List[Tuple[str,WHash,Any]]


#  _   _ _   _ _
# | | | | |_(_) |___
# | | | | __| | / __|
# | |_| | |_| | \__ \
#  \___/ \__|_|_|___/

def timestring()->str:
  return strftime("%m%d-%H:%M:%S")

def logdir(tag:str="",logrootdir:str=MODELCAP_LOGDIR):
  return join(logrootdir,((str(tag)+'_') if len(tag)>0 else '')+timestring())

def mklogdir(tag:str="", subdirs:list=[], logrootdir:str=MODELCAP_LOGDIR, symlinks:bool=True):
  """ Creates `<logrootdir>/<tag>_<time>` folder and  `<logrootdir>/_<tag>_latest` symlink to
  it. """
  dirname=logdir(tag,logrootdir=logrootdir)
  mkdir(dirname)
  if symlinks:
    linkname=logrootdir+'/'+(('_'+str(tag)+'_latest') if len(tag)>0 else '_latest')
    try:
      symlink(basename(dirname),linkname)
    except OSError as e:
      if e.errno == EEXIST:
        remove(linkname)
        symlink(basename(dirname),linkname)
      else:
        raise e
  for sd in subdirs:
    mkdir(dirname+'/'+sd)
  return dirname

def forcelink(src,dst,**kwargs):
  """ Create a `dst` symlink poinitnig to `src`. Overwrites existing files, if any """
  symlink(src,dst+'__',**kwargs)
  replace(dst+'__',dst)

def dhash(path:str)->str:
  """ Calculate recursive MD5 hash of a directory.
  FIXME: stop ignoring file/directory names
  """
  assert isdir(path), f"dhash(path) expects a directory, but '{path}' is not"
  def _iter():
    for root, dirs, filenames in walk(abspath(path), topdown=True):
      for filename in filenames:
        yield abspath(join(root, filename))

  e=md5()
  nfiles=0
  for f in _iter():
    with open(f,'rb') as f:
      e.update(f.read())
    nfiles+=1

  if nfiles==0:
    print('Warning: hashpath2: empty dir')

  return e.hexdigest()


def _ref2sys(ref:Ref)->str:
  return store_systempath(store_refpath(ref))

def _scanref_list(l):
  assert isinstance(l,list)
  res=[]
  for i in l:
    if isinstance(i,tuple):
      res+=_scanref_tuple(i)
    elif isinstance(i,list):
      res+=_scanref_list(i)
    elif isinstance(i,dict):
      res+=_scanref_dict(i)
    elif isinstance(i,str) and i[:4]=='ref:':
      res.append(i)
  return res

def _scanref_tuple(t):
  assert isinstance(t,tuple)
  return _scanref_list(list(t))

def _scanref_dict(obj):
  assert isinstance(obj,dict)
  return _scanref_list(list(obj.values()))


def dicthash(d:dict)->Hash:
  """ Calculate hashsum of a Python dict. Top-level fields starting from '_' are ignored """
  string="_".join(str(k)+"="+str(v) for k,v in sorted(d.items()) if len(k)>0 and k[0]!='_')
  return md5(string.encode('utf-8')).hexdigest()

def assert_serializable(d:Any, argname:str)->Any:
  error_msg=(f"Content of this '{argname}' of type {type(d)} is not serializable!"
             f"\n\n{d}\n\n"
             f"Make sure that `json.dumps`/`json.loads` work and doesn't"
             f"change it. Typically, we want to use only simple Python types"
             f"like lists, dicts, strings, ints, etc. In particular,"
             f"overloaded floats like `np.float32` don't work. Also, we"
             f"don't use Python tuples, because they don't survive the JSON"
             f"serialization")
  s=json.dumps(d)
  assert s is not None, error_msg
  d2=json.loads(s)
  assert str(d)==str(d2), error_msg
  return d2

def assert_valid_dict(d:dict, argname:str)->None:
  d2=assert_serializable(d, argname)
  assert dicthash(d)==dicthash(d2)

# ____
# |  _ \ _ __ ___   __ _ _ __ __ _ _ __ ___
# | |_) | '__/ _ \ / _` | '__/ _` | '_ ` _ \
# |  __/| | | (_) | (_| | | | (_| | | | | | |
# |_|   |_|  \___/ \__, |_|  \__,_|_| |_| |_|
#                  |___/

class Program:
  """ Program is a collection of non-determenistic operations applied to a
  `Config`.

  Currently it is represented with a list of operation names, with possible
  arguments. Operation names and arguments should be JSON-serializable
  """
  def __init__(self, ops:list=[]):
    self.ops:List[Tuple[str,Any]]=ops

def program_add(p:Program, op:str, arg:Any=[])->Program:
  """ Add new operation to a program. Builds new program object """
  assert_serializable({'op':op,'arg':arg}, "op/arg")
  p2=deepcopy(p)
  p2.ops.append((op,arg))
  return p2

def program_hash(p:Program)->Hash:
  """ Calculate the hashe of a program """
  string=";".join([f'{nm}({str(args)})' for nm,args in p.ops if nm[0]!='_'])
  return md5(string.encode('utf-8')).hexdigest()


#   ____             __ _
#  / ___|___  _ __  / _(_) __ _
# | |   / _ \| '_ \| |_| |/ _` |
# | |__| (_) | | | |  _| | (_| |
#  \____\___/|_| |_|_| |_|\__, |
#                         |___/

class ConfigAttrs(dict):
  """ Helper object allowing to access dict fields as attributes """
  __getattr__ = dict.__getitem__ # type:ignore


class Config:
  """ Config is a JSON-serializable configuration object. It should match the
  requirements of `assert_valid_config`. Tupically, it's __dict__ should
  contain only either simple Python types (strings, bool, ints, floats), lists
  or dicts. No tuples, no `np.float32`, no functions. Note that fields with
  names starting from '_' are allowed but they don't preserved during
  serialization."""
  def __init__(self, d:dict):
    assert_valid_dict(d,'dict')
    self.__dict__=deepcopy(d)

def assert_valid_config(c:Config):
  assert c is not None, 'Expected `Config` object, but None was passed'
  assert_valid_dict(c.__dict__, 'Config')

def config_dict(c:Config)->dict:
  return deepcopy(c.__dict__)

def config_ro(c:Config)->Any:
  return ConfigAttrs(c.__dict__)

def config_hash(c:Config)->Hash:
  """ Calculate the hash of config. Top-level fields starting from '_' are ignored """
  return dicthash(config_dict(c))


#  ____  _        _
# / ___|| |_ __ _| |_ ___
# \___ \| __/ _` | __/ _ \
#  ___) | || (_| | ||  __/
# |____/ \__\__,_|\__\___|


""" State is a combination of config and program """
State = Tuple[Config,Program]

def state(c:Config)->State:
  """ State constructor """
  return (c,Program())

def state_add(s:State, op:str, arg:Any=[])->State:
  return (s[0],program_add(s[1],op,arg))

def state_deps(s:State)->List[Ref]:
  (c,p)=s
  refs=_scanref_dict(config_dict(c))+_scanref_list(p.ops)
  return list(set(refs))


#  ____  _
# / ___|| |_ ___  _ __ ___
# \___ \| __/ _ \| '__/ _ \
#  ___) | || (_) | | |  __/
# |____/ \__\___/|_|  \___|

def assert_valid_ref(ref:Ref)->None:
  error_msg=(f'Value of type {type(ref)} is not a valid reference! Expected '
             f'string of form \'ref:HASH\', but actual value is "{ref}"')
  assert len(ref)>4, error_msg
  assert ref[:4] == 'ref:', error_msg

def assert_valid_refpath(refpath):
  error_msg=(f'Value of type {type(refpath)} is not a valid refpath! Expected '
             f'list of strings starting from a reference, but actual value '
             f'is "{refpath}"')
  assert len(refpath)>0, error_msg
  assert_valid_ref(refpath[0]), error_msg

def store_systempath(refpath:RefPath)->str:
  """ Constructs a Refpath into system-specific path
  TODO: use joins here
  """
  assert_valid_refpath(refpath)
  return MODELCAP_STORE+'/'+refpath[0][4:]+'/'+'/'.join(refpath[1:])

def store_refpath(ref:Ref, items:List[str]=[])->RefPath:
  """ Constructs a Refpath out of a reference `ref` and a path within the node """
  assert_valid_ref(ref)
  return [ref]+items

def store_readjson(refpath:RefPath)->Any:
  with open(store_systempath(refpath), "r") as f:
    return json.load(f)

def config_deref(ref:Ref)->Config:
  assert_valid_ref(ref)
  return Config(store_readjson([ref, 'config.json']))

def config_deref_ro(ref:Ref)->Any:
  return config_ro(Config(store_readjson([ref, 'config.json'])))

def program_deref(ref:Ref)->Program:
  return Program(store_readjson([ref, 'program.json']))

def store_deps(ref:Ref)->List[Ref]:
  """ Return storage reference's dependencies, that is all the other references
  found in current ref's config and program """
  c=config_deref(ref)
  p=program_deref(ref)
  return state_deps((c,p))


#  __  __           _      _
# |  \/  | ___   __| | ___| |
# | |\/| |/ _ \ / _` |/ _ \ |
# | |  | | (_) | (_| |  __/ |
# |_|  |_|\___/ \__,_|\___|_|


class Model:
  """ Model tracks the process of building storage nodes.

  Lifecycle of a model starts from its creation from JSON-serializable
  `Config`.

  After the model is created, users typically perform non-determenistic
  operations on it. To make the model abstraction aware of them, users have to
  update the _state_ of the model, which is a combination of `config`, `program`
  and `protocol` field.  The separation of state into `config` and `program` is
  not strictly important, but we hope it will help us to build a more
  user-friendly search system. We encourage users to keep config immutable after
  it was passed to model, and use `program` to track changes.  During
  operations, users are welcome to save various processing artifacts into
  temporary folder as returned by `model_outpath(m)` function.

  Note, that the rational behind `protocol` is unclear, maybe it should be
  moved into userland code completely.

  Finally, users typically call `model_save` which finishes the node creation,
  'seals' the node with a hash and sets the `storedir` field.  The storage item
  is believed to be immutable (but nothing special is done to enforce this
  restriction). `model_storelink` may be used to drop a symlink to this node
  into user-specified folder """

  def __init__(self, config:Config, timeprefix:Optional[str]=None):
    assert_valid_config(config)
    self.timeprefix:str = timestring() if timeprefix is None else timeprefix
    self.config:Config = config
    self.program:Program = Program([])
    self.protocol:Protocol = []
    self.outdir:str=f'{self.timeprefix}_{config_hash(config)[:8]}'
    self.storedir:Optional[str]=None

  def get_whash(self)->WHash:
    assert self.storedir is None, \
      "This model is already saved so we don't want to get the hash of its temporary state"
    return dhash(model_outpath(self))

def model_program(m:Model)->Program:
  return m.program

def model_outpath(m:Model)->str:
  path=MODELCAP_TMP+'/'+m.outdir
  makedirs(path, exist_ok=True)
  return path

def model_storepath(m:Model)->str:
  assert m.storedir is not None, \
      "Looks like this model is not saved yet and thus it's `storepath` is undefined"
  return MODELCAP_STORE+'/'+m.storedir

def model_config(m:Model)->Config:
  return m.config

def model_config_ro(m:Model)->Any:
  return config_ro(model_config(m))

def model_lasthash(m:Model)->Optional[WHash]:
  assert m.protocol is not None
  if len(m.protocol) == 0:
    return None
  else:
    return m.protocol[-1][1]

def protocol_add(m:Model, name:str, arg:Any=[], result:Any=[], expect_wchange:bool=True)->None:
  assert_serializable(name,'name')
  assert_serializable(arg,'arg')
  assert_serializable(result,'result')
  new_whash=m.get_whash()
  old_whash=model_lasthash(m)
  if expect_wchange:
    assert new_whash != old_whash, \
        (f"Modelcap sanity check: Operation was marked as parameter-changing,"
         f"but Model parameters didn't change their hashes as expected."
         f"Both hashes are {new_whash}.")
  else:
    assert new_whash == old_whash or (old_whash is None), \
        (f"Modelcap sanity check: Operation was marked as"
         f"non-paramerer-changing, but Model parameters were in fact changed by"
         f"something. Expected {old_whash}, got {new_whash}.")
  c=model_config(m)
  m.program.ops.append((name, arg))
  m.protocol.append((name, new_whash, result))

def model_storelink(m:Model, expdir:str, linksuffix:str, withtime=True)->None:
  """ Puts a link to model's storage into user-specified directory `expdir` """
  assert m.storedir is not None, \
      "Looks like this model is not saved yet and thus it's `storelink` is undefined"
  timeprefix=f'{m.timeprefix}_' if withtime else ''
  forcelink(relpath(model_storepath(m), expdir), expdir+f'/{timeprefix}{linksuffix}')

# def metricslink(m:Model, expdir:str, tmpname:Optional[str]='tmplink')->None:
#   """ FIXME: move this out of generic libraty to ML-specific place """
#   prefix=tmpname if tmpname is not None else f'{m.timeprefix}'
#   forcelink(relpath(model_metricspath(m), expdir), expdir+f'/{prefix}')

def model_save(m:Model)->Ref:
  """ Create new node in the storage. Return reference to newly created storage node.
  Node artifacts should be already prepared in the `model_output` directory.
  This function saves additional metadata and seals the node with hash. Sealed
  state is marked by assigning non-empty `storedir`. """
  c = model_config(m)
  p = model_program(m)
  o = model_outpath(m)

  oops_message = ("Oops: Attempting to overwrite file %(F)s with builtin"
                  "version. Please don't save files with this name in model's"
                  "output folder for now.")

  assert not isfile(o+'/config.json'), oops_message % {'F':'config.json'}
  assert not isfile(o+'/program.json'), oops_message % {'F':'program.json'}
  assert not isfile(o+'/protocol.json'), oops_message % {'F':'protocol.json'}

  with open(o+'/config.json', 'w') as f:
    json.dump(config_dict(c), f, indent=4)
  with open(o+'/program.json', 'w') as f:
    json.dump(m.program.ops, f, indent=4)
  with open(o+'/protocol.json', 'w') as f:
    json.dump(m.protocol, f, indent=4)

  ho=dhash(o)
  storedir=config_dict(c).get('name','unnamed')+'-'+ho
  storepath=MODELCAP_STORE+'/'+storedir
  if isdir(storepath):
    hs=dhash(storepath)
    assert ho==hs, f"Oops: {storedir} exists, but have incorrect hash {hs}."
    rmdir(o)
  else:
    makedirs(MODELCAP_STORE, exist_ok=True)
    replace(o, storepath)

  m.storedir=storedir
  print(m.storedir)
  ref='ref:'+storedir
  assert_valid_ref(ref)
  return ref


#  ____                      _
# / ___|  ___  __ _ _ __ ___| |__
# \___ \ / _ \/ _` | '__/ __| '_ \
#  ___) |  __/ (_| | | | (__| | | |
# |____/ \___|\__,_|_|  \___|_| |_|


def search_(chash:Hash, phash:Hash, storepath:str=MODELCAP_STORE)->List[Ref]:
  """ Return references matching the hashes of config and program """
  matched=[]
  for dirname in sorted(listdir(storepath)):
    ref='ref:'+dirname
    c=config_deref(ref)
    p=program_deref(ref)
    if config_hash(c)==chash and program_hash(p)==phash:
      matched.append(ref)
  return matched

def search(cp:State, **kwargs)->List[Ref]:
  """ Return list of references to Store nodes that matches given `State` i.e.
  `Config` and `Program` (in terms of corresponding `*_hash` functions)."""
  return search_(config_hash(cp[0]), program_hash(cp[1]), **kwargs)

def only(refs:List[Ref])->Ref:
  """ Take a list and extract it's single item, or complain loudly """
  for r in refs:
    assert_valid_ref(r)
  if len(refs)==0:
    assert False, \
        (f"Empty list was passed to only(). This may mean that preceeding "
         f"search founds no results in storage. You may have to either update "
         f"the storage from elsewhere or re-run the associated computations to "
         f"produce that nodes locally")
  else:
    assert len(refs)==1, \
        (f"only() expected exactly one matched ref, but there are {len(refs)} "
         f"of them:\n{refs}\n. Probably you need a more clever filter to make "
         f"a right choice")
  return refs[0]

