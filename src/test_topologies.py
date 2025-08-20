import os
import json
import tempfile
import shutil
import time

import pytest

from topologyFinder import *

TMP_CACHE_FOLDER = '/tmp/cache'

def contains_none(obj):
    """
    Recursively returns True if obj, or any nested item inside a dict/list,
    is exactly Python None.
    """
    if obj is None:
        return True
    if isinstance(obj, dict):
        return any(contains_none(v) for v in obj.values())
    if isinstance(obj, list):
        return any(contains_none(item) for item in obj)
    return False
  
def find_none(obj, path=""):
    """
    Recursively traverse obj (which can be a dict, list, or scalar).
    If any value is None, print its path.
    Returns True if at least one None was found, else False.
    """
    found = False
    if obj is None:
        print(f"None found at: {path or '<root>'}")
        return True

    if isinstance(obj, dict):
        for key, value in obj.items():
            subpath = f"{path}.{key}" if path else key
            if find_none(value, subpath):
                found = True

    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            subpath = f"{path}[{idx}]"
            if find_none(item, subpath):
                found = True

    return found


def test_compute_server_pairs():
    # clientIP = "176.59.168.2" # TODO: does not pass because it has null parts
    clientIP = "37.110.210.2"
    filepath = getTopologyFilepath(clientIP, os.path.join('topos', '2025-04-28'))
    if filepath is None:
        return json.dumps({'success': False, 'error': 'No Y-topology found in filepath'})
    print("found filepath:", filepath)
    with open(filepath) as json_file:
        ytopologies = json.load(json_file)
    
    ss, si = computeServerPairs(ytopologies)
    print("server pairs:", ss)
    
    assert ("del02","del04","UNITEL LLC") in ss
    assert ("del02","bom04","UNITEL LLC") in ss

    print("IP pairs:", si)

    assert ("61.246.223.40","49.45.151.104","UNITEL LLC") in si
    assert ("61.246.223.14","49.45.151.104","UNITEL LLC") in si
    assert ("61.246.223.40","49.45.151.14","UNITEL LLC") in si
    
def test_if_none_detected():
  # check if the None values are detected
  clientIP = "176.59.168.2"
  local_file = getTopologyFilepath(clientIP, os.path.join('topos', '2025-04-28'))
  with open(local_file, 'r') as f:
    data = json.load(f)
    print(data)
    assert find_none(data), "None was not detected in the data even though there is at least one"

@pytest.mark.slow  
def test_downloadCAIDA():
  datasets_cache = os.path.join(TMP_CACHE_FOLDER, 'res')
  
  upstreams_cache = os.path.join(datasets_cache, 'as-upstreams-info')
  # invalidate cache
  # if os.path.exists(upstreams_cache) and os.path.isdir(upstreams_cache):
  #   shutil.rmtree(upstreams_cache)
  os.makedirs(upstreams_cache, exist_ok=True)
  
  current_time = time.time()
  upstreams_df = download_CAIDA_as_relationships(upstreams_cache)
  assert not upstreams_df.empty
  print("downloaded CAIDA AS relationships in", time.time() - current_time, "seconds")

# takes veeery long time to run
@pytest.mark.slow
def test_downloadIXPs():
  datasets_cache = os.path.join(TMP_CACHE_FOLDER, 'res')
  
  ixps_cache = os.path.join(datasets_cache, 'ixps')
  # invalidate cache
  if os.path.exists(ixps_cache) and os.path.isdir(ixps_cache):
        shutil.rmtree(ixps_cache)
  
  os.makedirs(ixps_cache, exist_ok=True)
  
  current_time = time.time()
  ixps_dataset = download_ixps_dataset(ixps_cache)
  
  assert(not ixps_dataset.empty)
  
  print("downloaded IXPs in", time.time() - current_time, "seconds") # took 24min to parse output
  
@pytest.mark.slow
def test_fillYGaps():
  datasets_cache = os.path.join(TMP_CACHE_FOLDER, 'res')
  
  ixps_cache = os.path.join(datasets_cache, 'ixps')
  ixps_dataset = download_ixps_dataset(ixps_cache)
  
  upstreams_cache = os.path.join(datasets_cache, 'as-upstreams-info')
  os.makedirs(upstreams_cache, exist_ok=True)
  upstreams_df = download_CAIDA_as_relationships(upstreams_cache)
  
  kwargs = {'upstreams-dir': upstreams_cache, 'caida-upstreams-df': upstreams_df, 'ixps_dataset': ixps_dataset}

  date = "2025-05-29"
  ytopos_urls = getYTopologiesGCSUrls(date, True)
  
  topo_cache = os.path.join(TMP_CACHE_FOLDER, 'ytopologies')
  if os.path.exists(topo_cache) and os.path.isdir(topo_cache):
      shutil.rmtree(topo_cache)
  os.makedirs(topo_cache, exist_ok=True)
  print("going through subnets:")
  for subnet in ytopos_urls.keys():
      print("subnet:", subnet)
      
      local_file = os.path.join(topo_cache, 'ytopologies-{}-{}.json'.format(*subnet.split('/')))
      downloadYTopologiesPerSubnet(ytopos_urls[subnet], local_file, kwargs, date, local_subnet = subnet.split('/'))
      try:
        with open(local_file, 'r') as f:
            data = json.load(f)
            if any(value is None for value in data.values()):
              assert False, f"None value found in {local_file}"
      except:
        print(f"Error reading {local_file}")
    
def test_recheckTopology():
  # check if the topology is rechecked
  clientIP = ""
  datasets_cache = os.path.join(TMP_CACHE_FOLDER, 'res')
  
  ixps_cache = os.path.join(datasets_cache, 'ixps')
  os.makedirs(ixps_cache, exist_ok=True)
  ixps_dataset = download_ixps_dataset(ixps_cache)
  
  upstreams_cache = os.path.join(datasets_cache, 'as-upstreams-info')
  os.makedirs(upstreams_cache, exist_ok=True)
  upstreams_df = download_CAIDA_as_relationships(upstreams_cache)
  
  kwargs = {'upstreams-dir': upstreams_cache, 'caida-upstreams-df': upstreams_df, 'ixps_dataset': ixps_dataset}
  date = "2025-05-29"

  local_subnet = ["2a04:cec0::", "29"]
  location = f"{LOCAL_TOPOS}/{date}/ytopologies-{local_subnet[0]}-{local_subnet[1]}.json"
  print("opening the local file: ", location)
  with open(location, 'r', encoding='utf-8') as f:
      data = json.load(f)

  ip_version = ipaddress.ip_network(data['subnet']).version
  print("subnet IP version:", ip_version)
  
  as_upstreams = get_as_upstreams(
      data['ASN'], kwargs['upstreams-dir'], kwargs['caida-upstreams-df'])[f'upstreams{ip_version}']

  print("AS upstreams:", as_upstreams)
  
  client_info = {'ASN': data['ASN'], 'ASName': data['ASName'], 'subnet': data['subnet']}
  
  data['topos'] = [topo for topo in data['topos'] if recheck_topology(topo, as_upstreams, kwargs['ixps_dataset'], client_info)]

  assert len(data['topos']) > 0, "No topologies found after rechecking"

  
def test_localhost():
  date = "2025-05-07"
  clientIP = "128.178.122.72"
   
  filepath = getTopologyFilepath(clientIP, os.path.join('topos', date))
  if filepath is None:
    assert False
    return json.dumps({'success': False, 'error': 'No Y-topology found in filepath'})
  print("found filepath:", filepath)
  with open(filepath) as json_file:
      ytopologies = json.load(json_file)
  
  ss, si = computeServerPairs(ytopologies)
  print("server pairs:", ss)
  
  assert ("del02","del04","UNITEL LLC") in ss
  assert ("del02","bom04","UNITEL LLC") in ss

  print("IP pairs:", si)

  assert ("61.246.223.40","49.45.151.104","UNITEL LLC") in si
  assert ("61.246.223.14","49.45.151.104","UNITEL LLC") in si
  assert ("61.246.223.40","49.45.151.14","UNITEL LLC") in si


def test_topofiles_exist():
    # check if the topology files exist
    date = "2025-08-19"
    ytopos_urls = {}
    bucket_root = "https://storage.googleapis.com/archive-measurement-lab/"
    prefix = f"wehe/ytopologies/{date}/"

    r = requests.get(bucket_root, params={"prefix": prefix, "delimiter": "/"})
    if r.status_code == 200:
        content = bs4.BeautifulSoup(r.text, "xml")
        for key in content.find_all("Key"):
          m = re.search(rf"{re.escape(prefix)}ytopologies-(.*?)-(.*?)-.*\.json$", key.getText())
          if m:
              ytopos_urls['/'.join(m.groups())] = urllib.parse.urljoin(bucket_root, key.getText())
    assert len(ytopos_urls.keys()) > 0, "No Y-topology files found for the date {}".format(date)
    
    print("ytopos_urls:", ytopos_urls)
    return ytopos_urls
    
def test_gcs():
    # run the download of Y topologies to see if it crashes
    date = "2025-08-19"
    
    topos_urls = test_topofiles_exist()
    for subnet, gcs_url in topos_urls.items():
      
      data = requests.get(gcs_url).json()
      try:
        print(f"Client info for subnet {subnet}: 'ASN': {data['ASN']}, 'ASName': {data['ASName']}, 'subnet': {data['subnet']}")
      except Exception as e:
        print(f"Error processing data for subnet {subnet}: {e}")
        continue
      
      assert data is not None, f"Data for {subnet} is None"
      