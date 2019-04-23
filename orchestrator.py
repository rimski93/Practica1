from cos_backend import COSBackend
import yaml
import itertools
import sys
from ibm_cf_connector import CloudFunctions
import time
tempsi = 0
tempsf = 0

fitxer = sys.argv[1]
totalmaps = int(sys.argv[2])

with open('./ibm_cloud_config_d', 'r') as config_file:
	res = yaml.safe_load(config_file)
cos=COSBackend(res)
f1=cos.get_object(res['ibm_cos']['repositori'],fitxer,True)
cfConn = CloudFunctions(res)

f1=open('mapper.zip')
cfConn.create_action('mapper',f1.read(),is_binary=True)
f2=open('reducer.zip')
cfConn.create_action('reducer',f2.read(),is_binary=True)

longitud= cos.head_object(res['ibm_cos']['repositori'],fitxer)
longit= longitud['content-length']
chunk= int(longit)/totalmaps
tempsi = time.clock()
for x in range(totalmaps):
	aux = dict(res)
	aux.update({'nmaper':str(x)})
	aux.update({'rangin': str(x*chunk)})
	aux.update({'rangout': str(x*chunk+chunk)})
	aux.update({'fitxer': fitxer})
	cfConn.invoke('mapper', payload=aux)

aux = dict(res)
aux.update({'chunks': str(totalmaps)})


print cfConn.invoke_with_result('reducer',aux)
tempsf= time.clock()
tempsTot=tempsf-tempsi
print "L' execucio ha tardat ",tempsTot," segons."
