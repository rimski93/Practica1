from cos_backend import COSBackend

def main(cos_config):
	diccionari = {}
	
	cos=COSBackend(cos_config)

	for x in range(int(cos_config['chunks'])):
		fit = 'fitxer_pujat'+str(x)
		trobat=False
		while trobat is not True:
			trobat=True
			try:
				cos.head_object(cos_config['ibm_cos']['repositori'],fit)
			except Exception as e:
				trobat=False
	countWords=0
	for x in range(int(cos_config['chunks'])):
		fit = 'fitxer_pujat'+str(x)
		f1=cos.get_object(cos_config['ibm_cos']['repositori'],fit,True)
		f1=f1.read().decode("utf-8")
		f1 = f1.strip(':').split(',')
		for line in f1:
			sep= line.split(':')
			par = sep[0]
			par=par.replace("{", "")
			par=par.replace("'", "")
			conta= sep[1]
			conta=conta.replace('}',"")
			conta=int(conta)
			countWords=countWords+conta
			if par in diccionari:
				diccionari[par] +=conta
			else:
				diccionari[par] = conta

	cos.put_object(cos_config['ibm_cos']['repositori'],'reducer',str(diccionari))
	
	
	for x in range(int(cos_config['chunks'])):
		fit = 'fitxer_pujat'+str(x)
		cos.delete_object(cos_config['ibm_cos']['repositori'],fit)

	return {'Count Words':countWords}
