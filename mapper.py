from cos_backend import COSBackend

def main(cos_config):
	cos=COSBackend(cos_config)
	
	x = cos_config['nmaper']
	fit = 'fitxer_pujat'+str(x)
	
	f1=cos.get_object(cos_config['ibm_cos']['repositori'],cos_config['fitxer'],True, extra_get_args={'Range': 'bytes='+cos_config["rangin"]+'-'+cos_config["rangout"]})

	i=0
	dic= {}
	f1=f1.read().decode("utf-8").split('\n')
	for line in f1:
		line=line.replace(",", " ")
		line=line.replace(".", " ")
		line=line.replace("!", " ")
		line=line.replace("?", " ")
		line=line.replace(";", " ")
		line=line.replace(":", " ")
		line=line.replace(")", " ")
		line=line.replace("(", " ")
		line=line.replace('"', ' ')
		line=line.replace("=", " ")
		line=line.replace("'", " ")
		line=line.replace("_", " ")
		line=line.replace("|", " ")
		line=line.replace("]", " ")
		line=line.replace("/", " ")
		line=line.replace("  ", " ")
		line=line.replace("#", " ")
		line=line.replace("-", "")
		line=line.replace("[", "")
		line=line.replace("*", "")

		paraules=line.split( )

		for par in paraules:
			par=par.lower()
			if par in dic:
				dic[par]+=1
			else:
				dic[par]=1
	cos.put_object(cos_config['ibm_cos']['repositori'],fit,str(dic))
		
		
