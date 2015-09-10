# -*- coding: utf-8 -*- 

import h5py
import numpy as np
import os
import shutil
import glob
from datetime import datetime

rics = [
"pp1509","pp1601",
"rb1509","rb1510","rb1601",
"ru1509","ru1601",
"fg509","fg601",
"ta509","ta601",
"ag1506","ag1512",
"au1506","au1512",
"cu1507","cu1508",
"ni1507","ni1508"
]

tdays = [
"20150504",
"20150505",
"20150506",
"20150507",
"20150508"
#"20150511",
#"20150512",
#"20150513",
#"20150514",
#"20150515",
#"20150518",
#"20150519",
#"20150520",
#"20150521",
#"20150522",
#"20150525",
#"20150526",
#"20150527",
#"20150528",
#"20150529"
]

def to_utc(x):
	dt = datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
	epoch = datetime.utcfromtimestamp(0)
	delta = dt - epoch
	return delta.total_seconds()

def to_time(x):
	dt = datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
	t = dt.hour*10000+dt.minute*100+dt.second+dt.microsecond/1000000.0
	return t

def to_side(x):
	return 1.0 if x == 'S' else 0.0
	
def dump2_h5(tday):
	""" the separate folders """
	day0 = datetime.strptime(tday, "%Y%m%d")
	h5 = h5py.File(tday+".h5", "w")
	h5.attrs["header"] = "datetime,last,position,increase,turnover,volume,open,close,type,side,bp01,ap01,bs01,as01"
	h5.attrs["tday"] = tday
	h5.attrs["side"] = "0-B,1-S"
	
	for mkt in ('dc','sc','zc'):
		for txt in glob.glob("D:/Tick/f_c201505d/f_c201505d/%s/%s/*.csv" % (mkt,tday)):
			if os.stat(txt).st_size == 0: continue
			ric = txt.split("\\")[-1].split(".")[0][:-9]
			if ric.lower() not in rics:
				continue
			print "==>", ric
			dat = np.genfromtxt(txt, delimiter=",", converters={2: to_utc, 2: to_time, 11: to_side}, skip_header=1, usecols=(2,3,4,5,6,7,8,9,11,12,13,14,15))
			ds = h5.create_dataset(ric, data=dat, dtype="float64", compression="gzip")

def process_rawtxt():
	for tday in tdays:
		print tday
		try:
			dump2_h5(tday)
			#shutil.rmtree("C:/stock/FoxTrader/DATA/SH/%s" % tday)
			#shutil.rmtree("C:/stock/FoxTrader/DATA/SZ/%s" % tday)
		except Exception,ex:
			print ex
		
process_rawtxt()
#process_7z()