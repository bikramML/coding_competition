import xml.etree.ElementTree as ET
import pandas as pd
import pyedflib
import numpy as np
'''
xml_to_df(file_name,search_child,search_element,cols,save_file) , This function take xml file and convert it to CSV and take the file name where to save


'''

def xml_to_df(file_name,search_child,search_element,cols,save_file= None):

	tree=ET.parse(file_name)
	root=tree.getroot()

	df=pd.DataFrame(columns=cols)
	for results in root.iter(search_child): 
		if(results.find(search_element['name']).text==search_element['value']):
			df=df.append(pd.DataFrame([[results.find(x).text for x in cols]],columns=cols),ignore_index=True)
	if save_file is not None:
		df.to_csv(save_file,index=False)
	return df
df1=xml_to_df(file_name="data/apnea/mesa-sleep-0001-profusion.xml",search_child="ScoredEvent",search_element={'name':'Name','value':'Hypopnea'},cols=['Duration','Input','Name','Start'],save_file="task/mesa-sleep-0001-profusion.xml.csv")

df2=xml_to_df(file_name="data/stage/mesa-sleep-0001-nsrr.xml",search_child="ScoredEvent",search_element={'name':'EventType','value':'Stages|Stages'},cols=['Duration','EventConcept','Start','EventType'])
save_file="task/mesa-sleep-0001-nsrr.xml.csv"
df2 = df2.rename(columns={'EventConcept': 'Stages', 'EventType': 'Type'})
df2.to_csv(save_file,index=False)

f = pyedflib.EdfReader("data/flow/mesa-sleep-0001.edf")

signal_labels = np.array(f.getSignalLabels())
flow_index=np.where(signal_labels=='Flow')[0]

idex=[x*(1.0/32.0) for x in range(f.getNSamples()[flow_index][0])]
datas=[np.ndarray.tolist(f.readSignal(flow_index))][0]
f._close()
df3=pd.DataFrame([idex,datas])
df3=df3.T

df3.columns=["Epoch","Flow"]
df3=pd.DataFrame(df3)
df3.to_csv("task/mesa-sleep-0001.edf.csv",index=False)
colum_values=['Time','Flow','Apenea','Stage']
df4=pd.DataFrame(columns=colum_values)
x=500.0 # As 500 second requered 
n=0.0 
i_n=(1.0/32.0) #
def_df1='0' 
def_df2='0'
def_df31='0'
def_df32='0'
m=0
while n <x:
	i_df1=df1[((df1['Start']).astype(float)<=n)&(((df1['Start'].astype(float))+(df1['Duration'].astype(float)))>=n)]#df1[(float(df1['Start'])<=n)&((float(df1['Start'])+float(df1['Duration']))>=n)]
	i_df2=df2[((df2['Start']).astype(float)<=n)&(((df2['Start'].astype(float))+(df2['Duration'].astype(float)))>=n)]#df2[(float(df2['Start'])<=n)&((float(df2['Start'])+float(df2['Duration']))>=n)]
	i_df1_len=len(i_df1)
	i_df2_len=len(i_df2)
	
	if i_df1_len > i_df2_len:
		for s in range(i_df1_len):
			i_df3=df3[df3['Epoch'].astype(float)==n]
			if len(i_df3) >0:
				def_df31=i_df3['Epoch'][m]
				def_df32=i_df3['Flow'][m]
				if s < i_df2_len:
					def_df1=i_df1['Name'][s]
					def_df2=i_df2['Stages'][s]
					df4=df4.append(pd.DataFrame([[def_df31,def_df32,def_df1,def_df2]],columns=colum_values),ignore_index=True)
				else:
					def_df1=i_df1['Name'][s]
					df4=df4.append(pd.DataFrame([[def_df31,def_df32,def_df1,def_df2]],columns=colum_values),ignore_index=True)
			n = n+(i_n)
			m=m+1
	else:
		for s in range(i_df2_len):
			i_df3=df3[df3['Epoch'].astype(float)==n]
			if len(i_df3) >0:
				def_df31=i_df3['Epoch'][m]
				def_df32=i_df3['Flow'][m]
				if s < i_df1_len:
					def_df1=i_df1['Name'][s]
					def_df2=i_df2['Stages'][s]
					df4=df4.append(pd.DataFrame([[def_df31,def_df32,def_df1,def_df2]],columns=colum_values),ignore_index=True)
				else:
					def_df2=i_df2['Stages'][s]
					df4=df4.append(pd.DataFrame([[def_df31,def_df32,def_df1,def_df2]],columns=colum_values),ignore_index=True)
			n = n+(i_n)
			m=m+1
del df1
del df2
del df3
df4.to_csv("task/combined/mesa-sleep-0001.csv",index=False)
del df4

