import csv
import numpy
import pandas as pd
from multiprocessing import Pool
from itertools import product,combinations
import time
start_time = time.time()
#df_state= pd.read_csv('d:/state.csv')
df_prob= pd.read_csv('d:/prob.csv')
df_substate= pd.read_csv('d:/substate.csv')
df_code= pd.read_csv('d:/code.csv')
x=100
def do_stuff(i):
   s1 = pd.Series(i, name='code')
   dff=pd.DataFrame({ 'code':s1.values})
   dff['bit']=1
   df_join = pd.merge(df_substate, dff, how='left', on=['code'])
   df_join.fillna(0 ,inplace=True)
   df_join['fin']=df_join['point']*x*df_join['bit']
   bygroup_number = df_join.groupby('substate')
   df_agg=bygroup_number['fin'].agg({'sum':'sum'})
   total=len(i)*x
   df_agg['substate2']=df_agg.index
   df_join_fin=pd.merge(df_agg, df_prob, on='substate2', how='inner')
   expected_i= (df_join_fin['sum']-total)*df_join_fin['prob']
   expect_fin= sum(expected_i)
   return (expect_fin,i)

def main():
  stuff=df_code['code']
  for L in range(1, 5):
             pool=Pool()
             result=pool.map(do_stuff,  combinations(stuff, L))
             pool.close()
             pool.join()
             print(max(result, key=lambda x:x[0]))
  print("Run time: %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
  main()

