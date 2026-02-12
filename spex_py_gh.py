# *************************************************************************** #
# *************************************************************************** #
# %% Prepare data                                                             
# *************************************************************************** #
# *************************************************************************** #

import pandas as pd
import numpy as np
import pyarrow.feather as feather
import os
import pyreadr as pr
import datetime
from scipy import stats

%xmode Minimal

# *********************************** #
# %%% Publications                    
# *********************************** #

# Import and downcast cwts-wos file.
pubs = pd.read_csv('eda_pub_cit_2025.txt', delimiter='\t', dtype=str)
pubs.drop(columns=['cwts_pmid', 'ut'], inplace=True)
pubs = pubs.rename(columns={
  'eda_pmid': 'pmid', 
  'pub_year': 'year',
  'meso_cluster_id': 'meso_cid', 
  'micro_cluster_id': 'micro_cid',
  'doc_type_id': 'type'
})
pubs = pubs.replace(",", ".", regex=True).replace("NaN", pd.NA).astype({
    'pmid':      'int32',
    'year':      'Int16',
    'type':      'Int8',
    'meso_cid':  'Int16',
    'micro_cid': 'Int16',
    'cs_5':      'Int32',
    'cs_10':     'Int32',
    'ncs_5':     'float64',
    'ncs_10':    'float64',
    "top1_5":    'float32',
    "top1_10":   'float32',
    "top5_5":    'float32',
    'top5_10':   'float32'
})
pubs['year'] = (pubs['year'] - 1970).astype('Int8')
feather.write_feather(pubs, 'pubs_full.feather')

# Remove publications without normalized citations data
pubs = pubs[pubs['ncs_5'].notna() & pubs['meso_cid'].notna()]
pubs = pubs.astype({
  'year':      'int8',
  'type':      'int8',
  'meso_cid':  'int16',
  'micro_cid': 'int16',
  'cs_5':      'int32',
  'cs_10':     'int32'
})

# Remove 523 PMID duplicates
pubs = pubs.drop_duplicates(subset='pmid', keep=False)

feather.write_feather(pubs, 'pubs_clean.feather')
del pubs

# *********************************** #
# %%% MeSH, part 1                    
# *********************************** #

# Prepare MeSH data

# For the 'Author-PMID-Year' part, we need a list of PMIDS with MeSH. But to
# complete the MeSH processing (computing weights), we need the number of
# authors for each PMID, which is made in 'Author-PMID-Year'. Therefore, the
# processing of MeSH is split in two parts.

mesh = pd.read_csv(
  'eda_mesh_2025.txt', 
  sep='\t', 
  dtype={
    'pmid': 'int32',
    'descriptor_ui': 'str',
    'descriptor_major': 'bool',
    'qualifier_major': 'str'},
  na_values='NaN', 
  usecols=['pmid', 'descriptor_ui', 'descriptor_major', 'qualifier_major']
)
mesh['muid'] = (
  mesh['descriptor_ui']
  .str.replace('D', '')
  .str.lstrip('0')
  .astype('uint32')
)
mesh['qmr'] = mesh['qualifier_major'].eq('True')
feather.write_feather(mesh, 'mesh_raw.feather')

descriptor_key = (
  mesh[['descriptor_ui', 'muid']]
  .drop_duplicates('muid')
  .sort_values(by='muid')
)
feather.write_feather(descriptor_key, 'descriptor_key.feather')

# If any mesh-qualifier is major, then mesh is maj
mesh['mjr'] = np.where(
  (mesh['descriptor_major'] | mesh['qmr']),
  True,
  False
)

mesh.drop(columns=['descriptor_ui', 'descriptor_major', 'qualifier_major', 'qmr'], inplace=True)

# Remove check tags
mesh = mesh[~mesh['muid'].isin([5260, 8297])] 

mesh = (
  mesh[['pmid', 'muid', 'mjr']] 
  .groupby(['pmid', 'muid']) 
  .agg({'mjr': 'any'}).reset_index()
)

# Average number of MeSH and major
nm = mesh.groupby('pmid').size().mean().round(2)
nmjr = mesh.query('mjr').groupby('pmid').size().mean().round(2)

feather.write_feather(mesh, 'mesh_no-weights.feather')

del mesh, descriptor_key, nm, nmjr

# *********************************** #
# %%% Author-PMID-Year                    
# *********************************** #

# Here we make 'apy' showing each author of each PMID and the year of the PMID

# Merge author-pmid and pmid-year + only keep publications with MeSH
pmid_mesh = (
  pd.read_feather('mesh_no-weights.feather', columns = ['pmid'])
  .squeeze()
  .drop_duplicates(keep='first')
)

ap = pr.read_r('author_pmid.rds')[None]
ap.drop(columns = ['pmid_pos'], inplace = True)

# Is research more often than not conducted in teams?
naut = ap.groupby('pmid')['aid'].size()
answer = naut.mask(naut > 2, '+3').value_counts()
print(answer) #yes

del naut, answer 

ap = ap[ap['pmid'].isin(pmid_mesh)]

py = pd.read_csv('pmid_year.txt', sep = '\t')
py = py[py['pmid'].isin(pmid_mesh)]

apy = pd.merge(ap, py, on = 'pmid')
del ap, py, pmid_mesh

apy = apy.rename(columns={'pub_year': 'year'})
apy['year'] = apy['year'] - 1970
apy = apy.astype({'year' : 'int8', 'pmid' : 'int32'})

feather.write_feather(apy, 'apy_all.feather')

# For each PMID, compute number of authors
n_authors = apy['pmid'].value_counts().reset_index()
n_authors = n_authors.rename(columns = {'count' : 'n'})
n_authors['n'] = n_authors['n'].astype('int16')
feather.write_feather(n_authors, 'n_authors.feather')
del n_authors

# Publications where each author has miniumum 3 previous publications --------

# Create column showing number of previous publications. npp initially shows
# the total number of publication including current year, so subtract 1 from
# year in the end.
apy['npp'] = apy.groupby('aid')['year'].transform(lambda x: x.rank(method='min') - 1).astype('int16')

feather.write_feather(apy, 'apy_t1.feather')

# For each PMID, show npp for author with fewest npp
t1 = apy.groupby('pmid').agg(min_npp=('npp', 'min')).reset_index()

# Shows proportion of pmids where min_npp is x or larger
t2 = (t1['min_npp'].value_counts(normalize=True)
  .sort_index(ascending = False)
  .cumsum()
  .round(4)
  .reset_index()
  .sort_values(by='min_npp'))
t2.to_excel('stats.xlsx', sheet_name='npp', index=False)

# Now select pmids, where min_npp is 3 or larger
t1 = t1[t1['min_npp'] >= 3]

apy2 = apy[apy['pmid'].isin(t1['pmid'])]
feather.write_feather(apy2, 'apy_npp3.feather')

# Keep publications from year 2000 -------------------------------------------

apy3 = apy2[apy2['year'] >= 30]
feather.write_feather(apy3, 'apy_npp3_2000.feather')

apy_cols = pd.DataFrame({
  'col': apy3.columns,
  'index': range(len(apy3.columns))
}).head(3)
feather.write_feather(apy_cols, 'apy_cols.feather')

apy_np = apy3[['aid', 'pmid', 'year']].to_numpy()
np.save('apy.npy', apy_np)

del apy, apy2, apy3, t1, t2

# Save individual files for each year (used in loop) -------------------------
pmids = pd.read_feather('pubs_clean.feather')['pmid']

for i in range(30, 40):
  t1 = apy_np[(apy_np[:, 2] == i) & np.isin(apy_np[:, 1], pmids)]
  t1 = t1[t1[:, 1].argsort()]
  t1 = t1[:, [0, 1]]
  np.save(f'apy{i}.npy', t1)

del apy_cols, apy_np, i, t1, pmids

# *********************************** #
# %%% MeSH, part 2                 
# *********************************** #

mesh = pd.read_feather('mesh_no-weights.feather')

# Add weights, diveded by number of terms / authors
mesh = pd.merge(mesh, pd.read_feather('n_authors.feather'), on = 'pmid')
mesh['w'] = np.where(mesh['mjr'], 3, 1)
mesh['sumw'] = mesh.groupby(['pmid'])['w'].transform('sum')
mesh['w_w'] = mesh['w'] / mesh['sumw']
mesh['w_w_a'] = mesh['w_w'] / mesh['n']
mesh.drop(columns = ['w', 'sumw', 'n'], inplace = True)

feather.write_feather(mesh, 'mesh_df.feather')

# PMIDS after year 2000
py = pd.read_csv('pmid_year.txt', sep = '\t')
py = py[py['pub_year'] >= 2000]['pmid']
mesh = (mesh[mesh['pmid'].isin(py)]
        .sort_values(by=['pmid', 'muid'])
        .reset_index(drop=True)
)
feather.write_feather(mesh, 'mesh_df2000.feather')

# Files for each year, used in loop
pmids = pd.read_feather('pubs_clean.feather')['pmid']
py = pd.read_csv('pmid_year.txt', sep = '\t')
py = py[(py['pub_year'] > 1999) & (py['pmid'].isin(pmids))]

for i in range(30, 40):
  t1 = py[py['pub_year'] == (i+1970)]['pmid']
  t2 = mesh[mesh['pmid'].isin(t1)].copy()
  feather.write_feather(t2, f'mesh_{i}.feather')
  t2['w'] = np.where(t2['mjr'], 1, 0)
  t3 = t2[['pmid', 'muid', 'w']].to_numpy(dtype='int32')
  np.save(f'mesh_np{i}.npy', t3)

mesh_cols = pd.DataFrame({
  'col': ['pmid', 'muid', 'w'], 
  'index': range(0,3)}
)
feather.write_feather(mesh_cols, 'mesh_cols.feather')

del i, mesh, mesh_cols, py, t1, t2, t3, pmids

# *********************************** #
# %%% Author-Year-Mesh                 
# *********************************** #

# For each author for each year, shows the number of points recieved for each
# MeSH. Done in chunks due to memory limitations. First create the individual
# files...

apy = pd.read_feather("apy_all.feather", columns=["aid", "pmid", "year"])
mesh = pd.read_feather("mesh_df.feather")

index = np.array_split(np.arange(0, len(apy)), 10)

for i in range(10):
    print(pd.Timestamp.now().strftime("%H:%M:%S"), i)
    aym = (
        pd.merge(apy.iloc[index[i]], mesh, on='pmid')
        .groupby(["aid", "muid", "year"])
        .agg({"w_w": "sum", "w_w_a": "sum"})
        .reset_index()
        .sort_values(by=["aid", "year", "muid"])
        .reset_index(drop=True)
    )

    feather.write_feather(aym, f'aym_recieved{i}.feather')

del apy, mesh, aym, index

# ... then stack them

files = [f'aym_recieved{i}.feather' for i in range(10)]
df_list = [pd.read_feather(file) for file in files]
aym = pd.concat(df_list, ignore_index=True)

feather.write_feather(aym, 'aym_recieved.feather')

del df_list, files

# Split this into 10 chunks based on authors

aym = pd.read_feather('aym_recieved.feather')

authors = np.array_split(aym['aid'].unique(), 10)

for a in range(10):
  print(pd.Timestamp.now().strftime("%H:%M:%S"), a)
  aym_a = aym[aym['aid'].isin(authors[a])].drop(columns='w_w_a')
  feather.write_feather(aym_a, f'aym_authors{a}.feather')

del aym, aym_a, authors

# Now, for the years 1999-2009 show cumulative amount of MeSH points recieved 
# in the year or preceeding years.

for a in range(10):
    for y in range(29, 40):
        print(pd.Timestamp.now().strftime("%H:%M:%S"), 'a', a, 'y', y)
        aym_a_y = (
            pd.read_feather(f'aym_authors{a}.feather')
            .query('year <= @y')
            .groupby(['aid', 'muid'])
            ['w_w'].sum()
            .reset_index()
        )
        feather.write_feather(aym_a_y, f'aym_{a}_{y}.feather')
        del aym_a_y

# Collect the authors chunks, so there is 1 file pr year

for y in range(29, 40):
    print(pd.Timestamp.now().strftime("%H:%M:%S"), y)

    files = [f'aym_{a}_{y}.feather' for a in range(10)]
    df_list = [pd.read_feather(file) for file in files]
    aym_y = pd.concat(df_list, ignore_index=True)

    feather.write_feather(aym_y, f'aym{y}.feather')
    del df_list, files, aym_y

del y

# *********************************** #
# %%% Term similarity matrix                      
# *********************************** #

# This is made in the R-script. Here we just convert to Python-style-data and
# sort
sm = pd.read_feather('sm.feather')
sm.columns = pd.to_numeric(sm.columns.str.lstrip('X'), downcast = 'integer')
sm.index = sm.columns
sm.sort_index(axis=0, inplace=True)
sm.sort_index(axis=1, inplace=True)

feather.write_feather(sm, 'sm_df.feather')

del sm

# *********************************** #
# %%% PMID-author similarity              
# *********************************** #

# Here we calculate the specialisation and expertise of team with respect to a
# PMID. It is also done for each author, although this is not used in the
# current paper. Because the files are large, the calculations are split up in
# 10 parts for each year. 

sm_full = pd.read_feather('sm_df.feather')

mesh_ic = pd.read_feather('mesh_ic.feather')[['muid', 'ic']]
mesh_ic.index = mesh_ic['muid']
mesh_ic.drop(columns = 'muid', inplace = True)

for year in range(30, 39):  
  # We first subset for each year...
  
  apy = np.load(f'apy{year}.npy')
  
  pmids_list = np.array_split(
    np.unique(apy[:, 1]), 
    10)
  
  my = year-1
  aym = pd.read_feather(f'aym{my}.feather')
  
  mesh = np.load(f'mesh_np{year}.npy')
  
  for i in range(0, 10):
    print(f"Year {year}, part {i}, {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    #... and then within each year we subset for each part (1/10 and so on) ...
    
    pmids = pmids_list[i]
    
    apy_i = apy[np.isin(apy[:, 1], pmids)]
    
    aym_i = aym[aym['aid'].isin(apy_i[:, 0])]    
    
    mesh_i = mesh[np.isin(mesh[:, 0], pmids)]
           
    muids_i = np.union1d(mesh_i[:, 1], aym['muid'])
      
    sm = sm_full.loc[muids_i, muids_i]
    
    a_sim, t_sim = [], []
    
    for p in pmids:
      ### ... and finally we subset within each PMID ------
      
      #MeSH of publication
      mesh_t1 = mesh_i[mesh_i[:, 0] == p]
      
      #Authors of publication
      aids = apy_i[apy_i[:, 1] == p, 0]
      
      #MeSH points of authors
      aym_t1 = aym_i[aym_i['aid'].isin(aids)] 
          
      #Soft cosine, similarity matrix for MeSH in authors or publication
      a_muids = np.sort(
          aym_t1['muid']
          .drop_duplicates(keep='first')
          .to_numpy())
      sc_muids = np.union1d(a_muids, mesh_t1[:, 1])
      sm_sc = sm.loc[sc_muids, sc_muids]
      
      w_sc = np.where(mesh_t1[:, 2] == 0, 1, 3) # Weights
      
      ### For each author ---------------------------------
      for author in aids: 
        
        #Mesh of author
        mask = aym_t1['aid'].values == author
        am = aym_t1.loc[mask, ['muid', 'w_w']].values
        
        # Subset similarity matrix, only has MeSH in author or publication
        muids = np.union1d(am[:, 0], mesh_t1[:, 1])
        S = sm_sc.loc[muids, muids].to_numpy()

        #Create equal sized vectors, store in a matrix    
        vec_mat = np.zeros((muids.shape[0], 3))
        vec_mat[:, 0] = muids
        
        idx_a = np.searchsorted(muids, am[:, 0])
        idx_b = np.searchsorted(muids, mesh_t1[:, 1])
        
        vec_mat[idx_a, 1] = am[:, 1]
        vec_mat[idx_b, 2] = w_sc
        
        #Multiply vectors by their IC
        ic = mesh_ic.loc[muids].to_numpy().flatten()
        
        a = vec_mat[:, 1] * ic
        b = vec_mat[:, 2] * ic
        
        #Calculate specialisation (_s) and expertise (_e)
        Sb = S @ b
        sqrt_bSb = np.sqrt(b @ Sb)
        aSb = a @ Sb
        sc_a_s = aSb / (np.sqrt(a @ S @ a) * sqrt_bSb)
        sc_a_e = aSb / sqrt_bSb
        
        #Append 
        a_sim.append([p, author, sc_a_s, sc_a_e])
      
      ### For the team ------------------------------------
      
      #Single author-publications
      if aids.shape[0] == 1:   
        sc_tsum_e = sc_tmean_e = sc_tmax_e = sc_a_e
        sc_tsum_s = sc_tmean_s = sc_tmax_s = sc_a_s
      else:
        team = (aym_t1
          .groupby('muid')['w_w']
          .agg(['sum', 'mean', 'max'])
          .reset_index() #muid gets :, 0 - max gets :, 3
          .to_numpy())
        
        S = sm_sc.to_numpy()
        
        #Create equal sized vectors, store in a matrix    
        vec_mat = np.zeros((sc_muids.shape[0], 5))
        vec_mat[:, 0] = sc_muids
        
        idx_a = np.searchsorted(sc_muids, team[:, 0])
        idx_b = np.searchsorted(sc_muids, mesh_t1[:, 1])
        
        vec_mat[idx_a, 1] = team[:, 1]
        vec_mat[idx_a, 2] = team[:, 2]
        vec_mat[idx_a, 3] = team[:, 3]
        vec_mat[idx_b, 4] = w_sc
        
        ic = mesh_ic.loc[sc_muids]
        if not (all(ic.index == vec_mat[:, 0])):
          print("Error: ic.index don't match team")
          break
        ic = ic.to_numpy().flatten()
        
        a_sum  = vec_mat[:, 1] * ic
        a_mean = vec_mat[:, 2] * ic
        a_max  = vec_mat[:, 3] * ic
        b      = vec_mat[:, 4] * ic
        
        #Calculate
        Sb       = S @ b
        sqrt_bSb = np.sqrt(b @ Sb)
        a_sumSb  = a_sum  @ Sb
        a_meanSb = a_mean @ Sb
        a_maxSb  = a_max  @ Sb
        
        sc_tsum_s  = a_sumSb  / (np.sqrt(a_sum @ S @ a_sum) * sqrt_bSb)
        sc_tmean_s = a_meanSb / (np.sqrt(a_mean @ S @ a_mean) * sqrt_bSb)
        sc_tmax_s  = a_maxSb  / (np.sqrt(a_max @ S @ a_max) * sqrt_bSb)
        
        sc_tsum_e  = a_sumSb  / sqrt_bSb
        sc_tmean_e = a_meanSb / sqrt_bSb
        sc_tmax_e  = a_maxSb  / sqrt_bSb
      
      #Append ---------------------------------------------
      t_sim.append(
        [p, sc_tsum_s, sc_tmean_s, sc_tmax_s, sc_tsum_e, sc_tmean_e, sc_tmax_e]
      )
    
    a_sim_np = np.array(a_sim)
    t_sim_np = np.array(t_sim)
    
    np.save(f'a_sim_{year}_{i}.npy', a_sim_np)
    np.save(f't_sim_{year}_{i}.npy', t_sim_np)
    
del a, apy, apy_i, aym, aym_i, a_max, a_maxSb, a_mean, a_meanSb, a_muids, a_sim, a_sim_np, a_sum, a_sumSb, aids, am, aSb, author, aym_t1, b, i, ic, idx_a, idx_b, mask, mesh_t1, muids, muids_i, my, p, S, Sb, sc_a_e, sc_a_s, sc_muids, sc_tmax_e, sc_tmax_s, sc_tmean_e, sc_tmean_s, sc_tsum_e, sc_tsum_s, sm_sc, sqrt_bSb, team, vec_mat, w_sc, year, mesh, mesh_i, mesh_ic, pmids, pmids_list, sm, sm_full, t_sim, a_sim, t_sim_np

# *********************************** #
# %%% Process data                      
# *********************************** #

# Authors -----------------------------

files = [f"a_sim_{year}_{i}.npy" for year in range(30, 38) for i in range(10)]
data = np.concatenate([np.load(f) for f in files])

a_df = pd.DataFrame(data)
a_df.columns = ['pmid', 'aid', 'sc_s', 'sc_e']
a_df = a_df.astype({'pmid': 'int32', 'aid': 'int32'})

feather.write_feather(a_df, 'a_sim_df.feather')

# Team --------------------------------

# Combine individual files created in the loop
files = [f"t_sim_{year}_{i}.npy" for year in range(30, 38) for i in range(10)]
data = np.concatenate([np.load(f) for f in files])

t_df = pd.DataFrame(data)
t_df.columns = ['pmid', 'sc_sum_s', 'sc_mean_s', 'sc_max_s', 'sc_sum_e', 'sc_mean_e', 'sc_max_e']
t_df = t_df.astype({'pmid': 'int32'})

feather.write_feather(t_df, 't_sim_df.feather')

# Merge to get...
t1 = (
  pd.merge(t_df, pd.read_feather('pubs_clean.feather'), on = 'pmid') # citation impact
  .merge(pd.read_feather('n_authors.feather'),  on = 'pmid') # number of authors
  .rename(columns={'n': 'n_authors'})
)

t2 = ( #Number of previous publications of the author team
  pd.read_feather('apy_t1.feather')
  .groupby('pmid')['npp']
  .agg(npp_sum='sum', npp_mean='mean', npp_median='median')
  .reset_index()
)      

team = pd.merge(t1, t2, on = 'pmid')

# Dichotomize topX% indicators
np.random.seed(123)
for col in ['top1_5', 'top1_10', 'top5_5', 'top5_10']:
    team[f'{col}_b'] = np.random.binomial(1, team[col]).astype(bool)

# log-transform expertise and number of previous publications
for col in ['sc_sum_e', 'sc_mean_e', 'sc_max_e', 'npp_sum']:
    team[f'{col}_ln'] = np.log(team[col])

# Save all data + publications with 11 authors or less.
feather.write_feather(team, 'team.feather')
team['type'] = np.where(team['type'] == 2, 1, 0)

team11 = team[team['n_authors'] <= 11]
feather.write_feather(team11, 'team11.feather')

#Average number of MeSH + major terms
mesh = pd.read_feather('mesh_no-weights.feather')[['pmid', 'mjr']]
mesh = mesh[mesh['pmid'].isin(team11['pmid'])]
nm = mesh.groupby('pmid').size().mean().round(2)
nmjr = mesh.query('mjr').groupby('pmid').size().mean().round(2)

del a_df, col, data, files, t1, t2, t3, t_df, team, team11, transformed_values, mesh, nm, nmjr

# ************************* #
# %%%% Fixed-effects         
# ************************* #

# Final step is to produce a dataset that 1) includes a group-identifier 
# column, 2) only has pmids where the group has at least 2 publications.

team = pd.read_feather('team11.feather')

# Add group-identifier
groups = pd.read_feather('apy_npp3_2000.feather')[['aid', 'pmid']]
groups = groups[groups['pmid'].isin(team['pmid'])]

groups = (
  groups
  .groupby('pmid')
  .agg(lambda x: '-'.join(sorted(x.astype(str))))
  .reset_index()
  .rename(columns={'aid': 'gid'})
)
groups['gid'] = pd.factorize(groups['gid'])[0]

# Remove groups with less than 2 publications
groups['gid_n'] = groups['gid'].map(groups['gid'].value_counts())
groups = groups[groups['gid_n'] > 1].drop(columns='gid_n')

feather.write_feather(groups, 'groups.feather')

# Merge onto full data
team11_fe = pd.merge(groups, team, on = 'pmid')

feather.write_feather(team11_fe, 'team11_fe.feather')

del groups, team11_fe, team

# *************************************************************************** #
# *************************************************************************** #
# %% Analyze                                                             
# *************************************************************************** #
# *************************************************************************** #

# In this section we just make the descriptive tables and figures. The
# regression models are made in R.

import pandas as pd
import numpy as np
import pyarrow.feather as feather
import seaborn as sns
import matplotlib.pyplot as plt
import os
os.chdir(r'C:\Users\au544242\OneDrive - Aarhus universitet\Ph.d\Projekter\Spex\Code\parts')

pd.set_option('display.float_format', '{:.6f}'.format)

team2 = pd.read_feather('team11.feather').query('n_authors > 1')
team2['top5_10_b'] = team2['top5_10_b'].astype(int)

# *********************************** #
# %%% Descriptive stats
# *********************************** #

# Descriptive stats
table = pd.DataFrame({})
cols = ['sc_max_s', 'sc_max_e_ln', 'top5_10_b', 'n_authors', 'npp_sum_ln', 'type']
table = team2[cols].describe(
    percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]
).round(3)
for col in ['sc_max_e_ln', 'n_authors', 'npp_sum_ln']: table[col] = table[col].round(1)
table = table.T
table.drop(columns=['count'], inplace = True)
table.index = ['Specialisation', 'ln(Expertise)', 'Top5%', '#Authors', 'ln(#Previous publications)', 'Publication type']

with pd.ExcelWriter(
        "../results/stats.xlsx",
        mode="a",
        engine="openpyxl",
        if_sheet_exists="replace",
    ) as writer:
    table.to_excel(writer, sheet_name="desc") 

# Comparing means
means = team2.groupby('top5_10_b')[['sc_max_e_ln', 'sc_max_s']].mean()

means['sc_max_e_ln'] = means['sc_max_e_ln'].round(1)
means['sc_max_s'] = means['sc_max_s'].round(3)

with pd.ExcelWriter(
        "../results/stats.xlsx",
        mode="a",
        engine="openpyxl",
        if_sheet_exists="replace",
    ) as writer:
    means.to_excel(writer, sheet_name="means") 
    
# Density plots
p99 = team2['sc_max_e'].quantile(0.99)

fig, axes = plt.subplots(1, 3, figsize=(15, 5))

sns.kdeplot(team2['sc_max_s'], color='black', ax=axes[0]).set(xlabel='Specialisation', ylabel=None)
sns.kdeplot(team2['sc_max_e_ln'], color='black', ax=axes[1]).set(xlabel='ln(Expertise)', ylabel=None)
sns.kdeplot(team2[team2['sc_max_e'] <= p99]['sc_max_e'], color='black', ax=axes[2]).set(xlabel='Expertise, truncated at 99th %ile', ylabel=None)

for ax in axes:
    ax.set_xlabel(ax.get_xlabel(), fontsize=17)
    ax.tick_params(axis='both', labelsize=15)

plt.tight_layout()
plt.savefig('../results/density_x.png')
plt.show()