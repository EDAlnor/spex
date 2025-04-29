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
os.chdir(r'C:\Users\au544242\OneDrive - Aarhus universitet\Ph.d\Projekter\Topic switch\Code\testrun\parts')

%xmode Minimal

# *********************************** #
# %%% Publications                    
# *********************************** #

#Import and downcast cwts-wos file.
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

#Remove publications without normalized citations data
pubs = pubs[pubs['ncs_5'].notna() & pubs['meso_cid'].notna()]
pubs = pubs.astype({
  'year':      'int8',
  'type':      'int8',
  'meso_cid':  'int16',
  'micro_cid': 'int16',
  'cs_5':      'int32',
  'cs_10':     'int32'
})

#Remove 523 PMID duplicates
pubs = pubs.drop_duplicates(subset='pmid', keep=False)

feather.write_feather(pubs, 'pubs_clean.feather')
del pubs

# *********************************** #
# %%% MeSH, part 1                    
# *********************************** #

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
mesh['qualifier_major'] = mesh['qualifier_major'].fillna(False).astype(bool)

descriptor_key = (
  mesh[['descriptor_ui', 'muid']]
  .drop_duplicates('muid')
  .sort_values(by='muid')
)
feather.write_feather(descriptor_key, 'descriptor_key.feather')

#Remove check tags
mesh = mesh[~mesh['muid'].isin([5260, 8297])] 

#If any mesh-qualifier is major, then mesh is maj
mesh['mjr'] = np.where(
  (mesh['descriptor_major'] | mesh['qualifier_major']),
  True,
  False
)
mesh = (
  mesh[['pmid', 'muid', 'mjr']] 
  .groupby(['pmid', 'muid']) 
  .agg({'mjr': 'any'}).reset_index()
)

feather.write_feather(mesh, 'mesh_no-weights.feather')

del mesh, descriptor_key

# *********************************** #
# %%% Author-PMID-Year                    
# *********************************** #

#Merge author-pmid and pmid-year + only keep publications with MeSH
pmid_mesh = (
  pd.read_feather('mesh_no-weights.feather', columns = ['pmid'])
  .squeeze()
  .drop_duplicates(keep='first')
)

ap = pr.read_r('author_pmid.rds')[None]
ap.drop(columns = ['pmid_pos'], inplace = True)
ap = ap[ap['pmid'].isin(pmid_mesh)]

py = pd.read_csv('pmid_year.txt', sep = '\t')
py = py[py['pmid'].isin(pmid_mesh)]

apy = pd.merge(ap, py, on = 'pmid')
del ap, py, pmid_mesh

apy = apy.rename(columns={'pub_year': 'year'})
apy['year'] = apy['year'] - 1970
apy = apy.astype({'year' : 'int8', 'pmid' : 'int32'})

feather.write_feather(apy, 'apy_all.feather')

#For each PMID, Compute number of authors
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

#For each PMID, show npp for author with fewest npp
t1 = apy.groupby('pmid').agg(min_npp=('npp', 'min')).reset_index()

#Shows proportion of pmids where min_npp is x or larger
t2 = (t1['min_npp'].value_counts(normalize=True)
  .sort_index(ascending = False)
  .cumsum()
  .round(4)
  .reset_index()
  .sort_values(by='min_npp'))
t2.to_excel('stats.xlsx', sheet_name='npp', index=False)

#Now select pmids, where min_npp is 3 or larger
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
np.save('apy', apy_np)

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

#Add weights, diveded by number of terms / authors
mesh = pd.merge(mesh, pd.read_feather('n_authors.feather'), on = 'pmid')
mesh['w'] = np.where(mesh['mjr'], 3, 1)
mesh['sumw'] = mesh.groupby(['pmid'])['w'].transform('sum')
mesh['w_w'] = mesh['w'] / mesh['sumw']
mesh['w_w_a'] = mesh['w_w'] / mesh['n']
mesh.drop(columns = ['w', 'sumw', 'n'], inplace = True)

feather.write_feather(mesh, 'mesh_df.feather')

#PMIDS after year 2000
py = pd.read_csv('pmid_year.txt', sep = '\t')
py = py[py['pub_year'] >= 2000]['pmid']
mesh = (mesh[mesh['pmid'].isin(py)]
        .sort_values(by=['pmid', 'muid'])
        .reset_index(drop=True)
)
feather.write_feather(mesh, 'mesh_df2000.feather')

#Files for each year, used in loop
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
# MeSH
aym = (
    pd.merge(
        pd.read_feather("apy_all.feather", columns = ["aid", "pmid", "year"]),
        pd.read_feather("mesh_df.feather"),
        on = "pmid")
    .groupby(["aid", "muid", "year"])
    .agg({"w_w": "sum", "w_w_a": "sum"})
    .reset_index()
    .sort_values(by = ["aid", "year", "muid"])
    .reset_index(drop = True)
)

feather.write_feather(aym, 'aym_recieved.feather')

# For the years 1999-2009 show cumulative amount of MeSH points recieved in the
# year or preceeding years
for i in range(29, 40):
  print(f"Part {i} {datetime.datetime.now().strftime('%H:%M:%S')}")
  t1 = (
    aym[aym['year'] <= i]
    .groupby(['aid', 'muid'])
    .agg({"w_w": "sum", "w_w_a": "sum"})
    .reset_index())
  feather.write_feather(t1,  f'aym{i}.feather')

del aym, t1, i

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
  apy = np.load(f'apy{year}.npy')
  
  pmids_list = np.array_split(
    np.unique(apy[:, 1]), 
    10)
  
  my = year-1
  aym = pd.read_feather(f'aym{my}.feather')
  
  mesh = np.load(f'mesh_np{year}.npy')
  
  for i in range(0, 10):
    print(f"Year {year}, part {i}, {datetime.datetime.now().strftime('%H:%M:%S')}")
    
    pmids = pmids_list[i]
    
    apy_i = apy[np.isin(apy[:, 1], pmids)]
    
    aym_i = aym[aym['aid'].isin(apy_i[:, 0])]    
    
    mesh_i = mesh[np.isin(mesh[:, 0], pmids)]
           
    muids_i = np.union1d(mesh_i[:, 1], aym['muid'])
      
    sm = sm_full.loc[muids_i, muids_i]
    
    a_sim, t_sim = [], []
    
    for p in pmids:
      ### Subset objects used -----------------------------
      
      #Mesh of publication
      mesh_t1 = mesh_i[mesh_i[:, 0] == p]
      
      #Authors of publication
      aids = apy_i[apy_i[:, 1] == p, 0]
      
      #Author-year-mesh expertise of authors
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
        
        #Subset similarity matrix
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
        
        #Calculate
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

#Combine files
files = [f"t_sim_{year}_{i}.npy" for year in range(30, 38) for i in range(10)]
data = np.concatenate([np.load(f) for f in files])

t_df = pd.DataFrame(data)
t_df.columns = ['pmid', 'sc_sum_s', 'sc_mean_s', 'sc_max_s', 'sc_sum_e', 'sc_mean_e', 'sc_max_e']
t_df = t_df.astype({'pmid': 'int32'})

feather.write_feather(t_df, 't_sim_df.feather')

#Merge
t1 = pd.merge(t_df, pd.read_feather('pubs_clean.feather'), on = 'pmid')

t2 = pd.merge(t1, pd.read_feather('n_authors.feather'),  on = 'pmid')
t2 = t2.rename(columns = {'n': 'n_authors'})

t3 = (
  pd.read_feather('apy_t1.feather')
  .groupby('pmid')['npp']
  .agg(npp_sum='sum', npp_mean='mean', npp_median='median')
  .reset_index())      

team = pd.merge(t2, t3, on = 'pmid')

#Dichotomize topX% indicators
for col in ['top1_5', 'top1_10', 'top5_5', 'top5_10']:
    team[f'{col}_b'] = np.random.binomial(1, team[col]).astype(bool)

#Box-Cox transform expertise and number of previous publications
lambda_values = []

xs = ['sc_sum_e', 'sc_mean_e', 'sc_max_e', 'npp_sum']

for col in xs:
    transformed_values, lambda_ = stats.boxcox(team[col])
    team[f'{col}_bcx'] = transformed_values
    lambda_values.append({'column': col, 'lambda': lambda_})

lambda_df = pd.DataFrame(lambda_values)
feather.write_feather(lambda_df, 'lamba_df.feather')

# Save all data + publications with 11 authors or less.
feather.write_feather(team, 'team.feather')
team['type'] = np.where(team['type'] == 2, 1, 0)

team11 = team[team['n_authors'] <= 11]
feather.write_feather(team11, 'team11.feather')

del a_df, col, data, files, lambda_, lambda_df, lambda_values, t1, t2, t3, t_df, team, team11, transformed_values, xs 

# ************************* #
# %%%% Fixed-effects         
# ************************* #

team = pd.read_feather('team11.feather')

groups = pd.read_feather('apy_npp3_2000.feather')[['aid', 'pmid']]
groups = groups[groups['pmid'].isin(team['pmid'])]

#Add group-identifier
groups = (
  groups
  .groupby('pmid')
  .agg(lambda x: '-'.join(sorted(x.astype(str))))
  .reset_index()
  .rename(columns={'aid': 'gid'})
)
groups['gid'] = pd.factorize(groups['gid'])[0]

#Remove groups with less than 2 publications
groups['gid_n'] = groups['gid'].map(groups['gid'].value_counts())
groups = groups[groups['gid_n'] > 1].drop(columns='gid_n')

feather.write_feather(groups, 'groups.feather')

#Merge onto full data
team11_fe = pd.merge(groups, team, on = 'pmid')

feather.write_feather(team11_fe, 'team11_fe.feather')

del groups, team11_fe, team

# *************************************************************************** #
# *************************************************************************** #
# %% Analyze                                                             
# *************************************************************************** #
# *************************************************************************** #

import statsmodels.api as sm
import seaborn as sns
import pandas as pd
import numpy as np
import pyarrow.feather as feather
import os
import matplotlib.pyplot as plt
from stargazer.stargazer import Stargazer
import pickle
os.chdir(r'C:\Users\au544242\OneDrive - Aarhus universitet\Ph.d\Projekter\Topic switch\Code\testrun\parts')

pd.set_option('display.float_format', '{:.6f}'.format)

team = pd.read_feather('team11.feather')

#Function used to write the results to an .xlsx-file
def fnPd2xl(table, sheet_name):
    with pd.ExcelWriter(
        os.path.join(os.getcwd(), '..', 'stats.xlsx'),
        engine='openpyxl', mode='a', if_sheet_exists='replace'
      ) as writer:
        table.to_excel(writer, sheet_name=sheet_name)

# *********************************** #
# %%% Descriptive stats
# *********************************** #

#Descriptive stats
def fnT(var):
  return team[var].describe(percentiles = [.1, 0.25, .5, .75, 0.9]).round(6)

table = pd.DataFrame({})

for col in [['sc_max_s', 'sc_max_e_bcx', 'top5_10', 'ncs_10', 'n_authors', 'npp_sum_bcx', 'type']]: table[col] = fnT(col)

table = table.T
table.drop(columns=['count'], inplace = True)
table.index = ['Specialization', 'Expertise', 'Top5%_10', 'NCS_10', '#Authors', '#Previous publications', 'Publication type']

fnPd2xl(table, 'X')

# Distribution plot. 
p99 = team['sc_max_e'].quantile(0.99)

fig, axes = plt.subplots(1, 3, figsize=(15, 5))

sns.kdeplot(team['sc_max_s'], color='black', ax=axes[0]).set(xlabel='Specialisation', ylabel=None)
sns.kdeplot(team['sc_max_e_bcx'], color='black', ax=axes[1]).set(xlabel='Expertise, Box-Cox transformed', ylabel=None)
sns.kdeplot(team[team['sc_max_e'] <= p99]['sc_max_e'], color='black', ax=axes[2]).set(xlabel='Expertise, truncated at 99th %ile', ylabel=None)

for ax in axes:
    ax.set_xlabel(ax.get_xlabel(), fontsize=17)
    ax.tick_params(axis='both', labelsize=15)

plt.tight_layout()
plt.savefig('x.png')
plt.show()

# *********************************** #
# %%% Top5%
# *********************************** #

#Comparing means
t1 = team.groupby('top5_10_b')[['sc_max_e', 'sc_max_s', 'sc_max_e_bcx']].mean()
fnPd2xl(t1, 'top5_mean')

# Predicted probabilities -------------
probs = pd.DataFrame({
  'perc': ['min', '10%', '25%', 'mean', '75%', '90%', 'max'] * 2,
  'x': ['spec'] * 7 + ['exp'] * 7,
  'prob': [None] * 14
})

#Specialization
X = team[['n_authors', 'npp_sum_bcx', 'type', 'sc_max_s']]
X = sm.add_constant(X)
model = sm.Logit(team['top5_10_b'], X)
fit = model.fit()

pred = X.copy().head()
pred['n_authors'] = X['n_authors'].mean()
pred['npp_sum_bcx'] = X['npp_sum_bcx'].mean()
pred['type'] = X['type'].mode()[0]

s = X['sc_max_s']
quantiles = [s.min(), s.quantile(0.1), s.quantile(0.25), s.mean(), s.quantile(0.75), s.quantile(0.9), s.max()]

for i, q in enumerate(quantiles):
    pred['sc_max_s'] = q
    probs.loc[i, 'prob'] = fit.predict(pred)[0]

#Expertise
X = team[['n_authors', 'npp_sum_bcx', 'type', 'sc_max_e_bcx']]
X = sm.add_constant(X)
model = sm.Logit(team['top5_10_b'], X)
fit = model.fit()

pred = X.copy().head()
pred['n_authors'] = X['n_authors'].mean()
pred['npp_sum_bcx'] = X['npp_sum_bcx'].mean()
pred['type'] = X['type'].mode()[0]

e = X['sc_max_e_bcx']
quantiles = [e.min(), e.quantile(0.1), e.quantile(0.25), e.mean(), e.quantile(0.75), e.quantile(0.9), e.max()]

for i, q in enumerate(quantiles, start=7):
    pred['sc_max_e_bcx'] = q
    probs.loc[i, 'prob'] = fit.predict(pred)[0]

#To Excel
probs_table = probs.pivot(index='x', columns='perc', values='prob')
probs_table = probs_table.astype('float64').multiply(100).round(2)
probs_table = probs_table[['min', '10%', '25%', 'mean', '75%', '90%', 'max']].sort_index(ascending=False)

fnPd2xl(probs_table, 'probs')

del e, fit, i, model, pred, probs, probs_table, q, quantiles, s, t1, X

# *********************************** #
# %%% MNCS
# *********************************** #

#Function to make Tweedie regression
def fnTw (y, x, sheet):
  
  #Initials
  cols = x
  X = team[cols]
  X = sm.add_constant(X)
  y = team[y]
  
  #Estimate the power parameter by maximizing log-likehood
  ps = np.linspace(1.6, 1.99, 20)
  llhs = []
  
  for p in ps:
      model = sm.GLM(y, X, family=sm.families.Tweedie(var_power=p)).fit()
      llh = model.llf
      if np.isinf(llh): llh = -np.inf
      llhs.append(llh)  # Log-likelihood
  
  llh_df = pd.DataFrame({'p': ps, 'llh': llhs})
    
  p_hat = llh_df.loc[llh_df['llh'].idxmax(), 'p']
  
  #Estimate the model
  model = sm.GLM(y, X, family=sm.families.Tweedie(var_power=p_hat))  
  fit = model.fit()
  
  #Create table for excel
  table = fit.summary2().tables[1]
  table['llh'] = fit.llf
  table['p'] = p_hat
  fnPd2xl(table, sheet)
    
  return llh_df, model

zs = ['n_authors', 'npp_sum_bcx', 'type']

#Citation window: 10 years
llh_df1_10, tw1_10 = fnTw(y = 'ncs_10', x = 'sc_max_s', sheet = 'tw1')
llh_df2_10, tw2_10 = fnTw(y = 'ncs_10', x = 'sc_max_e_bcx', sheet = 'tw2')
llh_df3_10, tw3_10 = fnTw(y = 'ncs_10', x = zs, sheet = 'tw3')
llh_df4_10, tw4_10 = fnTw(y = 'ncs_10', x = zs + ['sc_max_s'], sheet = 'tw4')
llh_df5_10, tw5_10 = fnTw(y = 'ncs_10', x = zs + ['sc_max_e_bcx'], sheet = 'tw5')

#Citation window: 5 years
llh_df1_5, tw1_5 = fnTw(y = 'ncs_5', x = 'sc_max_s', sheet = 'tw1')
llh_df2_5, tw2_5 = fnTw(y = 'ncs_5', x = 'sc_max_e_bcx', sheet = 'tw2')
llh_df3_5, tw3_5 = fnTw(y = 'ncs_5', x = zs, sheet = 'tw3')
llh_df4_5, tw4_5 = fnTw(y = 'ncs_5', x = zs + ['sc_max_s'], sheet = 'tw4')
llh_df5_5, tw5_5 = fnTw(y = 'ncs_5', x = zs + ['sc_max_e_bcx'], sheet = 'tw5')

#Save the results
models = {f'tw{i}_{j}': globals()[f'tw{i}_{j}'] for i in range(1, 6) for j in (10, 5)}
llh_dfs = {f'llh_df{i}_{j}': globals()[f'llh_df{i}_{j}'] for i in range(1, 6) for j in (10, 5)}

with open('tweedie_results.pkl', 'wb') as f: pickle.dump({'models': models, 'llh_dfs': llh_dfs}, f)

#Stargazer
t1 = tw1_10.fit()
t2 = tw2_10.fit()
t3 = tw3_10.fit()
t4 = tw4_10.fit()
t5 = tw5_10.fit()
t6 = tw1_5.fit()
t7 = tw2_5.fit()
t8 = tw3_5.fit()
t9 = tw4_5.fit()
t10 = tw5_5.fit()

sg = Stargazer([t1, t2, t3, t4, t5, t6, t7, t8, t9, t10])

sg.rename_covariates({
  'sc_max_s': 'Specialisation',
  'sc_max_e_bcx': 'Expertise',
  'n_authors': '#Authors',
  'npp_sum_bcx': '#Previous publications',
  'type': 'Publication type',
  'const': 'Constant'
})
sg.covariate_order(['sc_max_s', 'sc_max_e_bcx', 'type', 'npp_sum_bcx', 'n_authors', 'const'])
sg.cov_spacing = 2

with open("tweedie2.html", "w") as f: f.write(sg.render_html())
