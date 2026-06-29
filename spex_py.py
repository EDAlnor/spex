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
import joblib
from scipy import stats
os.chdir(r'C:\Users\au544242\OneDrive - Aarhus universitet\Ph.d\Projekter\Spex\Code\parts') # Change to your preferred working directory

%xmode Minimal

# *********************************** #
# %%% Publications                    
# *********************************** #

# Prepare data on focal publications: Import and downcast cwts-wos file.

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
# %%% MeSH                    
# *********************************** #

# Prepare MeSH data

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

# If any mesh-qualifier is major, then mesh is major
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

# Weights
mesh = pd.read_feather('mesh_no-weights.feather')

mesh['w'] = np.where(mesh['mjr'], 3, 1)
mesh['sumw'] = mesh.groupby(['pmid'])['w'].transform('sum')
mesh['w_w'] = mesh['w'] / mesh['sumw']
mesh = mesh.merge(pd.read_feather('mesh_ic.feather')[['muid', 'ic']], on='muid')
mesh['w_ic'] = mesh['w_w'] * mesh['ic']
mesh.drop(columns = ['w', 'mjr', 'sumw', 'ic'], inplace = True)

feather.write_feather(mesh, 'mesh_df.feather')

# Save a file for MeSH of PMIDS after year 2000, since regression is only done
# on these publications
py = pd.read_csv('pmid_year.txt', sep = '\t')
py = py[py['pub_year'] >= 2000]['pmid']
mesh = (
  mesh
  .query('pmid in @py')
  .sort_values(by=['pmid', 'muid'])
)
feather.write_feather(mesh, 'mesh_df2000.feather')

# To compute PMIDS-author similarity, make files for each year, to speed up
# calculations.
pmids = pd.read_feather('pubs_clean.feather')['pmid']
py = (
  pd.read_csv('pmid_year.txt', sep = '\t')
  .query('pub_year > 1999 & pmid in @pmids')
)

for i in range(30, 40):
  print(i, pd.Timestamp.now().strftime("%H:%M:%S"))
  
  pmids_i = py[py['pub_year'] == (i+1970)]['pmid'] #Subset PMIDs of year 'i'
  mesh_i = mesh.query('pmid in @pmids_i') # Subset MeSH-terms of those PMIDS
  feather.write_feather(mesh_i, f'mesh_{i}.feather')
  
del mesh, descriptor_key, nm, nmjr, i, py

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
        .agg({"w_w": "sum"})
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

aym = pd.read_feather('aym_recieved.feather', columns=['aid', 'muid', 'year', 'w_w'])

authors = np.array_split(aym['aid'].unique(), 10)

for a in range(10):
  print(pd.Timestamp.now().strftime("%H:%M:%S"), a)
  aym_a = aym[aym['aid'].isin(authors[a])]
  feather.write_feather(aym_a, f'aym_authors{a}.feather')

del aym, aym_a, authors

# For the years 1999-2009 show cumulative amount of MeSH points recieved 
# in the year or preceeding years.

for a in range(10):
  
  aym_a = pd.read_feather(f'aym_authors{a}.feather')
  
  for y in range(29, 40):
    print(pd.Timestamp.now().strftime("%H:%M:%S"), 'a', a, 'y', y)
    aym_a_y = (
      aym_a
      .query('year <= @y')
      .groupby(['aid', 'muid'], as_index=False)
      ['w_w'].sum()
    )
    feather.write_feather(aym_a_y, f'aym_{a}_{y}.feather')
    del aym_a_y
    
  del aym_a

# Collect the authors chunks, so there is 1 file pr year

for y in range(29, 40):
    print(pd.Timestamp.now().strftime("%H:%M:%S"), y)

    files = [f'aym_{a}_{y}.feather' for a in range(10)]
    df_list = [pd.read_feather(file) for file in files]
    aym_y = pd.concat(df_list, ignore_index=True)

    feather.write_feather(aym_y, f'aym{y}.feather')
    del df_list, files, aym_y

del y, a

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
# %%% Prepare PMID-team-vectors                    
# *********************************** #

# We first create team-pmid-vectors. Then we load them and calculate
# specialisation and experience. Why 2 steps? Because the vector creation (1) 
# has many object in memory which is not needed for the calcation (2), so
# calculation is faster that way. 

mesh_ic = pd.read_feather('mesh_ic.feather')[['muid', 'ic']]

for year in range(30, 39):  

  apy = np.load(f'apy{year}.npy') #author-pmid
  aids = np.unique(apy[:, 0]) #Author-IDs
  
  my = year-1 #Take author publication record vectors of year before PMID year
  aym = (
    pd.read_feather(f'aym{my}.feather')
    .merge(mesh_ic, on='muid')
    .assign(w_ic=lambda d: d['w_w'] * d['ic'])
    .drop(columns=['w_w', 'ic'])
  )
  
  mesh = pd.read_feather(f'mesh_{year}.feather', columns=['muid', 'pmid', 'w_ic']) # MeSH-terms for PMIDs

  pmids_list = np.array_split(np.unique(apy[:, 1]), 10) #Split in 10 chunks to make memory manageable

  for i in range(10):

    print(year, i, pd.Timestamp.now().strftime("%H:%M:%S")) #Monitor progess

    pmids = pmids_list[i]

    apy_i = pd.DataFrame(
      apy[np.isin(apy[:, 1], pmids)],
      columns = ['aid', 'pmid']
    )

    aids_i = apy_i['aid'].unique()
    
    aym_i = aym.query('aid in @aids_i')
    
    mesh_i = mesh.query('pmid in @pmids')
    
    team = (
      pd.merge(apy_i, aym_i, on='aid')
      .groupby(['pmid', 'muid'], as_index=False)
      ['w_ic'].agg(['sum', 'mean', 'max']) # We test different aggregation rules
    )
    
    vecs = pd.merge(team, mesh_i, on=['pmid', 'muid'], how='outer').fillna(0)
    
    vecs_grouped = { # Create a dict for faster subsetting
      k: v.drop(columns='pmid')
      for k, v in vecs.groupby('pmid')
    }
    
    joblib.dump(vecs_grouped, f'pmid-team-vecs_{year}_{i}.joblib')
  
  del aids, aids_i, apy, apy_i, aym, aym_i, i, mesh, mesh_i, my, pmids, pmids_list, team, vecs, vecs_grouped, year #Free memory after each year iteration, since otherwise can't load aym.

del mesh_ic

# *********************************** #
# %%% Calculate specialisation and experience              
# *********************************** #

# Here we calculate specialisation and experience using the vectors created in
# the previous step and the formulas in the paper.

for year in range(30, 39): 
  
  results = []
  
  for i in range(10):
    print(year, i, len(results), pd.Timestamp.now().strftime("%H:%M:%S"))
    
    #PMID-team-vectors for current chunk
    vecs = joblib.load(f'pmid-team-vecs_{year}_{i}.joblib')
    
    # Only load similarities between terms which are in the current chunk
    muids = pd.concat(
      (df["muid"] for df in vecs.values()),
      ignore_index=True
      ).unique()
    
    sm = pd.read_feather('sm_df.feather').loc[muids, muids]
    
    # Calculate specialisation and experience for each PMID
    for p in vecs.keys():
      
      pmid = vecs[p]
      
      # Setup
      S = sm.loc[pmid['muid'], pmid['muid']].values
           
      a_sum = pmid['sum'].values # We test 3 definitions of the team vector
      a_mean = pmid['mean'].values
      a_max = pmid['max'].values
      
      b = pmid['w_ic'].values
           
      aSb = a_sum @ S @ b
      sqrt_aSa = np.sqrt(a_sum @ S @ a_sum)
      sqrt_bSb = np.sqrt(b @ S @ b)
      
      # Specialisation
      spec_sum = aSb / (sqrt_aSa * sqrt_bSb)
      
      spec_mean = (
        (a_mean @ S @ b) /
        (np.sqrt(a_mean @ S @ a_mean) * sqrt_bSb)
      )
      
      spec_max = (
        (a_max @ S @ b) /
        (np.sqrt(a_max @ S @ a_max) * sqrt_bSb)
      )
      
      # Experience
      expe_sum = aSb / sqrt_bSb      
      expe_mean = (a_mean @ S @ b) / sqrt_bSb
      expe_max = (a_max @ S @ b) / sqrt_bSb
      
      # End
      results.append([
        p, aSb, sqrt_aSa, sqrt_bSb, spec_sum, spec_mean, spec_max, expe_sum,
        expe_mean, expe_max]
      )
        
    del sm, vecs
  
  #Save after each year iteration
  results_df = pd.DataFrame(
    results,
    columns=['pmid', 'aSb', 'sqrt_aSa', 'sqrt_bSb', 'spec_sum', 'spec_mean', 'spec_max', 'expe_sum', 'expe_mean', 'expe_max']
  )
  
  results_df.to_feather(f'spex{year}.feather')
  
  del results, results_df

# Combine files
spex = pd.concat(
  [pd.read_feather(f'spex{year}.feather') for year in range(30, 39)],
  ignore_index=True
)
spex.to_feather('spex.feather')

del a_max, a_mean, a_sum, aSb, b, expe_max, expe_mean, expe_sum, i, p, pmid, S, spec_max, spec_mean, spec_sum, sqrt_aSa, sqrt_bSb, year, spex, muids, 

# *********************************** #
# %%% Process data                      
# *********************************** #

# Merge to get...
spex = (
  pd.read_feather('spex.feather')
  .merge(pd.read_feather('pubs_clean.feather'), on = 'pmid') # ... citation impact
  .merge(pd.read_feather('n_authors.feather'),  on = 'pmid') # ... number of authors
  .rename(columns={'n': 'n_authors'})
)

# ... number of previous publications of the author team
npp = ( 
  pd.read_feather('apy_t1.feather')
  .query('pmid in @spex.pmid')
  .groupby('pmid', as_index=False)
  ['npp'].sum()
)      

team = pd.merge(spex, npp, on = 'pmid')

# Dichotomize topX% indicators
np.random.seed(123)
for col in ['top1_5', 'top1_10', 'top5_5', 'top5_10']:
    team[f'{col}_b'] = np.random.binomial(1, team[col]).astype(bool)

# Log-transform expertise and number of previous publications
for col in ['expe_sum', 'expe_mean', 'expe_max', 'npp']:
    team[f'{col}_ln'] = np.log(team[col])

team.drop(columns=['sqrt_aSa', 'meso_cid', 'micro_cid', 'cs_5', 'cs_10', 'npp'], inplace=True)

# Convert type to dummy
team['type'] = np.where(team['type'] == 2, 1, 0)

# Save all data + publications with 11 authors or less.
feather.write_feather(team, 'team.feather')

team11 = team[team['n_authors'] <= 11]
feather.write_feather(team11, 'team11.feather')

#Average number of MeSH + major terms
mesh = pd.read_feather('mesh_no-weights.feather')[['pmid', 'mjr']]
mesh = mesh[mesh['pmid'].isin(team11['pmid'])]
nm = mesh.groupby('pmid').size().mean().round(2)
nmjr = mesh.query('mjr').groupby('pmid').size().mean().round(2)

del spex, npp, col, team11, mesh, nm, nmjr

# Final step is to produce a dataset that 1) includes a group-identifier 
# column, 2) only has pmids where the group has at least 2 publications.

# Add group-identifier
ap = (
  pd.read_feather('apy_npp3_2000.feather', columns=['aid', 'pmid'])
  .query('pmid in @team.pmid')
  .sort_values(['pmid', 'aid'])
  .assign(aid=lambda d: d['aid'].astype(str))
)

sig = ap.groupby('pmid')['aid'].agg(','.join)

groups = pd.DataFrame({
  'pmid': sig.index,
  'gid': pd.factorize(sig)[0]}
)

# Remove groups with less than 2 publications
groups['gid_n'] = groups['gid'].map(groups['gid'].value_counts())
groups = groups[groups['gid_n'] > 1].drop(columns='gid_n')

feather.write_feather(groups, 'groups.feather')

# Merge onto full data
team11_fe = pd.merge(groups, team, on = 'pmid')

feather.write_feather(team11_fe, 'team11_fe.feather')

del groups, team11_fe, team, ap, sig

# *************************************************************************** #
# *************************************************************************** #
# %% Analyze                                                             
# *************************************************************************** #
# *************************************************************************** #

# In this section we make the descriptive tables and figures. The regression
# models are made in R.

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
cols = ['spec_sum', 'expe_sum_ln', 'top5_10_b', 'n_authors', 'npp_ln', 'type']
table = (
  team2[cols]
  .describe(percentiles=[0.1, 0.5, 0.9])
  .round(3)
)
for col in ['expe_sum_ln', 'n_authors', 'npp_ln']: table[col] = table[col].round(2)

table = table.T.drop(columns='count')

table.index = ['Specialisation', 'ln(Expertise)', 'Top5%', '#Authors', 'ln(#Previous publications)', 'Publication type']

table.to_excel('../results/stats.xlsx')

# Comparing means
means = team2.groupby('top5_10_b')[['spec_sum', 'expe_sum_ln']].mean().round(3)
means['expe_sum_ln'] = means['expe_sum_ln'].round(2)
means.to_excel('../results/means.xlsx')
    
# Density plots
import matplotlib as mpl
mpl.rcParams['font.family'] = 'Times New Roman'
mpl.rcParams['font.size'] = 10

fig, ax = plt.subplot_mosaic(
  [['left', 'mid', 'right',]],
  constrained_layout=True,
  figsize=(6.5, 2)
)

sns.kdeplot(team2['spec_sum'], color='black', linewidth=0.8, ax=ax['left'])
ax['left'].set_xlabel('Specialisation')

sns.kdeplot(team2['expe_sum_ln'], color='black', linewidth=0.8, ax=ax['mid'])
ax['mid'].set_xlabel('ln(Experience)')

p99 = team2['expe_sum'].quantile(0.99)
sns.kdeplot(team2.loc[team2['expe_sum'] <= p99, 'expe_sum'], color='black', linewidth=0.8, ax=ax['right'])
ax['right'].set_xlabel('Experience, 0-99th percentile')

for a in ['left', 'mid', 'right']: ax[a].set_ylabel('')

plt.tight_layout()

# Number of publications with a tie citation score

frac = ((team2['top5_10'] > 0) & (team2['top5_10'] < 1)).sum()
frac
round((frac / len(team2)) * 100, 2)

del cols, frac, means, table, p99, a, ax

# *********************************** #
# %%% Robustness: 10,000 Bernoulli trials
# *********************************** #

bt = pd.read_feather('bt10000_results.feather')
for col in ['prop_all', 'prop_fe']: bt[col] = bt[col] * 100

# %%%% Plots

nbins = 70

fig, ax = plt.subplot_mosaic(
  [['topL', 'topM', 'topR',],
   ['botL', 'botM', 'botR',]],
  constrained_layout=True,
  figsize=(10, 6)
)
sns.histplot(bt['prop_all'], ax=ax['topL'], bins = nbins)
sns.histplot(bt['s_coef'], ax=ax['topM'], bins = nbins)
sns.histplot(bt['e_coef'], ax=ax['topR'], bins = nbins)
sns.histplot(bt['prop_fe'], ax=ax['botL'], bins = nbins)
sns.histplot(bt['s_coef_fe'], ax=ax['botM'], bins = nbins)
sns.histplot(bt['e_coef_fe'], ax=ax['botR'], bins = nbins)

for p in ['topM', 'topR', 'botM', 'botR']:
  ax[p].set_ylabel('')
  ax[p].set_yticks([])
ax['topL'].set_ylabel('Full sample')
ax['botL'].set_ylabel('Team and year fixed effects')

ax['topL'].set_xlabel("% top5%")
ax['topM'].set_xlabel("Specialisation coefficient")
ax['topR'].set_xlabel("Experience coefficient")
ax['botL'].set_xlabel("% top5%")
ax['botM'].set_xlabel("Specialisation coefficient")
ax['botR'].set_xlabel("Experience coefficient")

plt.show()

# %%%% Table
table = (
  bt
  .describe()
  [['prop_all', 's_coef', 'e_coef', 'prop_fe', 's_coef_fe', 'e_coef_fe']]
  .T
  [['std', 'min', 'mean', 'max']]
  .rename(columns={
    'std': 'Standard deviation',
    'min': 'Min.',
    'mean': 'Mean',
    'max': 'Max' 
  })
  .round(5)
)
table.index = ['% top 5%', 'Specialisation coefficient', 'Experience coefficient'] * 2
table.to_excel('../results/bt10000.xlsx')

# %%%% Numbers reported in the appendix

# Difference in odds between +/- 1SD

# Specialisation
p1sd_s = team2['spec_sum'].mean() + team2['spec_sum'].std()
m1sd_s = team2['spec_sum'].mean() - team2['spec_sum'].std()
p1sd_s.round(3)
m1sd_s.round(3)

coef_s_min = bt['s_coef_fe'].min()
round(coef_s_min, 2)
or_s = np.exp(coef_s_min*(p1sd_s-m1sd_s))
round(or_s, 2)

# Experience
p1sd_e = team2['expe_sum_ln'].mean() + team2['expe_sum_ln'].std()
m1sd_e = team2['expe_sum_ln'].mean() - team2['expe_sum_ln'].std()
p1sd_e.round(2)
m1sd_e.round(2)

coef_e_min = bt['e_coef_fe'].min()
round(coef_e_min, 2)
or_e = np.exp(coef_e_min*(p1sd_e-m1sd_e))
round(or_e, 2) - 1

# Ratio
coef_s_max = bt['s_coef_fe'].max()

round(coef_s_max, 2)
or_s_max = np.exp(coef_s_max*(p1sd_s-m1sd_s))
round(or_s_max, 2)

round((or_e-1)/(or_s_max-1), 2)

94/55
