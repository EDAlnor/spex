# **************************************************************************** #
#******************************************************************************#
# 1. Intro                                                                  ####
#******************************************************************************#
# **************************************************************************** #

setwd('C:/Users/au544242/OneDrive - Aarhus universitet/Ph.d/Projekter/Topic switch/Code/testrun/parts')

library(arrow)
library(data.table)
library(digest)
library(dplyr)
library(httr)
library(igraph)
library(jsonlite)
library(lubridate)
library(openxlsx)
library(plotly)
library(stargazer)
library(stringr)
library(survival)
library(tidyr)
library(XML)

# ***************************************************************************  #
#******************************************************************************#
# 2. Prepare data                                                           ####
#******************************************************************************#
# **************************************************************************** #

# ********************************************* #
##  2.1 Unzip and combine                    ####
# ********************************************* #

# Here we process the data downloaded from the 'Author-ity' data webpage.

#Need 1st line to ensure 47 columns
con <- gzfile('authority2009.part00.gz', 'rt')
lines <- readLines(con)
close(con)
l <- lines[1]

data <- fread(
  text = lines, 
  sep = "\t", 
  header = F, 
  fill = T
) %>% slice(-n())

saveRDS(data, file = 'p00.rds')

data_clean <- data %>% 
  select(id = 4, pmid = 19) %>% 
  separate_rows(pmid, sep = '\\|') %>%
  mutate(
    aid = str_extract(id, '\\d+_\\d+$'),
    pmid_pos = str_extract(pmid, '(?<=_)(\\d+$)') %>% as.integer(),
    pmid = str_remove(pmid, '_\\d+$') %>% as.integer()
  )

saveRDS(data_clean, file = 'p00_ap.rds')

#Now do the rest
for (num in sprintf('%02d', 1:17)) {
  
  print(num)
  
  con <- gzfile(
    paste0('authority2009.part', num, '.gz'),
    'rt'
  )
  lines <- c(l, readLines(con)) #Ensure 47 columns
  close(con)
  
  data <- fread(
    text = lines, 
    sep = "\t", 
    header = F, 
    fill = T
  ) %>% .[2:nrow(.),]
  
  saveRDS(data, file = paste0('p', num, '.rds'))
  
  data_clean <- data %>% 
    select(aid = 4, pmid = 19) %>% 
    separate_rows(pmid, sep = '\\|') %>%
    mutate(
      aid = str_extract(aid, '\\d+_\\d+$'),
      pmid_pos = str_extract(pmid, '(?<=_)(\\d+$)') %>% as.integer(),
      pmid = str_remove(pmid, '_\\d+$') %>% as.integer()
    )
  
  saveRDS(data_clean, file = paste0('p', num, '_ap.rds'))
  
}

rm(data, data_clean, con, l, lines, num)

#Combine files
combined <- bind_rows(lapply(
  paste0('p', sprintf('%02d', 0:17), '_ap.rds'), 
  readRDS
))

author_key <- data.frame(org_id = unique(combined$aid)) %>% 
  mutate(quick_id = 1:nrow(.))

saveRDS(author_key, file = 'author_key.rds')

combined <- inner_join(combined, author_key, by = c('aid' = 'org_id')) %>% 
  select(aid = quick_id, pmid, pmid_pos)

saveRDS(combined, file = 'author_pmid.rds')

rm(author_key)

# ********************************************* #
##  2.2 Treshold for previous pubs           ####
# ********************************************* #

# Here we produce statistics to decide how many previous publications each
# author must have, in order to be included in the final dataset. As written in
# the paper, this number is decided to be 3.

#****************************
### 2.2.1. Stats         ####
#****************************

stats_df <- combined %>% add_count(aid)

n_pub_author <- rep(NA, 10)
n_pub <- rep(NA, 10)
n_author <- rep(NA, 10)

sapply(1:10, function(x) {
  t <- stats_df %>% filter(n >=x)
  n_pub_author[x] <<- nrow(t)
  n_pub[x] <<- length(unique(t$pmid))
  n_author[x] <<- length(unique(t$aid))
}) 

#Shows how many pmids we retain depending on treshold for number of previous publications
stats <- data.frame(
  treshold = 1:10,
  n_author,
  p_author = n_author/n_author[1],
  n_pub_author,
  p_pub_author = n_pub_author/n_pub_author[1],
  n_pub, 
  p_pub = n_pub/n_pub[1]
)  

write.xlsx(stats, file = 'treshold_stats.xlsx')

rm(combined, stats, stats_df, n_author, n_pub, n_pub_author)

#****************************
## 2.2.2 Year for PMIDS  ####
#****************************

progress <- function(iterator, sequence) {
  percentage <- (which(sequence == iterator)-1) / length(sequence) * 100
  timestamp <- format(Sys.time(), format = "%d-%b %H:%M")
  cat("Round:", which(sequence == iterator), "/", length(sequence),
      "   Progress:", round(percentage, 0), "%",
      "  ", timestamp, "\n")
}

pmids <- readRDS('author_pmid.rds') %>% distinct(pmid) %>% pull(pmid)

save(pmids, file = 'pmids.rda')
fwrite(data.table(pmids), 'pmids.tsv')

#Create request
selectors <- '&select=ids,publication_year'

results <- list()
min <- 1
max <- 51

for (i in 1:floor(length(pmids)/50)) {
  
  if (i %% 1000 == 0) progress(i, 1:floor(length(pmids)/50))
  
  url <- paste0(
    "https://api.openalex.org/works?filter=pmid:", 
    paste(pmids[min:max], collapse = '|'), 
    selectors,
    "&per-page=50",
    #insert API-key here
  )
  
  #Get results
  answer <- GET(url) %>%
    content(as = "text", encoding = "UTF-8") %>%
    fromJSON(flatten = TRUE) %>%
    `[[`("results")
  
  results[[i]] <- cbind(
    year = as.integer(answer$publication_year),
    pmid = as.integer(str_extract(answer$ids.pmid, '\\d+$'))
  )
  
  min <- min + 50
  max <- max + 50
  
}

pmids_years <- do.call(rbind, results)
saveRDS(pmids_years, 'pmids_years.rds')

rm(combined, pmids_years, stats, stats_df, n_author, n_pub, n_pub_author, progress)

#****************************
## 2.2.3  Tresholds      ####
#****************************

#Load and merge
pmids_years <- readRDS('pmids_years.rds')

py <- pmids_years %>% as.data.table() %>% unique(by = 'pmid') 
setkey(py, pmid)

ap <- readRDS('author_pmid.rds') %>% select(-pmid_pos) %>% as.data.table() 
setkey(ap, pmid)

dta <- merge(ap, py)
rm(ap, py, pmids_years)

### Variables
setkey(dta, aid, year)
setorder(dta, aid, year)

#For each author-pmid row, compute number of previous publications, that the other had at the time of that publication. Then we take the max of that within each year.
dta[, prev_pmid := seq_len(.N), by = .(aid)]
dta[, prev_pmid := max(prev_pmid), by = .(aid, year)]

#Number of authors pr pmid
setkey(dta, pmid)
dta[, n_authors := .N, by = pmid]

#For each pmid, calculate proportion of authors who have at least x previous publications
for (x in 1:7) {
  print(x)
  dta[, paste0('n', x) := sum(prev_pmid >= x), by = pmid]
  dta[, paste0('p', x) := get(paste0('n', x)) / n_authors]
  dta[, paste0('n', x) := NULL]
}

saveRDS(dta, file = 'proportions.rds')

n_pub <- integer(0)
n_author <- integer(0)
n_pub_author <- integer(0)
i = 1

result <- list()

#Determine how many pmids are retained as we vary 2 tresholds 1) number of previous publications and 2) proportion of authors meeting treshold 1
for (pa in c(0.5, 0.51, 0.61, 0.7, 0.75, 0.8, 0.9, 1)) {
  
  print(pa)
  
  for (np in 1:7) {
    
    col <- paste0('p', np)
    
    n_pub_author[np] <- dta[get(col) >= pa, .N]
    n_pub[np]        <- dta[get(col) >= pa, uniqueN(pmid)]
    n_author[np]     <- dta[get(col) >= pa, uniqueN(aid)]
    
  }
  
  result[[i]] <- data.frame(
    prop_auth = pa,
    treshold = 1:7,
    n_pub_author,
    p_pub_author = n_pub_author/n_pub_author[1],
    n_pub,
    p_pub = n_pub/n_pub[1],
    n_author,
    p_author = n_author/n_author[1]
  )
  
  i = i+1
  
}

treshold_stats_pubs <- do.call(rbind, result) %>% 
  filter(!(treshold == 1 & prop_auth != 0.5))

write.xlsx(treshold_stats_pubs, file = 'treshold_stats_pubs.xlsx')

# Visualize
cs <- treshold_stats_pubs %>% 
  select(prop_auth, treshold, p_pub) %>%
  mutate(
    prop_auth = factor(prop_auth, levels = unique(prop_auth)),
    treshold = treshold-1
  ) %>% 
  .[-1,]

plot_ly(
  data = cs, 
  x = ~treshold, 
  y = ~prop_auth, 
  z = ~p_pub, 
  type = "heatmap",
  text = ~p_pub, 
  texttemplate = "%{text:.2f}",
  showscale = F
) %>%
  layout(
    title = "Prop. publications remaining",
    xaxis = list(title = "Minimum previous publications", dtick = 1),
    yaxis = list(title = "Prop. authors")
  )

rm(cs, dta, result, treshold_stats_pubs, col, i, n_author, n_pub, n_pub_author,
   np, pa, x)

# ********************************************* #
## 2.3 Information content                   ####
# ********************************************* #

# Here we calculate the information content (IC) of each MeSH. To see how
#'tree.rda' is made visit https://github.com/EDAlnor/MeSH-relatedness

mesh <- read_feather('mesh_df.feather') %>% select(pmid, muid)

load("tree.rda")
tree <- tree %>% 
  mutate(muid = str_extract(muid, '\\d+$') %>% as.integer()) %>% 
  select(muid, desc)

mesh_freq <- mesh %>% 
  dplyr::count(muid) %>% 
  right_join(tree, by = 'muid')

desc_freq <- mesh_freq %>%
  filter(!is.na(desc)) %>%
  separate_rows(desc, sep = ';') %>%
  mutate(desc = str_extract(desc, '\\d+$') %>% as.integer()) %>% 
  left_join(mesh_freq, by = c('desc' = 'muid')) %>% 
  select(muid, n.y) %>%
  group_by(muid) %>% summarise(ndesc = sum(n.y, na.rm = T))

mesh_ic <- left_join(mesh_freq, desc_freq, by = 'muid') %>% 
  select(-desc) %>% 
  mutate(
    n     = ifelse(is.na(n), 0, n),
    ndesc = ifelse(is.na(ndesc), 0, ndesc),
    ntot  = n + ndesc,
    ic    = -log(ntot / sum(ntot)) %>% ifelse(is.infinite(.), NA, .)
  )

write_feather(mesh_ic, 'mesh_ic.feather')
rm(desc_freq, mesh_freq, tree, mesh, mesh_ic)

# ********************************************* #
## 2.4 Similarity matrix                     ####
# ********************************************* #

# Here we calculate the matrix 'S' used in soft cosine. To see how
# 'edgelist.rda' is made visit https://github.com/EDAlnor/MeSH-relatedness

mesh_ic <- read_feather('mesh_ic.feather') %>% select(muid, ic)

load('edgelist.rda')

el <- edgelist %>% 
  mutate(across(
    c(muid, chld, prnt), 
    ~ str_extract(.x, '\\d+$') %>% as.integer())
  ) %>% 
  left_join(mesh_ic, by = c('chld' = 'muid')) %>% #Add IC of children
  rename(cic = ic) %>%
  left_join(mesh_ic, by = 'muid') %>% #Add IC of parents
  filter(!is.na(cic) & !is.na(muid)) %>% 
  mutate(
    ic = ifelse(is.na(ic), cic, ic),
    delta_ic = abs(cic - ic)
  ) %>% 
  select(from = muid, to = chld, weight = delta_ic)

g <- graph_from_data_frame(el, directed = F)

dm <- distances(g, weight = E(g)$weight) 
sm <- exp(-dm)

saveRDS(dm, 'dm.rds')
rm(dm, edgelist, el, g, mesh_ic)

sm <- data.frame(sm)
write_feather(sm, 'sm.feather')
rm(sm)
# **************************************************************************** #
#******************************************************************************#
# 3. Analyse                                                                ####
#******************************************************************************#
# **************************************************************************** #

# Here we produce the logit regression models.

team <- read_feather('team11.feather') %>% 
  select(top5_10_b, sc_max_s, sc_max_e_bcx, npp_sum_bcx, type, n_authors, top5_5_b, top1_10_b, top1_5_b, ncs_5, ncs_10)

stars <- c(0.05, 0.01, 0.001)

# ********************************************* #
##  3.1 Logit                                ####
# ********************************************* #

fnL <- function(xs) {
  formula <- as.formula(paste(
    'top5_10_b ~', paste(xs, collapse = ' + ')
  ))
  glm(formula, data = team, family = binomial)
}

m1 <- fnL('sc_max_s')
m2 <- fnL('sc_max_e_bcx')
m3 <- fnL(c('type', 'npp_sum_bcx', 'n_authors'))
m4 <- fnL(c('sc_max_s', 'type', 'npp_sum_bcx', 'n_authors'))
m5 <- fnL(c('sc_max_e_bcx', 'type', 'npp_sum_bcx', 'n_authors'))

psr2s <- sapply(list(m1, m2, m3, m4, m5), \(m) 
                {round(1 - (m$deviance / m$null.deviance), 3)}
)

stargazer(
  m1, m2, m3, m4, m5,
  type = "html",
  out = "top_l.html",
  keep.stat = c('ll'),
  star.cutoffs = stars,
  covariate.labels = c("Specialisation", "Expertise", "Type", "#Previous publications", "#Authors"),
  column.labels = c('I', 'II', 'III', 'IV', 'V'),
  model.numbers = FALSE,
  add.lines = list(c("Pseudo R²", psr2s)),
  notes = paste0("n: ", nrow(team))
)

# ********************************************* #
## 3.2 Group FE                              ####
# ********************************************* #

full <- read_feather('team11_fe.feather') %>% 
  select(top5_10_b, gid, year, sc_max_s, sc_max_e_bcx, npp_sum_bcx, type, n_authors, top5_5_b, top1_10_b, top1_5_b)

fnCl <- function(predictors) {
  formula <- as.formula(paste(
    "top5_10_b ~", paste(predictors, collapse = " + "), "+ strata(gid)"
  ))
  clogit(formula, data = team_fe)
}

team_fe <- full %>% 
  group_by(gid) %>% 
  filter(n_distinct(top5_10_b) > 1) %>% 
  ungroup() %>% 
  arrange(gid)

#Group
m1_fe <- fnCl(c('sc_max_s'))
m2_fe <- fnCl(c('sc_max_e_bcx'))
m3_fe <- fnCl(c('sc_max_s', 'type', 'npp_sum_bcx'))
m4_fe <- fnCl(c('sc_max_e_bcx', 'type', 'npp_sum_bcx'))

stargazer(
  m1_fe, m2_fe, m3_fe, m4_fe, type = "html", out = "top_fe.html",
  keep.stat = c('ll', 'rsq'),
  star.cutoffs = stars,
  covariate.labels = c("Specialisation", "Expertise", "Type", "#Previous publications"),
  column.labels = c('I', 'II', 'III', 'IV'),
  model.numbers = F,
  notes = paste0('n: ', as.character(nrow(team)))
)

# ********************************************* #
## 3.3 Group + Year FE                       ####
# ********************************************* #

#Setup
team_year <- full  %>%
  group_by(gid, year) %>% 
  filter(n_distinct(top5_10_b) > 1) %>% 
  ungroup() %>% 
  arrange(gid)

fnCl_2fe <- function(predictors) {
  formula <- as.formula(paste(
    "top5_10_b ~", paste(predictors, collapse = " + "), "+ strata(gid, year)"
  ))
  clogit(formula, data = team_year)
}

#Models
m1_2fe <- fnCl_2fe(c('sc_max_s'))
m2_2fe <- fnCl_2fe(c('sc_max_e_bcx'))
m3_2fe <- fnCl_2fe(c('sc_max_s', 'type'))
m4_2fe <- fnCl_2fe(c('sc_max_e_bcx', 'type'))

stargazer(
  m1_2fe, m2_2fe, m3_2fe, m4_2fe, type = "html", out = "top_2fe.html",
  keep.stat = c('ll', 'rsq'),
  star.cutoffs = stars,
  covariate.labels = c("Specialisation", "Expertise", "Type"),
  column.labels = c('I', 'II', 'III', 'IV'),
  model.numbers = F,
  notes = paste0('n: ', as.character(nrow(team_year)))
)

# ********************************************* #
## 3.4 Full table                            ####
# ********************************************* #

events <- c(
  rep(sum(team$top5_10_b), 5),
  sapply(list(m3_fe, m4_fe, m3_2fe, m4_2fe), \(m) m$nevent)
)

stargazer(
  m1, m2, m3, m4, m5, m3_fe, m4_fe, m3_2fe, m4_2fe,
  type = "html", out = "top_full.html",
  keep.stat = c('ll', 'n'),
  star.cutoffs = stars,
  covariate.labels = c("Specialisation", "Expertise", 'Type', '#Previous publications', "#Authors"),
  column.labels = c('I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX'),
  model.numbers = F,
  add.lines = list(
    c('n top cited', events)
  )
)

#Export to Excel
html_table <- readHTMLTable("top_full.html", header = TRUE)
wb <- createWorkbook()
addWorksheet(wb, "Stargazer Results")
writeData(wb, "Stargazer Results", html_table)
saveWorkbook(wb, "top_full.xlsx", overwrite = TRUE)

rm(full, html_table, m1, m1_2fe, m1_fe, m2, m2_2fe, m2_fe, m3, m3_2fe, m3_fe, m4, m4_2fe, m4_fe, m5, team, team_fe, team_year, wb, fnCl, fnCl_2fe, fnL, events, psr2s)

# **************************************************************************** #
#******************************************************************************#
# 4. Analyse, robust                                                        ####
#******************************************************************************#
# **************************************************************************** #

# Here we make a series of robustness tests.

#Data
team <- read_feather('team11.feather') %>% 
  select(top5_10_b, sc_max_s, sc_max_e_bcx, npp_sum_bcx, type, n_authors, top5_5_b, top1_10_b, top1_5_b, ncs_5, ncs_10)

full <- read_feather('team11_fe.feather') %>% 
  select(top5_10_b, sc_max_s, sc_max_e_bcx, npp_sum_bcx, type, n_authors, top5_5_b, top1_10_b, top1_5_b, ncs_5, ncs_10, gid, year)

stars <- c(0.05, 0.01, 0.001)

#Functions
fnL_R <- function(xs, y) {
  formula <- as.formula(paste(
    paste0(y, ' ~'), paste(xs, collapse = ' + ')
  ))
  glm(formula, data = team, family = binomial)
}

fnCl_R <- function(xs, y) {
  formula <- as.formula(paste(
    paste0(y, ' ~'), paste(xs, collapse = " + "), "+ strata(gid)"
  ))
  clogit(formula, data = team_fe)
}

fnCl_2fe_R <- function(xs, y) {
  formula <- as.formula(paste(
    paste0(y, ' ~'), paste(xs, collapse = " + "), "+ strata(gid, year)"
  ))
  clogit(formula, data = team_year)
}

#Loop
for (y in c('top5_5_b', 'top1_10_b', 'top1_5_b')) {
  team_fe <- full %>% 
    group_by(gid) %>% 
    filter(n_distinct(!!sym(y)) > 1) %>% 
    ungroup() %>% 
    arrange(gid)
  
  team_year <- full %>%
    group_by(gid, year) %>% 
    filter(n_distinct(!!sym(y)) > 1) %>% 
    ungroup() %>% 
    arrange(gid)
  
  m1 <- fnL_R('sc_max_s', y)
  m2 <- fnL_R('sc_max_e_bcx', y)
  m3 <- fnL_R(c('type', 'npp_sum_bcx', 'n_authors'), y)
  m4 <- fnL_R(c('sc_max_s', 'type', 'npp_sum_bcx', 'n_authors'), y)
  m5 <- fnL_R(c('sc_max_e_bcx', 'type', 'npp_sum_bcx', 'n_authors'), y)
  m6 <- fnCl_R(c('sc_max_s', 'type', 'npp_sum_bcx'), y)
  m7 <- fnCl_R(c('sc_max_e_bcx', 'type', 'npp_sum_bcx'), y)
  m8 <- fnCl_2fe_R(c('sc_max_s', 'type'), y)
  m9 <- fnCl_2fe_R(c('sc_max_e_bcx', 'type'), y)
  
  events <- c(
    rep(sum(team[[y]]), 5),
    sapply(list(m6, m7, m8, m9), \(m) m$nevent)
  )
  
  stargazer(
    m1, m2, m3, m4, m5, m6, m7, m8, m9,
    type = "html", out = paste0('top_full_', y, '.html'),
    keep.stat = c('ll', 'n'),
    star.cutoffs = stars,
    covariate.labels = c("Specialisation", "Expertise", 'Type', '#Previous publications', "#Authors"),
    column.labels = c('I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX'),
    model.numbers = F,
    add.lines = list(
      c('n top cited', events)
    )
  )
}
