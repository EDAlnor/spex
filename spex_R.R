# **************************************************************************** #
#******************************************************************************#
# 1. Intro                                                                  ####
#******************************************************************************#
# **************************************************************************** #

setwd('C:/Users/au544242/OneDrive - Aarhus universitet/Ph.d/Projekter/Spex/Code/parts') # Change to your preferred working directory

library(arrow)
library(data.table)
library(dplyr)
library(future.apply)
library(ggplot2)
library(igraph)
library(magrittr)
library(openxlsx)
library(scales)
library(stargazer)
library(stringr)
library(survival)
library(tidyr)

# ***************************************************************************  #
#******************************************************************************#
# 2. Prepare data                                                           ####
#******************************************************************************#
# **************************************************************************** #

# ********************************************* #
##  2.1. Unzip and combine                    ####
# ********************************************* #

# Here we process the data downloaded from the 'Author-ity' data webpage.

con <- gzfile('authority2009.part00.gz', 'rt')
lines <- readLines(con)
close(con)
l <- lines[1]

data <- fread(
  text = lines, 
  sep = "\t", 
  header = F, 
  fill = T
) |> slice(-n())

saveRDS(data, file = 'p00.rds')

data_clean <- data |> 
  select(id = 4, pmid = 19) |> 
  separate_rows(pmid, sep = '\\|') |>
  mutate(
    aid = str_extract(id, '\\d+_\\d+$'),
    pmid_pos = str_extract(pmid, '(?<=_)(\\d+$)') |> as.integer(),
    pmid = str_remove(pmid, '_\\d+$') |> as.integer()
  )

saveRDS(data_clean, file = 'p00_ap.rds')

for (num in sprintf('%02d', 1:17)) {
  
  print(num)
  
  con <- gzfile(
    paste0('authority2009.part', num, '.gz'),
    'rt'
  )
  lines <- c(l, readLines(con))
  close(con)
  
  data <- fread(
    text = lines, 
    sep = "\t", 
    header = F, 
    fill = T
  ) |> .[2:nrow(.),]
  
  saveRDS(data, file = paste0('p', num, '.rds'))
  
  data_clean <- data |> 
    select(aid = 4, pmid = 19) |> 
    separate_rows(pmid, sep = '\\|') |>
    mutate(
      aid = str_extract(aid, '\\d+_\\d+$'),
      pmid_pos = str_extract(pmid, '(?<=_)(\\d+$)') |> as.integer(),
      pmid = str_remove(pmid, '_\\d+$') |> as.integer()
    )
  
  saveRDS(data_clean, file = paste0('p', num, '_ap.rds'))
  
}

rm(data, data_clean, con, l, lines, num)

#Combine files
combined <- bind_rows(lapply(
  paste0('p', sprintf('%02d', 0:17), '_ap.rds'), 
  readRDS
))

author_key <- data.frame(org_id = unique(combined$aid)) |> 
  mutate(quick_id = 1:nrow(.))

saveRDS(author_key, file = 'author_key.rds')

combined <- inner_join(combined, author_key, by = c('aid' = 'org_id')) |> 
  select(aid = quick_id, pmid, pmid_pos)

saveRDS(combined, file = 'author_pmid.rds')

rm(author_key)

# ********************************************* #
## 2.3. Information content                   ####
# ********************************************* #

# Here we calculate the information content (IC) of each MeSH. To see how
#'tree.rda' is made visit https://github.com/EDAlnor/MeSH-relatedness

mesh <- read_feather('mesh_df.feather') |> select(pmid, muid)

load("tree.rda")
tree <- tree |> 
  mutate(muid = str_extract(muid, '\\d+$') |> as.integer()) |> 
  select(muid, desc)

mesh_freq <- mesh |> 
  dplyr::count(muid) |> 
  right_join(tree, by = 'muid')

desc_freq <- mesh_freq |>
  filter(!is.na(desc)) |>
  separate_rows(desc, sep = ';') |>
  mutate(desc = str_extract(desc, '\\d+$') |> as.integer()) |> 
  left_join(mesh_freq, by = c('desc' = 'muid')) |> 
  select(muid, n.y) |>
  group_by(muid) |> summarise(ndesc = sum(n.y, na.rm = T))

mesh_ic <- left_join(mesh_freq, desc_freq, by = 'muid') |> 
  select(-desc) |> 
  mutate(
    n     = ifelse(is.na(n), 0, n),
    ndesc = ifelse(is.na(ndesc), 0, ndesc),
    ntot  = n + ndesc,
    ic    = -log(ntot / sum(ntot)) |> ifelse(is.infinite(.), NA, .)
  )

write_feather(mesh_ic, 'mesh_ic.feather')
rm(desc_freq, mesh_freq, tree, mesh, mesh_ic)

# ********************************************* #
## 2.4. Similarity matrix                     ####
# ********************************************* #

# Here we calculate the matrix 'S' used in soft cosine. To see how
# 'edgelist.rda' is made visit https://github.com/EDAlnor/MeSH-relatedness

mesh_ic <- read_feather('mesh_ic.feather') |> select(muid, ic)

load('edgelist.rda')

el <- edgelist |> 
  mutate(across(
    c(muid, chld, prnt), 
    ~ str_extract(.x, '\\d+$') |> as.integer())
  ) |> 
  left_join(mesh_ic, by = c('chld' = 'muid')) |> #Add IC of children
  rename(cic = ic) |>
  left_join(mesh_ic, by = 'muid') |> #Add IC of parents
  filter(!is.na(cic) & !is.na(muid)) |> 
  mutate(
    ic = ifelse(is.na(ic), cic, ic),
    delta_ic = abs(cic - ic)
  ) |> 
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

# After the data preperation is done in Python (e.g., computing specialisation
# and experience) we now do the analysis here.

# **************************************************************************** #
##  3.1. Setup                                                              ####
# **************************************************************************** #

# Data
vars <- c(
  "top5_10_b", "spec_sum", 'expe_sum_ln', # X and Y
  'npp_ln', "type", "n_authors", 'year', # Z
  'spec_mean', 'spec_max', 'expe_mean_ln', 'expe_max_ln' # Alternative team vector
)

team <- read_feather('team11.feather') |> 
  select(all_of(vars)) |> 
  filter(n_authors > 1)

fe <- read_feather('team11_fe.feather') |> 
  select(all_of(vars), gid) |> 
  filter(n_authors > 1) 

team_gfe <- fe |> 
  group_by(gid) |> 
  filter(n_distinct(top5_10_b) > 1) |> 
  ungroup() |> 
  arrange(gid)

team_gyfe <- fe |> 
  group_by(gid, year) |> 
  filter(n_distinct(top5_10_b) > 1) |> 
  ungroup() |> 
  arrange(gid)

# Functions to make models concisely
fnL <- function(xs) {
  formula <- as.formula(paste(
    'top5_10_b ~', paste(xs, collapse = ' + ')
  ))
  glm(formula, data = team, family = binomial)
}

fnCl <- function(predictors) {
  formula <- as.formula(paste(
    "top5_10_b ~", paste(predictors, collapse = " + "), "+ strata(gid)"
  ))
  clogit(formula, data = team_gfe)
}

fnCl_2fe <- function(predictors) {
  formula <- as.formula(paste(
    "top5_10_b ~", paste(predictors, collapse = " + "), "+ strata(gid, year)"
  ))
  clogit(formula, data = team_gyfe)
}

# **************************************************************************** #
##  3.2. Table                                                              ####
# **************************************************************************** #

# Make the models
m1 <- fnL('spec_sum')
m2 <- fnL('expe_sum_ln')
m3 <- fnL(c('type', 'npp_ln', 'n_authors'))
m4 <- fnL(c('spec_sum', 'type', 'npp_ln', 'n_authors'))
m5 <- fnL(c('expe_sum_ln', 'type', 'npp_ln', 'n_authors'))
m6 <- fnCl(c('spec_sum', 'type', 'npp_ln'))
m7 <- fnCl(c('expe_sum_ln', 'type', 'npp_ln'))
m8 <- fnCl_2fe(c('spec_sum', 'type'))
m9 <- fnCl_2fe(c('expe_sum_ln', 'type'))

# Compute number of top5% publications for conditional logits
events <- c(
  rep(sum(team$top5_10_b), 5),
  sapply(list(m6, m7, m8, m9), \(m) m$nevent)
)

# Compute Pseudo R^2
ps_r_sqrd <- sapply(
  list(m1, m2, m3, m4, m5), \(m) 
  {round(1 - (m$deviance / m$null.deviance), 3)}
)

# Make the table
stargazer(
  m1, m2, m3, m4, m5, m6, m7, m8, m9,
  type = "html", out = "../results/main.html",
  keep.stat = c('ll', 'n'),
  star.cutoffs = NA,
  covariate.labels = c("Specialisation", "ln(Experience)", 'Type', 'ln(#Previous publications)', "#Authors"),
  column.labels = c('I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX'),
  model.numbers = F,
  add.lines = list(c('n top cited', events)),
  single.row = T
)


rm(fe, events, ps_r_sqrd, m1, m2, m3, m6, m7, team_gfe)

# **************************************************************************** #
##  3.3. Interpretation                                                     ####
# **************************************************************************** #

### Predicted probabilities *********************************************** ####

# Experience
mean_e <- mean(team$expe_sum_ln)
sd_e <- sd(team$expe_sum_ln)
sum_e <- c(
  quantile(team$expe_sum_ln, 0.1),
  mean_e - sd_e,
  mean_e,
  mean_e + sd_e,
  quantile(team$expe_sum_ln, 0.9)
)

newdata_e <- data.frame(
  expe_sum_ln = sum_e,
  type = 0,
  n_authors = mean(team$n_authors),
  npp_ln = mean(team$npp_ln)
)
prob_e <- predict(m5, newdata = newdata_e, type = 'response')

#Specialisation
mean_s <- mean(team$spec_sum)
sd_s <- sd(team$spec_sum)
sum_s <- c(
  quantile(team$spec_sum, 0.1),
  mean_s - sd_s,
  mean_s,
  mean_s + sd_s,
  quantile(team$spec_sum, 0.9)
)

newdata_s <- data.frame(
  spec_sum = sum_s,
  type = 0,
  n_authors = mean(team$n_authors),
  npp_ln = mean(team$npp_ln)
)
prob_s <- predict(m4, newdata = newdata_s, type = 'response')

# Table
pred <- cbind(prob_s, prob_e) |> t() |> as.data.frame() |> 
  mutate(across(everything(), ~ paste0(round(.x * 100, 2), "%")))

write.xlsx(pred, '../results/predicted_probabilities.xlsx')

# Figure

vars = c("Specialisation", "Experience")

levels <- c("10%", "-1 SD", "Mean", "+1 SD", "90%")

pred_long <- data.frame(
  level = rep(levels, 2) |> factor(levels = levels),
  var = rep(vars, each = 5) |> factor(levels = vars),
  prob = c(prob_s, prob_e) |> multiply_by(100) |> round(2)
)

ggplot(pred_long, aes(x = var, y = prob, fill = level)) +
  geom_col(position = position_dodge(width = 0.9), width = 0.6) +
  geom_text(
    aes(label = paste0(prob, "%")), 
    position = position_dodge(width = 0.9), 
    vjust = -0.5, size = 3
  ) +
  scale_fill_viridis_d(direction = -1) +
  scale_y_continuous(
    breaks = c(0, 2.5, 5, 7.5),
    labels = scales::percent_format(scale = 1)
  ) +
  labs(
    x = NULL,
    y = NULL,
    fill = NULL
  ) +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    panel.grid.minor.y = element_blank(),
    panel.grid.major.x = element_blank()
  )

ggsave('../results/pred_probs.png', width = 1300, height = 1100, units = "px", dpi = 300)

# Comments for paper

c( # Percentage points difference in predicted probability
  spec = prob_s['4'] - prob_s['2'],
  expe = prob_e['4'] - prob_e['2']
  ) |> multiply_by(100) |> round(1) 

dif_p <- c( # Percent difference in predicted probability  
  spec = (prob_s['4'] / prob_s['2']) - 1,
  expe = (prob_e['4'] / prob_e['2']) - 1
  ) |> multiply_by(100) |> round(1) 
dif_p
dif_p[2] / dif_p[1]


### Differences in odds *************************************************** ####

# Odds ratio

scoef <- m8$coefficients['spec_sum']
ecoef <- m9$coefficients['expe_sum_ln']

dif_odds_s <- c(
  exp(scoef*(sum_s[4] - sum_s[3])), # Mean --> +1SD
  exp(scoef*(sum_s[5] - sum_s[3])), # Mean --> 90%
  exp(scoef*(sum_s[4] - sum_s[2])), # -1SD --> +1SD
  exp(scoef*(sum_s[5] - sum_s[1])) # -1SD --> +1SD
)

dif_odds_e <- c(
  exp(ecoef*(sum_e[4] - sum_e[3])), 
  exp(ecoef*(sum_e[5] - sum_e[3])), 
  exp(ecoef*(sum_e[4] - sum_e[2])), 
  exp(ecoef*(sum_e[5] - sum_e[1]))
)

# Comments

scoef |> round(3)
(mean_s - sd_s) |> round(3)
(mean_s + sd_s) |> round(3)
exp(scoef*((mean_s + sd_s) - (mean_s - sd_s)))

ecoef |> round(3)
(mean_e - sd_e) |> round(2)
(mean_e + sd_e) |> round(2)
exp(ecoef*((mean_e + sd_e) - (mean_e - sd_e)))


# Table
odds <- cbind(dif_odds_s, dif_odds_e) |> t() |> round(3)
write.xlsx(odds, '../results/odds_ratio.xlsx')

# Figure
comp <- c("Mean vs.\n+1SD", "Mean vs.\n90%", "-1SD vs.\n+1SD", "10% vs.\n90%")

odds_long <- data.frame(
  comp = rep(comp, 2) |> factor(levels = comp),
  var = rep(vars, each = 4) |> factor(levels = vars),
  odds_ratio = c(dif_odds_s, dif_odds_e)
) 

ggplot(odds_long, aes(x = comp, y = odds_ratio, fill = var)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.6) +
  geom_text(aes(label = round(odds_ratio, 3)), 
            position = position_dodge(width = 0.8), 
            vjust = -0.5, size = 3) +
  scale_fill_manual(values = c("#FDE725FF", "#3B528B")) +
  scale_y_continuous(limits = c(1, NA), oob = scales::squish) +
  labs(
    x = NULL,
    y = NULL,
    fill = NULL
  ) +
  theme_minimal() +
  coord_cartesian(clip = "off") +
  theme(
    legend.position = "bottom",
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank()
  )

ggsave('../results/odds_ratio.png', width = 3.5, height = 2.8, units = "in", dpi = 600)

rm(m4, m5, m8, m9, newdata_e, newdata_s, pred, dif_odds_e, dif_odds_s, dif_p, ecoef, prob_e, prob_s, scoef, sum_e, sum_s, odds, odds_long, pred_long, comp, levels, mean_e, mean_s, sd_e, sd_s, vars)

# **************************************************************************** #
#******************************************************************************#
# 4. Analyse, robust                                                        ####
#******************************************************************************#
# **************************************************************************** #

# This section does various robustness analyses.

# ********************************************* #
##  4.1. Alternative defintions of TPRV      ####
# ********************************************* #

# Here we analyse if the results are robust to defining the team publication 
# record vector by the max and mean of the team members.

for (def in c('max', 'mean')) { cat(def, format(Sys.time(), "%H:%M:%S"))
  
  spec <- paste0('spec_', def)
  expe <- paste0('expe_', def, '_ln')
  
  m1 <- fnL(spec)
  m2 <- fnL(expe)
  m3 <- fnL(c('type', 'npp_ln', 'n_authors'))
  m4 <- fnL(c(spec, 'type', 'npp_ln', 'n_authors'))
  m5 <- fnL(c(expe, 'type', 'npp_ln', 'n_authors'))
  m6 <- fnCl(c(spec, 'type', 'npp_ln'))
  m7 <- fnCl(c(expe, 'type', 'npp_ln'))
  m8 <- fnCl_2fe(c(spec, 'type'))
  m9 <- fnCl_2fe(c(expe, 'type'))
  
  events <- c(
    rep(sum(team$top5_10_b), 5),
    sapply(list(m6, m7, m8, m9), \(m) m$nevent)
  )
  
  # Table
  stargazer(
    m1, m2, m3, m4, m5, m6, m7, m8, m9,
    type = "html", out = paste0('../results/reg_', def, '.html'),
    keep.stat = c('ll', 'n'),
    star.cutoffs = NA,
    covariate.labels = c("Specialisation", "ln(Experience)", 'Type', '#Previous publications', "#Authors"),
    column.labels = c('I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX'),
    model.numbers = F,
    add.lines = list(
      c('n top cited', events)
    ),
    single.row = T
  )
}

rm(spec, expe)

# ********************************************* #
##  4.2. Including singles and min 3 authors ####
# ********************************************* #

# Here we analyse if the results hold when excluding single-authored papers and 
# when defining teams as minimum 3 persons.

for (n in c(1, 3)) { cat(n, format(Sys.time(), "%H:%M:%S"))
  
  # Prepare the data
  team <- read_feather('team11.feather') |> 
    select(all_of(vars)) |> 
    filter(n_authors >= n)
  
  fe <- read_feather('team11_fe.feather') |> 
    select(all_of(vars), gid) |> 
    filter(n_authors >= n)
  
  team_gfe <- fe |> 
    group_by(gid) |> 
    filter(n_distinct(top5_10_b) > 1) |> 
    ungroup() |> 
    arrange(gid)
  
  team_gyfe <- fe |> 
    group_by(gid, year) |> 
    filter(n_distinct(top5_10_b) > 1) |> 
    ungroup() |> 
    arrange(gid)
  
  
  # Make the models
  m1 <- fnL('spec_sum')
  m2 <- fnL('expe_sum_ln')
  m3 <- fnL(c('type', 'npp_ln', 'n_authors'))
  m4 <- fnL(c('spec_sum', 'type', 'npp_ln', 'n_authors'))
  m5 <- fnL(c('expe_sum_ln', 'type', 'npp_ln', 'n_authors'))
  m6 <- fnCl(c('spec_sum', 'type', 'npp_ln'))
  m7 <- fnCl(c('expe_sum_ln', 'type', 'npp_ln'))
  m8 <- fnCl_2fe(c('spec_sum', 'type'))
  m9 <- fnCl_2fe(c('expe_sum_ln', 'type'))
  
  # Compute number of top5% publications
  events <- c(
    rep(sum(team$top5_10_b), 5),
    sapply(list(m6, m7, m8, m9), \(m) m$nevent)
  )
  
  # Table
  stargazer(
    m1, m2, m3, m4, m5, m6, m7, m8, m9,
    type = "html", out = paste0('../results/reg_min', n, '.html'),
    keep.stat = c('ll', 'n'),
    star.cutoffs = NA,
    covariate.labels = c("Specialisation", "ln(Experience)", 'Type', '#Previous publications', "#Authors"),
    column.labels = c('I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX'),
    model.numbers = F,
    add.lines = list(
      c('n top cited', events)
    ),
    single.row = T
  )
}

rm(team, fe, n, team_gfe, team_gyfe, m1, m2, m3, m4, m5, m6, m7, m8, m9, events)

# ********************************************* #
##  4.3. 10,000 draws of top 5%               ####
# ********************************************* #

# Here we analyze if the specific realisation of the Bernoulli trial used to
# dichotomize the top 5% indicator has any substantial influence on the results.
# This is done by analysing the distribution of top 5% proportions and
# coefficients estimates obtained by repeating the Bernoulli trial 10,000 times.

# We parallelise the simulations, so we use a fresh R session, to make sure each worker only have the packages they need in memory.

#.rs.restartR() # Run this

setwd('C:/Users/au544242/OneDrive - Aarhus universitet/Ph.d/Projekter/Spex/Code/parts')
library(arrow)
library(survival)
library(dplyr)
library(fastglm)
library(future.apply)
plan(multisession(workers = 6))

# Full sample (logit) ***************************
main_data <- read_feather('team11.feather') |> 
  select(top5_10, spec_sum, expe_sum_ln, n_authors, npp_ln, type) |> 
  filter(n_authors > 1) |> 
  mutate(not_fractional = top5_10 %in% c(0, 1)) |> 
  arrange(not_fractional) # This ensure that the fractional top5% publications are first

n_random = sum(!main_data$top5_10 %in% c(0,1)) # Number of publications affected
n_random
round((n_random / nrow(main_data))*100, 1)# % publications affected

# Y
y_fixed <- main_data |> filter(not_fractional) |> pull(top5_10) |> as.integer()
probs <- main_data |> filter(!not_fractional) |> pull(top5_10)

# X
X_s <- model.matrix(~ spec_sum + type + npp_ln + n_authors, data = main_data)
X_e <- model.matrix(~ expe_sum_ln + type + npp_ln + n_authors, data = main_data)

n_sims <- 10000

# To speed up the logits we use warm starting of coefficient estimation inside the loop
y_init <- c(rbinom(n=n_random, size=1L, p=probs), y_fixed)

fit_s_init <- fastglm(X_s, y_init, family = binomial())
coefs_s_init <- fit_s_init$coefficients

fit_e_init <- fastglm(X_e, y_init, family = binomial())
coefs_e_init <- fit_e_init$coefficients

# Fixed effects (conditional logit) *************
main_data_fe <- read_feather('team11_fe.feather') |> 
  select(
    top5_10, spec_sum, expe_sum_ln, n_authors, npp_ln, type, gid, year
  ) |> 
  filter(n_authors > 1) |> 
  mutate(not_top5 = top5_10 == 0) |> 
  group_by(gid, year) |> 
  filter(n_distinct(not_top5) > 1) |> 
  ungroup() |> 
  select(-not_top5, -n_authors)

nrow_fe <- nrow(main_data_fe)
probs_fe <- main_data_fe$top5_10

# Prepare the loop
rm(y_init, fit_s_init, fit_e_init, main_data)
detach("package:arrow", unload = TRUE)

results <- future_lapply(1:n_sims, \(i) {
  # In the largest sample, we speed things up by only making the Bernoulli trial
  # on the fractional top 5% publications. For simplicity, we don't do that in
  # the smaller FE sample
  
  y = c(
    rbinom(n = n_random, size = 1L, p = probs),
    y_fixed
  )
  
  # Logit
  fit_s <- fastglm(X_s, y, family = binomial(), start = coefs_s_init)
  fit_e <- fastglm(X_e, y, family = binomial(), start = coefs_e_init)
  
  # Conditional logit (fixed effects).
  # Start by creating a copy of the FE df for the worker.
  worker_fe <- main_data_fe |> 
    mutate(y = rbinom(n = nrow_fe, size = 1L, p = probs_fe))
  
  fit_s_fe <- clogit(
    y ~ spec_sum + type + strata(gid, year),
    data = worker_fe
  )
  fit_e_fe <- clogit(
    y ~ expe_sum_ln + type + strata(gid, year),
    data = worker_fe
  )
  
  # Return results
  list(
    prop_all  = mean(y),
    prop_fe   = mean(worker_fe$y),
    s_coef    = fit_s$coefficients[['spec_sum']],
    e_coef    = fit_e$coefficients[['expe_sum_ln']],
    s_coef_fe = fit_s_fe$coefficients[['spec_sum']],
    e_coef_fe = fit_e_fe$coefficients[['expe_sum_ln']]
  ) 
}, future.seed = 123L)

results_df <- bind_rows(results)

# Save the results in .feather, and analyze them in Python
library(arrow)
write_feather(results_df, 'bt10000_results.feather')

rm(main_data_fe, results_df, results, X_e, X_s, coefs_e_init, coefs_s_init, n_random, n_sims, nrow_fe, probs, probs_fe, y_fixed)