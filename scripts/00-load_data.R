#00-load

library(here)
library(tidyverse)
library(sf)
library(janitor)
library(lubridate)

data_dir <- here("data")

# --- 1) Load + clean_names: complaints (historic + YTD) -----------------------
complaints_hist <- read_csv(
  file.path(data_dir, "NYPD_Complaint_Data_Historic_20251212.csv"),
  show_col_types = FALSE
) %>% 
  clean_names()

complaints_ytd <- read_csv(
  file.path(data_dir, "NYPD_Complaint_Data_Current_(Year_To_Date)_20251212.csv"),
  show_col_types = FALSE
) %>% 
  clean_names() %>%
  mutate(housing_psa = as.character(housing_psa)) 

complaints <- bind_rows(complaints_hist, complaints_ytd) %>%
  distinct()  # helps if YTD overlaps with end of historic extract

# --- 2) Load + clean_names: shootings (historic + YTD) ------------------------
shootings_hist <- read_csv(
  file.path(data_dir, "NYPD_Shooting_Incident_Data_(Historic)_20251123.csv"),
  show_col_types = FALSE
) %>% 
  clean_names() %>%
  mutate(statistical_murder_flag = as.character(statistical_murder_flag))

shootings_ytd <- read_csv(
  file.path(data_dir, "NYPD_Shooting_Incident_Data_(Year_To_Date)_20251123.csv"),
  show_col_types = FALSE
) %>% 
  clean_names()

shootings <- bind_rows(shootings_hist, shootings_ytd) %>%
  distinct()

# --- 3) Load + clean_names: sf_since_2017 + YTD UPDATE.xlsx -------------------
shots_fired <- read_csv(here("data","shots_fired_enriched.csv")) %>%
  clean_names()

# --- 4) Quick sanity checks ---------------------------------------------------

# Path to the geodatabase
lion_gdb <- here("data", "lion", "lion.gdb")

# List available layers inside the .gdb
st_layers(lion_gdb)

lion_gdb <- here("data", "lion", "lion.gdb")

lion <- st_read(lion_gdb, layer = "lion") %>%
  st_transform(2263) %>% 
  clean_names() # NYC coordinates, ft


# read + clean + transform to EPSG:2263
physical_blocks <- st_read(here("data", "physical_blocks.gpkg"), quiet = TRUE) %>%
  clean_names() %>%
  st_transform(2263)

# quick check
st_crs(physical_blocks)

