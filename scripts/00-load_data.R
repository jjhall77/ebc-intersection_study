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

shots_fired_sf <- shots_fired |>
  # 1. Keep the original geometry string but under a different name
  rename(geom_raw = geometry) |>
  
  # 2. Clean and split "c(x, y)" into numeric columns
  mutate(
    geom_raw = str_remove_all(geom_raw, "c\\(|\\)"),               # remove "c(" and ")"
    x = as.numeric(str_trim(str_split_fixed(geom_raw, ",", 2)[,1])),
    y = as.numeric(str_trim(str_split_fixed(geom_raw, ",", 2)[,2]))
  ) |>
  
  # 3. Convert to sf using x/y as coordinates (NYC State Plane 2263)
  st_as_sf(coords = c("x", "y"), crs = 2263, remove = TRUE)

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

