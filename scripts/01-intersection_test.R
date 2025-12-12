#01- intersections_test
#-------------------------------------------------
# 00-setup.R - Intersection Assignment Sensitivity Analysis
#-------------------------------------------------

library(here)
library(tidyverse)
library(sf)
library(janitor)
library(lubridate)

# Your data is already loaded. Let's set up the analysis.

#-------------------------------------------------
# PART 1: Prepare LION - identify intersections
#-------------------------------------------------

# Filter LION to city streets (same as ebc-geography)
lion_streets <- lion |>
  filter(
    feature_typ %in% c("0", "6"),
    status == "2",
    segment_typ == "U",
    traf_dir %in% c("T", "A", "W"),
    !rw_type %in% c("2", "3", "4", "9")
  ) 

# Get intersection nodes (nodes where 2+ streets meet)
# Load nodes
nodes <- st_read(lion_gdb, layer = "node") |>
  st_transform(2263) |>
  clean_names() %>%
  mutate(nodeid = as.integer(nodeid))

node_stname <- st_read(lion_gdb, layer = "node_stname") |>
  clean_names()

# Count street names per node
node_street_counts <- lion_streets |>
  st_drop_geometry() |>
  select(node_id_from, node_id_to, street) |>
  pivot_longer(cols = c(node_id_from, node_id_to), values_to = "nodeid") |>
  filter(!is.na(nodeid)) |>
  distinct(nodeid, street) |>
  count(nodeid, name = "n_streets")%>%
  mutate(nodeid = as.integer(nodeid))

# Intersections = nodes with 2+ street names
intersection_nodes <- nodes |>
  inner_join(node_street_counts |> filter(n_streets >= 2), by = "nodeid")

cat("Total intersection nodes:", nrow(intersection_nodes), "\n")

# Create intersection buffers (25ft radius - typical intersection size)
intersection_buffers <- intersection_nodes |>
  st_buffer(dist = 25) |>  # 25 feet
  select(nodeid, n_streets)



#-------------------------------------------------
# PART 2: Build segment → physical_id lookup
#-------------------------------------------------

seg_to_physical <- lion_streets |>
  st_drop_geometry() |>
  select(segment_id, physical_id) |>
  filter(!is.na(physical_id)) |>
  distinct()

#-------------------------------------------------
# PART 3: Build intersection → adjacent physical_ids lookup
#-------------------------------------------------

# For each intersection node, find which physical_ids touch it
# (segments that start or end at that node)

intersection_to_physical <- lion_streets |>
  st_drop_geometry() |>
  select(node_id_from, node_id_to, physical_id) |>
  filter(!is.na(physical_id)) |>
  pivot_longer(
    cols = c(node_id_from, node_id_to),
    names_to = "node_type",
    values_to = "nodeid"
  ) |>
  filter(!is.na(nodeid)) |>
  mutate(nodeid = as.integer(nodeid)) |>
  inner_join(
    intersection_nodes |> st_drop_geometry() |> select(nodeid),
    by = "nodeid"
  ) |>
  distinct(nodeid, physical_id)

# Summary: how many physical blocks per intersection?
intersection_to_physical |>
  count(nodeid) |>
  summary()

cat("\nPhysical blocks per intersection:\n")
intersection_to_physical |>
  count(nodeid, name = "n_blocks") |>
  count(n_blocks, name = "n_intersections") |>
  print()

#-------------------------------------------------
# PART 4: Prepare crime datasets
#-------------------------------------------------

# Function to prepare crime data with coordinates
prepare_crime_sf <- function(df, lat_col, lon_col, date_col, id_col) {
  
  lat_col <- enquo(lat_col)
  lon_col <- enquo(lon_col)
  date_col <- enquo(date_col)
  id_col <- enquo(id_col)
  
  df |>
    filter(!is.na(!!lat_col), !is.na(!!lon_col)) |>
    mutate(
      incident_id = !!id_col,
      incident_date = !!date_col
    ) |>
    st_as_sf(coords = c(quo_name(lon_col), quo_name(lat_col)), crs = 4326) |>
    st_transform(2263)
}

# Get last full year end date for each dataset
get_analysis_window <- function(df, date_col) {
  date_col <- enquo(date_col)
  
  max_date <- df |> pull(!!date_col) |> max(na.rm = TRUE)
  
  # Last full year = year before the max date's year, or current year if we have full data
  if (month(max_date) == 12 & day(max_date) >= 28) {
    end_year <- year(max_date)
  } else {
    end_year <- year(max_date) - 1
  }
  
  end_date <- as.Date(paste0(end_year, "-12-31"))
  start_date <- end_date - years(5) + days(1)
  
  list(start = start_date, end = end_date)
}

# --- Complaints ---
complaints_dates <- complaints |>
  mutate(rpt_dt = mdy(rpt_dt)) |>
  pull(rpt_dt)

complaints_window <- list(
  start = max(complaints_dates, na.rm = TRUE) - years(5),
  end = max(complaints_dates, na.rm = TRUE)
)

# Adjust to full years
complaints_window$end <- floor_date(complaints_window$end, "year") + years(1) - days(1)
complaints_window$start <- complaints_window$end - years(5) + days(1)

cat("\nComplaints analysis window:", 
    as.character(complaints_window$start), "to", 
    as.character(complaints_window$end), "\n")

complaints_sf <- complaints |>
  mutate(cmplnt_fr_dt = mdy(cmplnt_fr_dt)) |>
  filter(
    cmplnt_fr_dt >= complaints_window$start,
    cmplnt_fr_dt <= complaints_window$end,
    !is.na(latitude),
    !is.na(longitude)
  ) |>
  mutate(incident_id = cmplnt_num) |>
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) |>
  st_transform(2263)

cat("Complaints in window:", nrow(complaints_sf), "\n")

# --- Shootings ---
shootings_dates <- shootings |>
  mutate(occur_date = mdy(occur_date)) |>
  pull(occur_date)

shootings_window <- list(
  end = max(shootings_dates, na.rm = TRUE),
  start = max(shootings_dates, na.rm = TRUE) - years(5)
)

# Adjust to full years  
shootings_window$end <- floor_date(shootings_window$end, "year") + years(1) - days(1)
shootings_window$start <- shootings_window$end - years(5) + days(1)

cat("\nShootings analysis window:", 
    as.character(shootings_window$start), "to", 
    as.character(shootings_window$end), "\n")

shootings_sf <- shootings |>
  mutate(occur_date = mdy(occur_date)) |>
  filter(
    occur_date >= shootings_window$start,
    occur_date <= shootings_window$end,
    !is.na(latitude),
    !is.na(longitude)
  ) |>
  mutate(incident_id = incident_key) |>
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) |>
  st_transform(2263)

cat("Shootings in window:", nrow(shootings_sf), "\n")

# --- Shots Fired ---


# Get date window
shots_window <- list(
  end = max(shots_fired$cmplnt_fr_dt, na.rm = TRUE),
  start = max(shots_fired$cmplnt_fr_dt, na.rm = TRUE) - years(5)
)

shots_window$end <- floor_date(shots_window$end, "year") + years(1) - days(1)
shots_window$start <- shots_window$end - years(5) + days(1)

cat("\nShots fired analysis window:", 
    as.character(shots_window$start), "to", 
    as.character(shots_window$end), "\n")

shots_fired_sf <- shots_fired_sf |>
  filter(
    cmplnt_fr_dt >= shots_window$start,
    cmplnt_fr_dt <= shots_window$end
  ) |>
  mutate(incident_id = cmplnt_key)

cat("Shots fired in window:", nrow(shots_fired_sf), "\n")

#-------------------------------------------------
# PART 5: Core function - assign incidents to blocks under different rules
#-------------------------------------------------

assign_incidents_to_blocks <- function(incidents_sf, 
                                       physical_blocks,
                                       intersection_buffers,
                                       intersection_to_physical,
                                       method = c("exclude", "equal_split", "full_count", "nearest")) {
  
  method <- match.arg(method)
  
  # Step 1: Identify which incidents are at intersections
  incidents_sf <- incidents_sf |>
    mutate(row_id = row_number())
  
  # Spatial join to intersection buffers
  at_intersection <- st_join(
    incidents_sf |> select(row_id, incident_id),
    intersection_buffers,
    join = st_within
  ) |>
    st_drop_geometry() |>
    filter(!is.na(nodeid))
  
  intersection_incident_ids <- unique(at_intersection$row_id)
  
  n_at_intersection <- length(intersection_incident_ids)
  n_on_block <- nrow(incidents_sf) - n_at_intersection
  
  cat("  Incidents at intersections:", n_at_intersection, 
      "(", round(n_at_intersection / nrow(incidents_sf) * 100, 1), "%)\n")
  cat("  Incidents on block faces:", n_on_block, 
      "(", round(n_on_block / nrow(incidents_sf) * 100, 1), "%)\n")
  
  # Step 2: Assign block-face incidents to nearest physical block
  block_face_incidents <- incidents_sf |>
    filter(!row_id %in% intersection_incident_ids)
  
  if (nrow(block_face_incidents) > 0) {
    # Find nearest physical block for each
    nearest_idx <- st_nearest_feature(block_face_incidents, physical_blocks)
    
    block_face_counts <- block_face_incidents |>
      st_drop_geometry() |>
      mutate(physical_id = physical_blocks$physical_id[nearest_idx]) |>
      count(physical_id, name = "count") |>
      mutate(weight = 1)
  } else {
    block_face_counts <- tibble(physical_id = integer(), count = numeric(), weight = numeric())
  }
  
  # Step 3: Handle intersection incidents based on method
  intersection_incidents <- incidents_sf |>
    filter(row_id %in% intersection_incident_ids)
  
  if (method == "exclude") {
    # Don't count intersection incidents
    intersection_counts <- tibble(physical_id = integer(), count = numeric(), weight = numeric())
    
  } else if (method == "equal_split") {
    # Divide equally among adjacent blocks
    # First, get which intersection each incident is at
    incident_intersections <- at_intersection |>
      distinct(row_id, nodeid)
    
    # For each intersection, get number of adjacent blocks
    blocks_per_intersection <- intersection_to_physical |>
      count(nodeid, name = "n_adjacent")
    
    # Calculate fractional counts
    intersection_counts <- incident_intersections |>
      left_join(blocks_per_intersection, by = "nodeid") |>
      left_join(intersection_to_physical, by = "nodeid") |>
      mutate(weight = 1 / n_adjacent) |>
      group_by(physical_id) |>
      summarise(
        count = sum(weight),
        weight = mean(weight),
        .groups = "drop"
      )
    
  } else if (method == "full_count") {
    # Count fully on each adjacent block (overcounting)
    incident_intersections <- at_intersection |>
      distinct(row_id, nodeid)
    
    intersection_counts <- incident_intersections |>
      left_join(intersection_to_physical, by = "nodeid") |>
      count(physical_id, name = "count") |>
      mutate(weight = 1)
    
  } else if (method == "nearest") {
    # Just use nearest block (same as block-face logic)
    if (nrow(intersection_incidents) > 0) {
      nearest_idx <- st_nearest_feature(intersection_incidents, physical_blocks)
      
      intersection_counts <- intersection_incidents |>
        st_drop_geometry() |>
        mutate(physical_id = physical_blocks$physical_id[nearest_idx]) |>
        count(physical_id, name = "count") |>
        mutate(weight = 1)
    } else {
      intersection_counts <- tibble(physical_id = integer(), count = numeric(), weight = numeric())
    }
  }
  
  # Step 4: Combine and create rankings
  all_counts <- bind_rows(
    block_face_counts |> mutate(source = "block_face"),
    intersection_counts |> mutate(source = "intersection")
  ) |>
    group_by(physical_id) |>
    summarise(
      total_count = sum(count),
      block_face_count = sum(count[source == "block_face"], na.rm = TRUE),
      intersection_count = sum(count[source == "intersection"], na.rm = TRUE),
      .groups = "drop"
    ) |>
    arrange(desc(total_count)) |>
    mutate(rank = row_number())
  
  # Add method info
  all_counts$method <- method
  all_counts$n_at_intersection <- n_at_intersection
  all_counts$n_on_block <- n_on_block
  all_counts$pct_intersection <- round(n_at_intersection / nrow(incidents_sf) * 100, 1)
  
  return(all_counts)
}

#-------------------------------------------------
# PART 6: Run analysis for each dataset and method
#-------------------------------------------------

run_full_analysis <- function(incidents_sf, 
                              dataset_name,
                              physical_blocks,
                              intersection_buffers,
                              intersection_to_physical) {
  
  cat("\n========================================\n")
  cat("Analyzing:", dataset_name, "\n")
  cat("Total incidents:", nrow(incidents_sf), "\n")
  cat("========================================\n")
  
  methods <- c("exclude", "equal_split", "full_count", "nearest")
  
  results <- map(methods, function(m) {
    cat("\nMethod:", m, "\n")
    assign_incidents_to_blocks(
      incidents_sf = incidents_sf,
      physical_blocks = physical_blocks,
      intersection_buffers = intersection_buffers,
      intersection_to_physical = intersection_to_physical,
      method = m
    ) |>
      mutate(dataset = dataset_name)
  })
  
  names(results) <- methods
  
  return(results)
}

# Run for each dataset
complaints_results <- run_full_analysis(
  complaints_sf, "complaints",
  physical_blocks, intersection_buffers, intersection_to_physical
)

shootings_results <- run_full_analysis(
  shootings_sf, "shootings",
  physical_blocks, intersection_buffers, intersection_to_physical
)

shots_results <- run_full_analysis(
  shots_fired_sf, "shots_fired",
  physical_blocks, intersection_buffers, intersection_to_physical
)

#-------------------------------------------------
# PART 7: Compare rankings across methods
#-------------------------------------------------

compare_rankings <- function(results_list, dataset_name, top_n = 100) {
  
  # Extract rankings for each method
  rankings <- map_dfr(names(results_list), function(method) {
    results_list[[method]] |>
      filter(rank <= top_n) |>
      select(physical_id, total_count, rank, method)
  })
  
  # Pivot to wide format for comparison
  rankings_wide <- rankings |>
    select(physical_id, rank, method) |>
    pivot_wider(names_from = method, values_from = rank, names_prefix = "rank_")
  
  # Calculate rank changes
  rankings_wide <- rankings_wide |>
    mutate(
      # Max rank change between any two methods
      max_rank_change = pmax(
        abs(coalesce(rank_exclude, Inf) - coalesce(rank_nearest, Inf)),
        abs(coalesce(rank_equal_split, Inf) - coalesce(rank_full_count, Inf)),
        abs(coalesce(rank_exclude, Inf) - coalesce(rank_full_count, Inf)),
        na.rm = TRUE
      ),
      # In all top N?
      in_all_methods = !is.na(rank_exclude) & !is.na(rank_equal_split) & 
        !is.na(rank_full_count) & !is.na(rank_nearest)
    )
  
  # Summary stats
  cat("\n--- Ranking Comparison:", dataset_name, "(Top", top_n, ") ---\n\n")
  
  # Blocks in top N under all methods
  in_all <- sum(rankings_wide$in_all_methods, na.rm = TRUE)
  cat("Blocks in top", top_n, "under ALL methods:", in_all, 
      "(", round(in_all / top_n * 100, 1), "%)\n\n")
  
  # Correlation matrix of ranks
  rank_cols <- rankings_wide |>
    select(starts_with("rank_")) |>
    drop_na()
  
  if (nrow(rank_cols) > 10) {
    cat("Rank correlations (Spearman):\n")
    cor_matrix <- cor(rank_cols, method = "spearman", use = "pairwise.complete.obs")
    print(round(cor_matrix, 3))
  }
  
  # Blocks with biggest rank changes
  cat("\n\nBlocks with largest rank differences:\n")
  rankings_wide |>
    filter(is.finite(max_rank_change)) |>
    arrange(desc(max_rank_change)) |>
    head(10) |>
    print()
  
  return(rankings_wide)
}

# Compare for each dataset
complaints_comparison <- compare_rankings(complaints_results, "Complaints", top_n = 100)
shootings_comparison <- compare_rankings(shootings_results, "Shootings", top_n = 100)
shots_comparison <- compare_rankings(shots_results, "Shots Fired", top_n = 100)

#-------------------------------------------------
# PART 8: Summary statistics across methods
#-------------------------------------------------

summarize_method_differences <- function(results_list, dataset_name) {
  
  cat("\n========================================\n")
  cat("Method Summary:", dataset_name, "\n")
  cat("========================================\n\n")
  
  # Total counts under each method
  totals <- map_dfr(names(results_list), function(method) {
    results_list[[method]] |>
      summarise(
        method = method,
        total_incidents = sum(total_count),
        total_blocks_with_crime = n(),
        top_block_count = max(total_count),
        median_count = median(total_count),
        pct_at_intersection = first(pct_intersection)
      )
  })
  
  cat("Total counts by method:\n")
  print(totals)
  
  # Top 10 comparison
  cat("\n\nTop 10 blocks by method:\n")
  top10 <- map_dfr(names(results_list), function(method) {
    results_list[[method]] |>
      filter(rank <= 10) |>
      select(physical_id, total_count, rank) |>
      mutate(method = method)
  }) |>
    pivot_wider(
      names_from = method,
      values_from = c(total_count, rank),
      names_glue = "{method}_{.value}"
    )
  
  print(top10)
  
  return(totals)
}

complaints_summary <- summarize_method_differences(complaints_results, "Complaints")
shootings_summary <- summarize_method_differences(shootings_results, "Shootings")
shots_summary <- summarize_method_differences(shots_results, "Shots Fired")

#-------------------------------------------------
# PART 9: Export results
#-------------------------------------------------

# Combine all results
all_results <- bind_rows(
  map_dfr(complaints_results, ~.x),
  map_dfr(shootings_results, ~.x),
  map_dfr(shots_results, ~.x)
)

write_csv(all_results, here("output", "intersection_sensitivity_all_results.csv"))

# Export comparison tables
write_csv(complaints_comparison, here("output", "complaints_ranking_comparison.csv"))
write_csv(shootings_comparison, here("output", "shootings_ranking_comparison.csv"))
write_csv(shots_comparison, here("output", "shots_fired_ranking_comparison.csv"))


# Summary table - fix by adding dataset before binding
all_summaries <- bind_rows(
  complaints_summary |> mutate(dataset = "complaints"),
  shootings_summary |> mutate(dataset = "shootings"),
  shots_summary |> mutate(dataset = "shots_fired")
)

write_csv(all_summaries, here("output", "intersection_method_summaries.csv"))
cat("\n\nResults exported to output/ folder\n")

#-------------------------------------------------
# PART 10: Visualization - Rank stability plot
#-------------------------------------------------

library(ggplot2)

plot_rank_stability <- function(results_list, dataset_name, top_n = 100) {
  
  # Get ranks under each method
  ranks_df <- map_dfr(names(results_list), function(method) {
    results_list[[method]] |>
      filter(rank <= top_n) |>
      select(physical_id, rank, total_count) |>
      mutate(method = method)
  })
  
  # Use 'nearest' as baseline
  baseline <- ranks_df |>
    filter(method == "nearest") |>
    select(physical_id, baseline_rank = rank)
  
  # Join and calculate difference from baseline
  ranks_diff <- ranks_df |>
    left_join(baseline, by = "physical_id") |>
    mutate(rank_diff = rank - baseline_rank)
  
  # Plot
  p <- ggplot(ranks_diff |> filter(method != "nearest"), 
              aes(x = baseline_rank, y = rank_diff, color = method)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
    geom_point(alpha = 0.5, size = 1.5) +
    geom_smooth(method = "loess", se = FALSE, linewidth = 1) +
    facet_wrap(~method, ncol = 1) +
    scale_color_brewer(palette = "Set1") +
    labs(
      title = paste("Rank Stability:", dataset_name),
      subtitle = "Difference from 'nearest block' method",
      x = "Rank under nearest-block method",
      y = "Rank difference (positive = ranked lower)",
      color = "Method"
    ) +
    theme_minimal() +
    theme(legend.position = "none")
  
  return(p)
}


p_complaints <- plot_rank_stability(complaints_results, "Complaints")
p_complaints
p_shootings <- plot_rank_stability(shootings_results, "Shootings")
p_shootings
p_shots <- plot_rank_stability(shots_results, "Shots Fired")
p_shots

ggsave(here("output", "rank_stability_complaints.png"), p_complaints, width = 8, height = 10)
ggsave(here("output", "rank_stability_shootings.png"), p_shootings, width = 8, height = 10)
ggsave(here("output", "rank_stability_shots.png"), p_shots, width = 8, height = 10)

cat("Plots saved to output/ folder\n")
