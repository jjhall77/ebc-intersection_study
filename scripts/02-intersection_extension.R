#02-intersection_extension

#-------------------------------------------------
# PART 11: Extended Analysis - Top 500 + Formal Measures
#-------------------------------------------------

library(DescTools)  # for Gini, Kendall's W
library(irr)        # for ICC

#-------------------------------------------------
# 11.1 Rerun comparisons for top 500
#-------------------------------------------------

compare_rankings_extended <- function(results_list, dataset_name, top_n = 500) {
  
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
  
  # Also get counts wide
  counts_wide <- rankings |>
    select(physical_id, total_count, method) |>
    pivot_wider(names_from = method, values_from = total_count, names_prefix = "count_")
  
  # Merge
  rankings_wide <- rankings_wide |>
    left_join(counts_wide, by = "physical_id")
  
  # Calculate rank changes
  
  rankings_wide <- rankings_wide |>
    mutate(
      # In all top N?
      in_all_methods = !is.na(rank_exclude) & !is.na(rank_equal_split) & 
        !is.na(rank_full_count) & !is.na(rank_nearest),
      # Rank range (max - min rank across methods)
      rank_range = pmap_dbl(
        list(rank_exclude, rank_equal_split, rank_full_count, rank_nearest),
        ~ max(c(..1, ..2, ..3, ..4), na.rm = TRUE) - min(c(..1, ..2, ..3, ..4), na.rm = TRUE)
      ),
      # Mean rank across methods
      mean_rank = rowMeans(
        select(cur_data(), starts_with("rank_")), 
        na.rm = TRUE
      )
    )
  
  return(rankings_wide)
}

# Run for top 500
complaints_500 <- compare_rankings_extended(complaints_results, "Complaints", top_n = 500)
shootings_500 <- compare_rankings_extended(shootings_results, "Shootings", top_n = 500)
shots_500 <- compare_rankings_extended(shots_results, "Shots Fired", top_n = 500)

#-------------------------------------------------
# 11.2 Formal Statistical Measures
#-------------------------------------------------

compute_formal_measures <- function(results_list, rankings_wide, dataset_name, top_n = 500) {
  
  cat("\n")
  cat("================================================================\n")
  cat("FORMAL MEASURES:", dataset_name, "- Top", top_n, "\n")
  cat("================================================================\n\n")
  
  # --- A. SET OVERLAP MEASURES ---
  cat("--- A. SET OVERLAP (Jaccard Similarity) ---\n")
  cat("What proportion of top-N blocks are shared between methods?\n\n")
  
  methods <- c("exclude", "equal_split", "full_count", "nearest")
  
  # Get top N block sets for each method
  top_n_sets <- map(methods, function(m) {
    results_list[[m]] |>
      filter(rank <= top_n) |>
      pull(physical_id)
  })
  names(top_n_sets) <- methods
  
  # Jaccard similarity matrix
  jaccard <- function(a, b) {
    length(intersect(a, b)) / length(union(a, b))
  }
  
  jaccard_matrix <- matrix(NA, 4, 4, dimnames = list(methods, methods))
  for (i in 1:4) {
    for (j in 1:4) {
      jaccard_matrix[i, j] <- jaccard(top_n_sets[[i]], top_n_sets[[j]])
    }
  }
  
  cat("Jaccard Similarity Matrix (1.0 = identical sets):\n")
  print(round(jaccard_matrix, 3))
  
  # Blocks in ALL methods
  in_all <- Reduce(intersect, top_n_sets)
  cat("\nBlocks in top", top_n, "under ALL methods:", length(in_all), 
      "(", round(length(in_all) / top_n * 100, 1), "%)\n")
  
  # Blocks unique to one method
  cat("\nBlocks unique to each method:\n")
  for (m in methods) {
    others <- setdiff(methods, m)
    other_blocks <- Reduce(union, top_n_sets[others])
    unique_to_m <- setdiff(top_n_sets[[m]], other_blocks)
    cat("  ", m, ":", length(unique_to_m), "\n")
  }
  
  # --- B. RANK CORRELATIONS ---
  cat("\n--- B. RANK CORRELATIONS ---\n\n")
  
  # Get complete cases (blocks in top N under all methods)
  complete_ranks <- rankings_wide |>
    filter(in_all_methods) |>
    select(starts_with("rank_"))
  
  cat("Blocks with ranks under all methods:", nrow(complete_ranks), "\n\n")
  
  # Spearman correlations
  cat("Spearman Rank Correlations:\n")
  spearman_cor <- cor(complete_ranks, method = "spearman", use = "complete.obs")
  print(round(spearman_cor, 4))
  
  # Kendall's tau (more robust for ties)
  cat("\nKendall's Tau Correlations:\n")
  kendall_cor <- cor(complete_ranks, method = "kendall", use = "complete.obs")
  print(round(kendall_cor, 4))
  
  # --- C. KENDALL'S W (Coefficient of Concordance) ---
  cat("\n--- C. KENDALL'S W (Coefficient of Concordance) ---\n")
  cat("Measures agreement among all 4 methods (0 = no agreement, 1 = perfect)\n\n")
  
  # Reshape for kendall calculation: rows = blocks, cols = methods
  rank_matrix <- as.matrix(complete_ranks)
  
  # Kendall's W
  if (nrow(rank_matrix) >= 3) {
    kendall_result <- kendall(t(rank_matrix), correct = TRUE)
    cat("Kendall's W:", round(kendall_result$value, 4), "\n")
    cat("Chi-squared:", round(kendall_result$statistic, 2), "\n")
    cat("p-value:", format.pval(kendall_result$p.value, digits = 4), "\n")
    
    # Interpretation
    w <- kendall_result$value
    interpretation <- case_when(
      w >= 0.9 ~ "Very strong agreement",
      w >= 0.7 ~ "Strong agreement",
      w >= 0.5 ~ "Moderate agreement",
      w >= 0.3 ~ "Fair agreement",
      TRUE ~ "Poor agreement"
    )
    cat("Interpretation:", interpretation, "\n")
  }
  
  # --- D. INTRACLASS CORRELATION (ICC) ---
  cat("\n--- D. INTRACLASS CORRELATION (ICC) ---\n")
  cat("Measures consistency of rankings across methods\n\n")
  
  if (nrow(rank_matrix) >= 3) {
    icc_result <- icc(rank_matrix, model = "twoway", type = "consistency", unit = "average")
    cat("ICC (consistency):", round(icc_result$value, 4), "\n")
    cat("95% CI: [", round(icc_result$lbound, 4), ",", round(icc_result$ubound, 4), "]\n")
    cat("F-statistic:", round(icc_result$Fvalue, 2), "\n")
    cat("p-value:", format.pval(icc_result$p.value, digits = 4), "\n")
  }
  
  # --- E. RANK DISPLACEMENT STATISTICS ---
  cat("\n--- E. RANK DISPLACEMENT STATISTICS ---\n\n")
  
  # Calculate displacement from 'nearest' (your likely default method)
  displacements <- rankings_wide |>
    filter(!is.na(rank_nearest)) |>
    mutate(
      disp_exclude = abs(coalesce(rank_exclude, top_n + 1) - rank_nearest),
      disp_equal_split = abs(coalesce(rank_equal_split, top_n + 1) - rank_nearest),
      disp_full_count = abs(coalesce(rank_full_count, top_n + 1) - rank_nearest)
    )
  
  cat("Mean Absolute Rank Displacement from 'nearest' method:\n")
  cat("  exclude:     ", round(mean(displacements$disp_exclude, na.rm = TRUE), 1), "\n")
  cat("  equal_split: ", round(mean(displacements$disp_equal_split, na.rm = TRUE), 1), "\n")
  cat("  full_count:  ", round(mean(displacements$disp_full_count, na.rm = TRUE), 1), "\n")
  
  cat("\nMedian Absolute Rank Displacement:\n")
  cat("  exclude:     ", round(median(displacements$disp_exclude, na.rm = TRUE), 1), "\n")
  cat("  equal_split: ", round(median(displacements$disp_equal_split, na.rm = TRUE), 1), "\n")
  cat("  full_count:  ", round(median(displacements$disp_full_count, na.rm = TRUE), 1), "\n")
  
  cat("\nMax Rank Displacement:\n")
  cat("  exclude:     ", max(displacements$disp_exclude, na.rm = TRUE), "\n")
  cat("  equal_split: ", max(displacements$disp_equal_split, na.rm = TRUE), "\n")
  cat("  full_count:  ", max(displacements$disp_full_count, na.rm = TRUE), "\n")
  
  # Displacement thresholds
  cat("\nBlocks displaced by >10 ranks:\n")
  cat("  exclude:     ", sum(displacements$disp_exclude > 10, na.rm = TRUE), "\n")
  cat("  equal_split: ", sum(displacements$disp_equal_split > 10, na.rm = TRUE), "\n")
  cat("  full_count:  ", sum(displacements$disp_full_count > 10, na.rm = TRUE), "\n")
  
  cat("\nBlocks displaced by >50 ranks:\n")
  cat("  exclude:     ", sum(displacements$disp_exclude > 50, na.rm = TRUE), "\n")
  cat("  equal_split: ", sum(displacements$disp_equal_split > 50, na.rm = TRUE), "\n")
  cat("  full_count:  ", sum(displacements$disp_full_count > 50, na.rm = TRUE), "\n")
  
  # --- F. TOP-N STABILITY BY THRESHOLD ---
  cat("\n--- F. TOP-N STABILITY ACROSS THRESHOLDS ---\n")
  cat("% of blocks in top N under ALL methods:\n\n")
  
  thresholds <- c(50, 100, 200, 300, 500)
  
  stability_results <- map_dfr(thresholds, function(n) {
    sets <- map(methods, function(m) {
      results_list[[m]] |>
        filter(rank <= n) |>
        pull(physical_id)
    })
    in_all <- length(Reduce(intersect, sets))
    tibble(
      top_n = n,
      in_all_methods = in_all,
      pct_stable = round(in_all / n * 100, 1)
    )
  })
  
  print(stability_results)
  
  # --- G. CONCENTRATION COMPARISON (Gini) ---
  cat("\n--- G. CONCENTRATION COMPARISON (Gini Coefficient) ---\n")
  cat("Does intersection method affect measured concentration?\n\n")
  
  gini_results <- map_dfr(methods, function(m) {
    counts <- results_list[[m]]$total_count
    tibble(
      method = m,
      gini = round(Gini(counts), 4),
      total_incidents = sum(counts),
      total_blocks = length(counts)
    )
  })
  
  print(gini_results)
  
  # --- H. CUMULATIVE CONCENTRATION ---
  cat("\n--- H. CUMULATIVE CONCENTRATION ---\n")
  cat("% of crime in top X blocks:\n\n")
  
  concentration_results <- map_dfr(methods, function(m) {
    df <- results_list[[m]] |> arrange(desc(total_count))
    total <- sum(df$total_count)
    tibble(
      method = m,
      top_50_pct = round(sum(df$total_count[1:min(50, nrow(df))]) / total * 100, 1),
      top_100_pct = round(sum(df$total_count[1:min(100, nrow(df))]) / total * 100, 1),
      top_200_pct = round(sum(df$total_count[1:min(200, nrow(df))]) / total * 100, 1),
      top_500_pct = round(sum(df$total_count[1:min(500, nrow(df))]) / total * 100, 1)
    )
  })
  
  print(concentration_results)
  
  # Return results as a list
  return(list(
    jaccard = jaccard_matrix,
    spearman = spearman_cor,
    kendall_tau = kendall_cor,
    kendall_w = if(exists("kendall_result")) kendall_result else NULL,
    icc = if(exists("icc_result")) icc_result else NULL,
    displacement = displacements,
    stability = stability_results,
    gini = gini_results,
    concentration = concentration_results
  ))
}

# Run formal measures for each dataset
complaints_measures <- compute_formal_measures(
  complaints_results, complaints_500, "Complaints", top_n = 500
)

shootings_measures <- compute_formal_measures(
  shootings_results, shootings_500, "Shootings", top_n = 500
)

shots_measures <- compute_formal_measures(
  shots_results, shots_500, "Shots Fired", top_n = 500
)

#-------------------------------------------------
# 11.3 Export Extended Results
#-------------------------------------------------

# Export top 500 comparisons
write_csv(complaints_500, here("output", "complaints_ranking_top500.csv"))
write_csv(shootings_500, here("output", "shootings_ranking_top500.csv"))
write_csv(shots_500, here("output", "shots_fired_ranking_top500.csv"))

# Combine all formal measures into summary tables
all_gini <- bind_rows(
  complaints_measures$gini |> mutate(dataset = "complaints"),
  shootings_measures$gini |> mutate(dataset = "shootings"),
  shots_measures$gini |> mutate(dataset = "shots_fired")
)
write_csv(all_gini, here("output", "gini_by_method.csv"))

all_concentration <- bind_rows(
  complaints_measures$concentration |> mutate(dataset = "complaints"),
  shootings_measures$concentration |> mutate(dataset = "shootings"),
  shots_measures$concentration |> mutate(dataset = "shots_fired")
)
write_csv(all_concentration, here("output", "concentration_by_method.csv"))

all_stability <- bind_rows(
  complaints_measures$stability |> mutate(dataset = "complaints"),
  shootings_measures$stability |> mutate(dataset = "shootings"),
  shots_measures$stability |> mutate(dataset = "shots_fired")
)
write_csv(all_stability, here("output", "stability_by_threshold.csv"))

#-------------------------------------------------
# 11.4 Summary Comparison Table (for paper)
#-------------------------------------------------

create_summary_table <- function(measures_list, dataset_names) {
  
  map2_dfr(measures_list, dataset_names, function(m, name) {
    
    # Extract key metrics
    kendall_w <- if(!is.null(m$kendall_w)) round(m$kendall_w$value, 3) else NA
    icc_val <- if(!is.null(m$icc)) round(m$icc$value, 3) else NA
    
    # Mean Jaccard (off-diagonal)
    jaccard_vals <- m$jaccard[lower.tri(m$jaccard)]
    mean_jaccard <- round(mean(jaccard_vals), 3)
    min_jaccard <- round(min(jaccard_vals), 3)
    
    # Mean Spearman (off-diagonal)
    spearman_vals <- m$spearman[lower.tri(m$spearman)]
    mean_spearman <- round(mean(spearman_vals), 3)
    min_spearman <- round(min(spearman_vals), 3)
    
    tibble(
      dataset = name,
      kendall_w = kendall_w,
      icc = icc_val,
      mean_jaccard_top500 = mean_jaccard,
      min_jaccard_top500 = min_jaccard,
      mean_spearman = mean_spearman,
      min_spearman = min_spearman,
      pct_stable_top100 = m$stability$pct_stable[m$stability$top_n == 100],
      pct_stable_top500 = m$stability$pct_stable[m$stability$top_n == 500]
    )
  })
}

summary_table <- create_summary_table(
  list(complaints_measures, shootings_measures, shots_measures),
  c("complaints", "shootings", "shots_fired")
)

cat("\n")
cat("================================================================\n")
cat("SUMMARY TABLE FOR PAPER\n")
cat("================================================================\n\n")
print(summary_table)

write_csv(summary_table, here("output", "intersection_sensitivity_summary.csv"))

#-------------------------------------------------
# 11.5 Additional Visualizations
#-------------------------------------------------

# A. Displacement histogram
plot_displacement <- function(measures, dataset_name) {
  
  displacements <- measures$displacement |>
    select(physical_id, starts_with("disp_")) |>
    pivot_longer(
      cols = starts_with("disp_"),
      names_to = "comparison",
      values_to = "displacement",
      names_prefix = "disp_"
    )
  
  ggplot(displacements, aes(x = displacement, fill = comparison)) +
    geom_histogram(binwidth = 5, alpha = 0.7, position = "identity") +
    facet_wrap(~comparison, ncol = 1) +
    scale_fill_brewer(palette = "Set1") +
    labs(
      title = paste("Rank Displacement from 'Nearest' Method:", dataset_name),
      x = "Absolute Rank Displacement",
      y = "Number of Blocks"
    ) +
    theme_minimal() +
    theme(legend.position = "none")
}

# B. Stability by threshold
plot_stability <- function(all_stability) {
  
  ggplot(all_stability, aes(x = top_n, y = pct_stable, color = dataset)) +
    geom_line(linewidth = 1.2) +
    geom_point(size = 3) +
    scale_color_brewer(palette = "Set1") +
    labs(
      title = "Ranking Stability Across Methods",
      subtitle = "% of top-N blocks that appear in top-N under ALL methods",
      x = "Top N Threshold",
      y = "% Stable",
      color = "Dataset"
    ) +
    theme_minimal() +
    ylim(0, 100)
}

# C. Concentration curves by method
plot_concentration_curve <- function(results_list, dataset_name) {
  
  methods <- names(results_list)
  
  curves <- map_dfr(methods, function(m) {
    df <- results_list[[m]] |> arrange(desc(total_count))
    total <- sum(df$total_count)
    n_blocks <- nrow(df)
    
    tibble(
      method = m,
      pct_blocks = seq(0, 100, length.out = min(1000, n_blocks)),
      pct_crime = map_dbl(seq(0, 100, length.out = min(1000, n_blocks)), function(p) {
        n <- ceiling(p / 100 * n_blocks)
        if (n == 0) return(0)
        sum(df$total_count[1:n]) / total * 100
      })
    )
  })
  
  ggplot(curves, aes(x = pct_blocks, y = pct_crime, color = method)) +
    geom_line(linewidth = 1) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray50") +
    scale_color_brewer(palette = "Set1") +
    labs(
      title = paste("Crime Concentration Curves:", dataset_name),
      subtitle = "How method affects perceived concentration",
      x = "% of Blocks (ranked by crime)",
      y = "% of Crime",
      color = "Method"
    ) +
    theme_minimal() +
    coord_fixed()
}

# Generate and save plots
p_disp_complaints <- plot_displacement(complaints_measures, "Complaints")
p_disp_complaints
p_disp_shootings <- plot_displacement(shootings_measures, "Shootings")
p_disp_shootings
p_disp_shots <- plot_displacement(shots_measures, "Shots Fired")
p_disp_shootings

p_stability <- plot_stability(all_stability)

p_conc_complaints <- plot_concentration_curve(complaints_results, "Complaints")
p_conc_complaints
p_conc_shootings <- plot_concentration_curve(shootings_results, "Shootings")
p_conc_shootings
p_conc_shots <- plot_concentration_curve(shots_results, "Shots Fired")
p_conc_shots




# Save all plots
ggsave(here("output", "displacement_complaints.png"), p_disp_complaints, width = 8, height = 8)
ggsave(here("output", "displacement_shootings.png"), p_disp_shootings, width = 8, height = 8)
ggsave(here("output", "displacement_shots.png"), p_disp_shots, width = 8, height = 8)
ggsave(here("output", "stability_by_threshold.png"), p_stability, width = 8, height = 6)
ggsave(here("output", "concentration_curve_complaints.png"), p_conc_complaints, width = 8, height = 8)
ggsave(here("output", "concentration_curve_shootings.png"), p_conc_shootings, width = 8, height = 8)
ggsave(here("output", "concentration_curve_shots.png"), p_conc_shots, width = 8, height = 8)

cat("\nAll extended analyses complete. Check output/ folder.\n")


#-------------------------------------------------
# 11.6 Rank Stability Plots for Top 500
#-------------------------------------------------

plot_rank_stability_extended <- function(results_list, dataset_name, top_n = 500) {
  
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
    mutate(rank_diff = rank - baseline_rank) |>
    filter(!is.na(baseline_rank))  # Only blocks that appear in nearest top-N
  
  # Plot
  p <- ggplot(ranks_diff |> filter(method != "nearest"), 
              aes(x = baseline_rank, y = rank_diff, color = method)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "gray50") +
    geom_point(alpha = 0.4, size = 1) +
    geom_smooth(method = "loess", se = FALSE, linewidth = 1, span = 0.3) +
    facet_wrap(~method, ncol = 1, scales = "free_y") +
    scale_color_brewer(palette = "Set1") +
    scale_x_continuous(breaks = seq(0, 500, 100)) +
    labs(
      title = paste("Rank Stability:", dataset_name, "(Top", top_n, ")
"),
      subtitle = "Difference from 'nearest block' method",
      x = "Rank under nearest-block method",
      y = "Rank difference (positive = ranked lower)",
      color = "Method"
    ) +
    theme_minimal() +
    theme(
      legend.position = "none",
      strip.text = element_text(size = 11, face = "bold")
    )
  
  return(p)
}

# Generate plots for top 500
p_stability_complaints_500 <- plot_rank_stability_extended(complaints_results, "Complaints", top_n = 500)
p_stability_complaints_500
p_stability_shootings_500 <- plot_rank_stability_extended(shootings_results, "Shootings", top_n = 500)
p_stability_shootings_500
p_stability_shots_500 <- plot_rank_stability_extended(shots_results, "Shots Fired", top_n = 500)
p_stability_shots_500

# Save plots
ggsave(here("output", "rank_stability_complaints_top500.png"), 
       p_stability_complaints_500, width = 10, height = 12, dpi = 300)
ggsave(here("output", "rank_stability_shootings_top500.png"), 
       p_stability_shootings_500, width = 10, height = 12, dpi = 300)
ggsave(here("output", "rank_stability_shots_top500.png"), 
       p_stability_shots_500, width = 10, height = 12, dpi = 300)

cat("Rank stability plots (top 500) saved to output/ folder\n")

# Display one to verify
print(p_stability_shootings_500)
