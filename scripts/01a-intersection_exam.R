library(leaflet)
library(sf)
library(dplyr)

# Leaflet needs lon/lat (EPSG:4326)
intersection_pts_ll <- intersection_nodes %>%
  st_transform(4326)

intersection_buf_ll <- intersection_buffers %>%
  st_transform(4326)

pal <- colorNumeric(
  palette = "viridis",
  domain  = intersection_pts_ll$n_streets
)

leaflet(intersection_pts_ll) %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  
  # OPTIONAL: show the 25ft buffers as translucent polygons
  addPolygons(
    data = intersection_buf_ll,
    fillColor = ~pal(n_streets),
    fillOpacity = 0.25,
    color = NA,
    group = "Buffers (25ft)"
  ) %>%
  
  # Points
  addCircleMarkers(
    radius = 2,
    stroke = FALSE,
    fillOpacity = 0.9,
    fillColor = ~pal(n_streets),
    popup = ~paste0("nodeid: ", nodeid, "<br>n_streets: ", n_streets),
    group = "Intersection nodes"
  ) %>%
  
  addLegend(
    position = "bottomright",
    pal = pal,
    values = ~n_streets,
    title = "Street names @ node",
    opacity = 1
  ) %>%
  
  addLayersControl(
    overlayGroups = c("Intersection nodes", "Buffers (25ft)"),
    options = layersControlOptions(collapsed = FALSE)
  )
