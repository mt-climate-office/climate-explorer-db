library(magrittr)

reassign_name_as_time <- function(x) {
  names(x) <- terra::time(x)
  return(x)
}


build_table <- function(path, shp, region_type, out_dir = "./db/data/") {
  
  print(glue::glue("Working on {region_type}"))
  out <- list.files(path, full.names = T, include.dirs = FALSE) %>% 
    grep(".json", ., value = T, invert = T) %>% 
    grep("/derived", ., value = T, invert = T) %>% 
    tibble::tibble(f = .) %>% 
    dplyr::mutate(base = basename(f) %>% tools::file_path_sans_ext()) %>% 
    tidyr::separate(base, c("model", "scenario", "drop", "variable"), sep = "_") %>% 
    dplyr::rowwise() %>% 
    dplyr::mutate(r = list(terra::rast(f))) %>% 
    dplyr::mutate(r = list(reassign_name_as_time(r))) %>% 
    dplyr::select(-c(f, drop)) 
  
  out %<>%
    dplyr::mutate(r = list(normals::spat_summary(r, shp, attr_id = "id", name_to = "date", fun = "mean"))) %>%
    tidyr::unnest(cols=r) %>%
    sf::st_drop_geometry() %>% 
    dplyr::select(-geometry) %>% 
    tidyr::separate(date, c("year", "month"), sep = "\\.") %>% 
    dplyr::mutate(
      month = paste0("0.", month) %>% 
        stringr::str_replace("NA", "0") %>% 
        as.numeric() %>% 
        magrittr::multiply_by(12) %>% 
        round() %>%
        magrittr::add(1), 
      date = glue::glue("{year}-{stringr::str_pad(month, 2, pad='0')}-01") %>% 
        lubridate::as_date(),
      value = round(value, 3),
      id = zone
    ) %>% 
    dplyr::select(-zone, -year, -month) %>%
    dplyr::filter(!is.na(value)) %>% 
    dplyr::select(model, scenario, variable, name, date, value, id) 
  
  readr::write_csv(out, file.path(out_dir, paste0(region_type, ".csv")))

  return(out)  
}

path = '~/MCO_onedrive/General/nexgddp_cmip6_montana/data-derived/nexgddp_cmip6/monthly/'
huc <- sf::read_sf("~/git/report-builder/app/app/data/mt_hucs.geojson")
county <- urbnmapr::get_urbn_map(map = "counties", sf = TRUE) %>% 
  sf::st_transform(crs = sf::st_crs(4326)) %>% 
  dplyr::filter(state_name == "Montana") %>% 
  dplyr::select(name=county_name, id=county_fips)

# build_table(path, huc, "name", "huc", "~/git/report-builder/db/data/")
build_table(path, county, "county", "~/git/report-builder/db/data/")
