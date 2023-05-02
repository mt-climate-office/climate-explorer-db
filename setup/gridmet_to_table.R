library(magrittr)

set_r_time <- function(r) {
  times <- names(r) %>% 
    stringr::str_split(pattern = "=") %>% 
    purrr::map(magrittr::extract2, 2) %>% 
    unlist() %>% 
    as.numeric() %>%
    as.Date(origin = "1900-01-01")
  
  terra::time(r) <- times
  names(r) <- times
  return(r)
}

zonal_info <- function(files, func) {
  files %>% 
    purrr::map(function(x) {
      print(x)
      terra::rast(x) %>% 
        terra::crop(county) %>% 
        set_r_time() %>% 
        terra::tapp("yearmonths", fun = func) 
    }) %>% 
    terra::rast() %>% 
    normals::spat_summary(county, "name", "date", "mean") %>% 
    dplyr::select(-geometry) %>% 
    dplyr::rename(name=zone) %>% 
    dplyr::mutate(
      date = paste0(date, "01") %>% 
        lubridate::as_date(format="ym_%Y%m%d")
    )
}

county <- urbnmapr::get_urbn_map(map = "counties", sf = TRUE) %>% 
  sf::st_transform(crs = sf::st_crs(4326)) %>% 
  dplyr::filter(state_name == "Montana") %>% 
  dplyr::select(name=county_name, id=county_fips)

test <- list.files(
  "~/data/gridmet/raw", full.names = T, recursive = T, pattern = ".nc"
) %>%
  tibble::tibble(r = .) %>%
  dplyr::mutate(
    name = basename(r) %>%
      tools::file_path_sans_ext()
  ) %>%
  tidyr::separate(name, c("variable", "year")) %>%
  dplyr::mutate(func = ifelse(variable %in% c("pr", "etr", "pet"), "sum", "mean")) %>% 
  dplyr::group_by(variable) %>% 
  dplyr::summarise(
    out = list(zonal_info(r, unique(func)))
  ) %>% 
  tidyr::unnest(out) %>%
  dplyr::select(name, id, variable, date, value) %>% 
  readr::write_csv("./db/data/county_historical.csv")
