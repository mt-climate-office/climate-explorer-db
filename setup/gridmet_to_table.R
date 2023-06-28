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

zonal_info <- function(files, func, shp) {
  files %>% 
    purrr::map(function(x) {
      print(x)
      terra::rast(x) %>% 
        terra::crop(shp) %>% 
        set_r_time() %>% 
        terra::tapp("yearmonths", fun = func) 
    }) %>% 
    terra::rast() %>% 
    normals::spat_summary(shp, "name", "date", "mean") %>% 
    dplyr::mutate(
      date = paste0(date, "01") %>% 
        lubridate::as_date(format="ym_%Y%m%d")
    )
}

# county <- urbnmapr::get_urbn_map(map = "counties", sf = TRUE) %>% 
#   sf::st_transform(crs = sf::st_crs(4326)) %>% 
#   dplyr::filter(state_name == "Montana") %>% 
#   dplyr::select(name=county_name, id=county_fips)
# 
# huc <- sf::read_sf("~/git/report-builder/app/app/data/mt_hucs.geojson")
# 
# tribes <- mcor::mt_tribal_land %>% 
#   dplyr::transmute(
#     name = Name, 
#     id = stringr::str_replace_all(Name, " ", "-") %>%
#       stringr::str_replace_all("'", "") %>% 
#       tolower()
#   ) %>% 
#   sf::st_transform(4326)

blm <- sf::read_sf("~/git/report-builder/app/app/data/blm.geojson")


make_historical_data <- function(shp, type) {
  list.files(
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
      out = list(zonal_info(r, unique(func), shp))
    ) %>% 
    tidyr::unnest(out) %>%
    dplyr::select(name, id, variable, date, value) %>% 
    readr::write_csv(glue::glue("~/git/climate-explorer-db/db/data/{type}_historical.csv"))
}

upload_to_db <- function(dat, schema, table) {
  
  con <-
    DBI::dbConnect(RPostgres::Postgres(),
                   host = "fcfc-mesonet-db.cfc.umt.edu",
                   dbname = "blm",
                   user = "mco",
                   password = rstudioapi::askForPassword("Database password"),
                   port = 5433
    )
  
  DBI::dbWriteTable(
    con, 
    RPostgres::Id(schema = schema, table = table),
    value = as.data.frame(dat),
    append = TRUE,
    overwrite = FALSE,
    row.names = FALSE
  )
  
  DBI::dbDisconnect(con)
} 

# make_historical_data(county, "county")
# make_historical_data(huc, "huc")
# make_historical_data(tribes, "tribes")
dat <- make_historical_data(blm, "blm")
upload_to_db(dat, "historical", "blm")
