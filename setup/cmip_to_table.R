library(magrittr)

reassign_name_as_time <- function(x) {
  names(x) <- terra::time(x)
  return(x)
}


build_table <- function(path, shp, region_type, out_dir = "./db/data/", write=TRUE) {
  
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
  
  out <- out %>%
    dplyr::mutate(r = list(normals::spat_summary(r, shp, attr_id = "id", name_to = "date", fun = "mean", as_spatial=FALSE))) %>%
    tidyr::unnest(cols=r) %>%
    dplyr::mutate(
      date = lubridate::as_date(date),
      value = round(value, 3)
    )  %>% 
    dplyr::filter(!is.na(value)) %>% 
    dplyr::select(model, scenario, variable, name, date, value, id)
  
  if (write) {
    readr::write_csv(out, file.path(out_dir, paste0(region_type, ".csv")))
  }

  return(out)  
}

path = '~/data/cmip6/monthly/'
huc <- sf::read_sf("~/git/report-builder/app/app/data/mt_hucs.geojson")
county <- urbnmapr::get_urbn_map(map = "counties", sf = TRUE) %>% 
  sf::st_transform(crs = sf::st_crs(4326)) %>% 
  dplyr::filter(state_name == "Montana") %>% 
  dplyr::select(name=county_name, id=county_fips)

tribes <- "./app/app/data/tribes.geojson" %>% 
  sf::read_sf() %>% 
  dplyr::mutate(
    id = tolower(name) %>% 
      stringr::str_replace_all(" ", "-") %>% 
      stringr::str_replace("'", "")
  )

blm <- sf::read_sf("~/git/report-builder/app/app/data/blm.geojson") 


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

# build_table(path, huc, "name", "huc", "~/git/report-builder/db/data/")
dat = build_table(path, blm, "blm", "", write=FALSE)
upload_to_db(dat, "future", "blm")
