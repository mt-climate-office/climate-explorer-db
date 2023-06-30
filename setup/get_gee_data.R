library(rgee)
reticulate::use_python("/home/cbrust/git/py-def-env/.venv/bin/python")
reticulate::import("ee")
ee$Initialize()


get_qa_bits = function(img, from, to) {
  size = 1 + to - from
  msk = bitwShiftL(1, size) - 1
  img$rightShift(from)$bitwiseAnd(msk)
}

clean_mod16 <- function(img) {
  qa = img$select("ET_QC")
  good_quality = get_qa_bits(qa, 0, 0)$eq(0)
  no_clouds = get_qa_bits(qa, 3, 4)$eq(0)
  mask = good_quality$And(no_clouds)
  
  img$updateMask(mask)$
    select(c("ET", "PET"))$
    copyProperties(img, list("system:time_start"))
}

clean_mod17 <- function(img) {
  qa = img$select("Psn_QC")
  good_quality = get_qa_bits(qa, 0, 0)$eq(0)
  no_clouds = get_qa_bits(qa, 3, 4)$eq(0)
  mask = good_quality$And(no_clouds)
  
  img$updateMask(mask)$
    select("Gpp")$
    copyProperties(img, list("system:time_start"))
}

clean_mod13 <- function(img) {
  qa = img$select("DetailedQA")
  mask = get_qa_bits(qa, 0, 1)$eq(0)

  img$updateMask(mask)$
    select(c("NDVI", "EVI"))$
    copyProperties(img, list("system:time_start"))
}

reduce_to_region <- function(shp, r, out_name, resolution=500) {
  
  reduced <- r$map(function(image) {
    image$reduceRegions(
      collection = shp,
      reducer = ee$Reducer$mean(),
      scale = resolution
    )$map(function(f) {
      f$set(
        list(
          date = image$date()$format()
        )
      )
    })
  })$flatten()
  
  dat <- rgee::ee_as_sf(reduced, maxFeatures=20000)
  
  dat %>% 
    sf::st_drop_geometry() %>% 
    tibble::as_tibble() %>%
    tidyr::pivot_longer(-c(date, id, name), names_to = "variable")
}

county <- urbnmapr::get_urbn_map(map = "counties", sf = TRUE) %>%
  sf::st_transform(crs = sf::st_crs(4326)) %>%
  dplyr::filter(state_name == "Montana") %>%
  dplyr::select(name=county_name, id=county_fips)

huc <- sf::read_sf("~/git/report-builder/app/app/data/mt_hucs.geojson")

tribes <- mcor::mt_tribal_land %>%
  dplyr::transmute(
    name = Name,
    id = stringr::str_replace_all(Name, " ", "-") %>%
      stringr::str_replace_all("'", "") %>%
      tolower()
  ) %>%
  sf::st_transform(4326)

blm <- sf::read_sf("~/git/report-builder/app/app/data/blm.geojson") 

tidyr::crossing(
  product = c("mod16", "mod17", "mod13", "cover", "npp"),
  loc_type = c("county", "huc", "tribe", "blm")
) %>% 
  dplyr::mutate(
    resolution = dplyr::case_when(
      product %in% c("mod13", "mod16", "mod17") ~ 500, 
      TRUE ~ 30
    ),
    shp =
      dplyr::case_when(
        loc_type == "county" ~ list(county), 
        loc_type == "huc" ~ list(huc),
        loc_type == "tribe" ~ list(tribes),
        loc_type == "blm" ~ list(blm)
      
    ),
    product = 
      dplyr::case_when(
        product == "mod16" ~ list(ee$ImageCollection("MODIS/061/MOD16A2")$map(clean_mod16)),
        product == "mod17" ~ list(ee$ImageCollection("MODIS/006/MOD17A2H")$map(clean_mod17)),
        product == "mod13" ~ list(ee$ImageCollection("MODIS/061/MOD13A1")$map(clean_mod17)),
        product == "cover" ~ list(ee$ImageCollection("projects/rangeland-analysis-platform/vegetation-cover-v3")),
        product == "npp" ~ list(ee.ImageCollection("projects/rangeland-analysis-platform/npp-partitioned-v3"))
      )
  )
mod16 <- ee$ImageCollection("MODIS/061/MOD16A2")$map(clean_mod16)
mod17 <- ee$ImageCollection("MODIS/006/MOD17A2H")$map(clean_mod17)
mod13 <- ee$ImageCollection("MODIS/061/MOD13A1")$map(clean_mod17)
cover <- ee$ImageCollection("projects/rangeland-analysis-platform/vegetation-cover-v3")
npp <- ee.ImageCollection("projects/rangeland-analysis-platform/npp-partitioned-v3")

county <- urbnmapr::get_urbn_map(map = "counties", sf = TRUE) %>% 
  sf::st_transform(crs = sf::st_crs(4326)) %>% 
  dplyr::filter(state_name == "Montana") %>% 
  dplyr::select(name=county_name, id=county_fips) %>% 
  sf_as_ee()