library(rgee)
reticulate::use_python("/home/cbrust/git/py-def-env/.venv/bin/python")
ee_Initialize(drive=T)
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
  print(glue::glue("Working on {out_name}..."))
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
  })$flatten()$map(
    function(x) {
      ee$Feature(NULL, x$toDictionary())
    }
  )

  dat <- rgee::ee_as_sf(reduced, maxFeatures=75000, via="drive")

  dat %>%
    sf::st_drop_geometry() %>%
    tidyr::pivot_longer(-c(date, id, name), names_to = "variable") %>%
    readr::write_csv(out_name)
}

county <- urbnmapr::get_urbn_map(map = "counties", sf = TRUE) %>%
  sf::st_transform(crs = sf::st_crs(4326)) %>%
  dplyr::filter(state_name == "Montana") %>%
  dplyr::select(name=county_name, id=county_fips)

huc <- sf::read_sf("~/git/report-builder/app/app/data/mt_hucs.geojson")

tribes <- sf::read_sf("~/git/report-builder/app/app/data/tribes.geojson") %>%
  dplyr::rowwise() %>%
  dplyr::mutate(id = stringr::str_split(id, "_") %>%
                  unlist() %>%
                  magrittr::extract(2))

blm <- sf::read_sf("~/git/report-builder/app/app/data/blm.geojson")

tidyr::crossing(
  product = c("mod16", "mod17", "npp", "mod13", "myd13", "cover"),
  # product = c("mod16"),
  loc_type = c("county", "huc", "tribe", "blm")
) %>%
  dplyr::mutate(
    resolution = dplyr::case_when(
      product %in% c("mod13", "mod16", "mod17") ~ 500,
      TRUE ~ 30
    ),
    shp =
      dplyr::case_when(
        loc_type == "county" ~ list(county %>%
                                      rgee::sf_as_ee()),
        loc_type == "huc" ~ list(huc %>%
                                   rgee::sf_as_ee()),
        loc_type == "tribe" ~ list(tribes %>%
                                     rgee::sf_as_ee()),
        loc_type == "blm" ~ list(blm %>%
                                   rgee::sf_as_ee())

    ),
    coll =
      dplyr::case_when(
        product == "mod16" ~ list(ee$ImageCollection("MODIS/006/MOD16A2")$map(clean_mod16)),
        product == "mod17" ~ list(ee$ImageCollection("MODIS/006/MOD17A2H")$map(clean_mod17)),
        product == "mod13" ~ list(ee$ImageCollection("MODIS/061/MOD13A1")$map(clean_mod13)),
        product == "myd13" ~ list(ee$ImageCollection("MODIS/061/MYD13A1")$map(clean_mod13)),
        product == "cover" ~ list(ee$ImageCollection("projects/rangeland-analysis-platform/vegetation-cover-v3")),
        product == "npp" ~ list(ee$ImageCollection("projects/rangeland-analysis-platform/npp-partitioned-v3"))
      ),
    out_name = glue::glue("~/git/climate-explorer-db/db/{loc_type}_{product}.csv")
  ) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(extracted = list(reduce_to_region(shp, coll, out_name, resolution)))

