con <-
  DBI::dbConnect(RPostgres::Postgres(),
                 host = "fcfc-mesonet-db.cfc.umt.edu",
                 dbname = "blm",
                 user = "mco",
                 password = rstudioapi::askForPassword("Database password"),
                 port = 5433
  )

list.files("./db", full.names = T, pattern = ".csv") %>%
  grep("preprocess.csv", ., value = T, invert = T) %>%
  purrr::map(function(x) {
    print(x)
    dat <- readr::read_csv(x, col_types = readr::cols(id="c")) %>%
      dplyr::mutate(
        variable = dplyr::case_when(
          variable == "mean" ~ "gpp",
          variable == "ET" ~ "et_m16",
          variable == "PET" ~ "pet_m16",
          TRUE ~ variable
        ) %>%
          tolower(),
        date = lubridate::date(date),
        value = dplyr::case_when(
          variable %in% c("ndvi", "evi") ~ value * 0.0001,
          variable %in% c("et_m16", "pet_m16") ~ value * 0.1,
          variable == "gpp" ~ value * 0.0001,
          TRUE ~ value * 0.0001
        )
      ) %>%
      dplyr::filter(
        variable != "ltr",
        date < as.Date("2021-01-01"))


  tab <- tools::file_path_sans_ext(x) %>%
                      basename() %>%
                      stringr::str_split_1("_") %>%
                      magrittr::extract(1)


      DBI::dbWriteTable(
        con,
        RPostgres::Id(schema = "historical", table = tab),
        value = as.data.frame(dat),
        append = TRUE,
        overwrite = FALSE,
        row.names = FALSE
      )
  })

DBI::dbDisconnect(con)




# del <- tidyr::crossing(
#   ids = c("afg", "bgr", "evi", "ltr", "pet_m16", 
#           "pfgnpp", "shrnpp", "trenpp", "afgnpp", "et_m16",
#           "gpp", "ndvi", "pfg", "shr", "tre"),
#   dbs = c("historical.blm", "historical.county", "historical.huc")
# ) %>%
#   dplyr::mutate(
#     delete_queries = glue::glue("DELETE from {dbs} WHERE (variable = '{ids}');")
#   )
# 
# purrr::map(.x = del$delete_queries, .f = DBI::dbExecute, conn = con)

