con <-
  DBI::dbConnect(RPostgres::Postgres(),
                 host = "fcfc-mesonet-db.cfc.umt.edu",
                 dbname = "blm",
                 user = "mco",
                 password = rstudioapi::askForPassword("Database password"),
                 port = 5433
  )

list.files("./db", full.names = T, pattern = ".csv") %>%
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
          TRUE ~ value
        )
      ) %>%
      dplyr::filter(variable != "ltr")


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
