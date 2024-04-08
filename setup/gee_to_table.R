library(magrittr)

con <-
  DBI::dbConnect(RPostgres::Postgres(),
                 host = "fcfc-mesonet-db.cfc.umt.edu",
                 dbname = "blm",
                 user = "mco",
                 password = rstudioapi::askForPassword("Database password"),
                 port = 5433
  )

combine_mod13 <- function(data_dir) {
  list.files(data_dir, full.names = T, pattern = "myd13|mod13") %>% 
    tibble::tibble(f = .) %>% 
    dplyr::mutate(b = basename(f) %>% 
                    tools::file_path_sans_ext()) %>% 
    tidyr::separate(b, c("loc", "model")) %>% 
    dplyr::group_by(loc) %>%
    dplyr::summarise(
      dat = list(
        purrr::map_df(f, readr::read_csv, show_col_types=FALSE)
      )
    ) %>% 
    dplyr::mutate(out = glue::glue("./db/{loc}_mcd13.csv")) %>%
    dplyr::rowwise() %>% 
    dplyr::mutate(dat = list(readr::write_csv(dat, out)))
  
  
}

combine_mod13("./db")
list.files("./db", full.names = T, pattern = ".csv") %>%
  grep("preprocess.csv|myd13.csv|mod13.csv", ., value = T, invert = T) %>%
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
          stringr::str_detect(variable, "npp") ~ value * 0.0001,
          TRUE ~ value
        )
      ) %>%
      dplyr::filter(
        variable != "ltr",
        date < as.Date("2021-01-01")) %>% 
      dplyr::group_by(year=lubridate::year(date), month=lubridate::month(date), variable, name, id) %>%
      dplyr::summarise(
        value = dplyr::case_when(
          dplyr::first(variable %in% c("afg", "bgr", "evi", "ltr", "ndvi", "pfg", "shr", "tre")) ~ mean(value, na.rm = T),
          TRUE ~ sum(value, na.rm = T)
        ),
        .groups = "drop"
      ) %>%
      dplyr::mutate(date = glue::glue("{year}-{month}-01") %>% 
                      as.Date()) %>%
      dplyr::select(-c(year, month))


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




del <- tidyr::crossing(
  ids = c("afg", "bgr", "evi", "ltr", "pet_m16",
          "pfgnpp", "shrnpp", "trenpp", "afgnpp", "et_m16",
          "gpp", "ndvi", "pfg", "shr", "tre"),
  dbs = c("historical.tribes")#  "historical.blm", "historical.county", "historical.huc")
) %>%
  dplyr::mutate(
    delete_queries = glue::glue("DELETE from {dbs} WHERE (variable = '{ids}');")
  )

purrr::map(.x = del$delete_queries, .f = DBI::dbExecute, conn = con)

