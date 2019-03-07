load.table <- function(glob, table, drop_hosts=TRUE) {
  data <- list()
  con <- file(glob, "r")
  while (TRUE) {
    line = readLines(con, n = 1)
    if ( length(line) == 0 ) {
      break
    }
    raw_data <- rjson::fromJSON(line)
    date <- raw_data$date
    tags <- raw_data$tags
    if (length(tags$hosts) == 0) {
      tags$hosts <- NULL
    }
    tags <- dplyr::bind_rows(tags)
    tab <- raw_data$tables[table]
    tab <- unname(tab)
    df <- data.frame(tab)
    df <- dplyr::bind_cols(tags, df)
    data <- append(data, list(df))
  }
  close(con)
  dplyr::bind_rows(data)
}

