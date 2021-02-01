FROM rocker/tidyverse:4.0.3

RUN apt-get update \
 && apt-get install -y libudunits2-dev

RUN install2.r --deps TRUE \
    kableExtra \
    here \
    units \
    ggforce

RUN install2.r --deps TRUE languageserver

WORKDIR /work

