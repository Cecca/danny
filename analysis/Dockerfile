FROM rocker/tidyverse:4.0.3

RUN apt-get update \
 && apt-get install -y libudunits2-dev

RUN install2.r --deps TRUE \
    kableExtra \
    here \
    units \
    ggforce

RUN install2.r --deps TRUE languageserver
RUN install2.r --deps TRUE --error patchwork
RUN install2.r --deps TRUE --error latex2exp
RUN install2.r --deps TRUE --error ggridges
RUN install2.r --deps TRUE --error ggrepel
RUN install2.r --deps TRUE --error Hmisc

WORKDIR /work

