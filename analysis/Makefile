DOCKER_CONTAINER="danny/analysis"
CWD=`pwd`
RSCRIPT=docker run -it --rm -v ${CWD}:/work -t ${DOCKER_CONTAINER} Rscript

.PHONY: all
all: imgs/dep_k.png imgs/sketches.png imgs/load.png tex/best.tex

.PHONY: build-docker
build-docker: Dockerfile
	docker build -t ${DOCKER_CONTAINER} .

imgs/dep_k.png: plot_k_dep.R tables.R
	${RSCRIPT} plot_k_dep.R

imgs/sketches.png: plot_sketches.R tables.R
	${RSCRIPT} plot_sketches.R

imgs/load.png: plot_load.R tables.R
	${RSCRIPT} plot_load.R

tex/best.tex: latex.R tables.R
	${RSCRIPT} latex.R


