DOCKER_CONTAINER="danny/analysis"
CWD=`pwd`
RSCRIPT=docker run -it --rm -v ${CWD}:/work -t ${DOCKER_CONTAINER} Rscript

.PHONY: all
all: imgs/dep_k.png

.PHONY: build-docker
build-docker: Dockerfile
	docker build -t ${DOCKER_CONTAINER} .

imgs/dep_k.png: plot_k_dep.R tables.R
	${RSCRIPT} plot_k_dep.R


