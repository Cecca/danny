DOCKER_CONTAINER="danny/analysis"
CWD=`pwd`
RSCRIPT=docker run -it --rm -v ${CWD}:/work -t ${DOCKER_CONTAINER} Rscript

.PHONY: all
all: imgs/dep_k.png imgs/sketches.png imgs/load.png tex/best.tex

.PHONY: build-docker
build-docker: Dockerfile
	docker build -t ${DOCKER_CONTAINER} .

imgs/dep_k.png: plot_k_dep.R tables.R danny-results.sqlite
	${RSCRIPT} plot_k_dep.R

imgs/sketches.png: plot_sketches.R tables.R danny-results.sqlite
	${RSCRIPT} plot_sketches.R

imgs/load.png: plot_load.R tables.R danny-results.sqlite
	${RSCRIPT} plot_load.R

imgs/recall_experiment.png: plot_recall_experiment.R tables.R danny-results.sqlite
	${RSCRIPT} plot_recall_experiment.R

imgs/full.png: plot_full.R tables.R danny-results.sqlite
	${RSCRIPT} plot_full.R

tex/best.tex: latex.R tables.R danny-results.sqlite
	${RSCRIPT} latex.R

tex/info.tex: latex.R tables.R danny-results.sqlite
	${RSCRIPT} latex.R


