DOCKER_CONTAINER="danny/analysis"
CWD=`pwd`
RSCRIPT=docker run -it --rm -v ${CWD}:/work -t ${DOCKER_CONTAINER} Rscript

.PHONY: all
all: imgs/dep_k.png imgs/sketches.png tex/best.tex tex/info.tex

.PHONY: build-docker
build-docker: Dockerfile
	docker build -t ${DOCKER_CONTAINER} .

imgs/counters.png: plot_k_dep.R tables.R danny-results.sqlite
	${RSCRIPT} plot_k_dep.R

imgs/plot_subproblem_size.png: plot_subproblem_size.R tables.R danny-results.sqlite
	${RSCRIPT} plot_subproblem_size.R

imgs/sketches.png: plot_sketches.R tables.R danny-results.sqlite
	${RSCRIPT} plot_sketches.R

imgs/load.png: plot_load.R tables.R danny-results.sqlite
	${RSCRIPT} plot_load.R

imgs/recall_experiment.png: plot_recall_experiment.R tables.R danny-results.sqlite
	${RSCRIPT} plot_recall_experiment.R

imgs/full.png: plot_full.R tables.R danny-results.sqlite
	${RSCRIPT} plot_full.R

imgs/profile.png: plot_profile.R tables.R danny-results.sqlite
	${RSCRIPT} plot_profile.R

imgs/profile_glove_detail.png: plot_profile.R tables.R danny-results.sqlite
	${RSCRIPT} plot_profile.R

imgs/scalability.png: plot_scalability.R tables.R danny-results.sqlite
	${RSCRIPT} plot_scalability.R

tex/best.tex: latex.R tables.R danny-results.sqlite
	${RSCRIPT} latex.R

tex/info.tex: latex.R tables.R danny-results.sqlite
	${RSCRIPT} latex.R


