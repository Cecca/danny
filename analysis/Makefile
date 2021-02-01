DOCKER_CONTAINER="danny/analysis"
CWD=`pwd`
RSCRIPT="docker run -it --rm -v ${CWD}:/work -t ${DOCKER_CONTAINER} Rscript"

.PHONY: build-docker
build-docker: Dockerfile
	docker build -t ${DOCKER_CONTAINER} .

.PHONY: tables
tables: tables.R build-docker
	${RSCRIPT} tables.R

