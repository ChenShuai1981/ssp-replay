#!/bin/bash

BUILD_NUM=$1
BUILD_TAG=$2

BUILD_FOLDER="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_PATH="$( cd "$( dirname "${BUILD_FOLDER}" )" && pwd )"
PROJECT_NAME=`grep "val Name" ${PROJECT_PATH}/project/build.scala | awk -F\" '{print $2}'`
PROJECT_VERSION=`grep "val Version" ${PROJECT_PATH}/project/build.scala | awk -F\" '{print $2}'`
IMAGE_NAME="192.168.105.243:5000/vpon/${PROJECT_NAME}-${BUILD_TAG}-v${PROJECT_VERSION}"

DOCKER_FILES_FOLDER="${BUILD_FOLDER}/docker_packager"
DOCKER_CONTAINER_NAME="${PROJECT_NAME}-build-files-holder-${BUILD_TAG}"
DOCKER_TAG_COMPILE="${PROJECT_NAME}-build-compile-${BUILD_TAG}-tmp"
DOCKER_TAG_PACKAGE="${PROJECT_NAME}-build-package-${BUILD_TAG}-tmp"

PACKAGE_NAME="project_source_code.tar.gz"
PACKAGE_FILE="${PROJECT_NAME}-assembly.jar"
PACKAGE_FILE_VERSION="${PROJECT_NAME}-assembly-${PROJECT_VERSION}.jar"

echo "================================"
echo " PROJECT_VERSION: [ ${PROJECT_VERSION} ]"
echo " BUILD_FOLDER:    [ ${BUILD_FOLDER} ]"
echo " BUILD_NUM:       [ ${BUILD_NUM} ]"
echo " IMAGE_NAME:      [ ${IMAGE_NAME} ]"
echo "================================"

# Load function tools
. "${BUILD_FOLDER}/tools/functions.sh"

function cleanup_container_and_image(){
  cleanup_container "${DOCKER_CONTAINER_NAME}"
  cleanup_image "${DOCKER_TAG_COMPILE}"
  cleanup_image "${DOCKER_TAG_PACKAGE}"
  cleanup_image "${IMAGE_NAME}" "${BUILD_NUM}"
  cleanup_image "${IMAGE_NAME}"
}

function cleanup_temp_folder(){
  cleanup_folder_files "${DOCKER_FILES_FOLDER}/tmp/"
  cleanup_folder_files "${PROJECT_PATH}/${PACKAGE_NAME}"
}

# Environment init
print_task " Cleaning up build environment "
cleanup_container_and_image
cleanup_temp_folder
cd "${PROJECT_PATH}"

# Pull submodule project
./git_submod_pull.sh master

# Set compression level to the fastest.  -1 (or --fast) to -9 (or --best)
export BZIP=--fast
export GZIP=--fast

# Package project to single file
print_task " Packaging project resources to a single file "
tar zcvf "${PACKAGE_NAME}" ./*
mkdir -p "${DOCKER_FILES_FOLDER}/tmp/"
mv "${PACKAGE_NAME}" "${DOCKER_FILES_FOLDER}/tmp/"

# sbt packager (output fat jar)
#If the <src> parameter of ADD is an archive in a recognised compression format, it will be unpacked
docker build -t "${DOCKER_TAG_COMPILE}" -f "${DOCKER_FILES_FOLDER}/Dockerfile.compile" "${DOCKER_FILES_FOLDER}/" || exit $?

print_task " Compiling project in docker container "
time docker run --cpuset-cpus=2,3 \
                --net=host \
                --name="${DOCKER_CONTAINER_NAME}" \
                --privileged=true \
                -v /etc/localtime:/etc/localtime:ro \
                -v ~/.sbt/:/root/.sbt/ \
                -v ~/.ivy2/:/root/.ivy2/ \
                "${DOCKER_TAG_COMPILE}" sh "/opt/package.sh" || exit $?

print_task " Extracting package file from container "
time docker cp "${DOCKER_CONTAINER_NAME}:/opt/target/scala/${PACKAGE_FILE_VERSION}" "${DOCKER_FILES_FOLDER}/tmp/"
mv   "${DOCKER_FILES_FOLDER}/tmp/${PACKAGE_FILE_VERSION}" "${DOCKER_FILES_FOLDER}/tmp/${PACKAGE_FILE}" || exit $?

# Prepare conf folder
cp -r "${BUILD_FOLDER}/conf" "${DOCKER_FILES_FOLDER}/tmp"

# docker image packager (output docker image)
print_task " Building final docker image "
time docker build -t "${DOCKER_TAG_PACKAGE}" -f "${DOCKER_FILES_FOLDER}/Dockerfile.package" "${DOCKER_FILES_FOLDER}/" || exit $?

# Push image to private docker registry
print_task " Pushing images to private docker registry "
echo "Docker image: [ ${IMAGE_NAME}:${BUILD_NUM} ]"
docker tag     "${DOCKER_TAG_PACKAGE}" "${IMAGE_NAME}:${BUILD_NUM}"
docker tag  -f "${DOCKER_TAG_PACKAGE}" "${IMAGE_NAME}:latest"
docker push "${IMAGE_NAME}:${BUILD_NUM}"
docker push "${IMAGE_NAME}:latest"

# Remove intermediate images and containers on this host
print_task " Removing intermediate data, images and containers "
cleanup_container_and_image
cleanup_temp_folder
