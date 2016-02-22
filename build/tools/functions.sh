#!/bin/bash

# Modify Date: 20151123

function print_task() {
  local TITLE=$1 && local LINE_LENGTH=80 && local PAD=$(printf '%0.1s' "="{1..60})
  local LEFT_PADDING=$((($LINE_LENGTH-${#TITLE})/2)) && local RIGHT_PADDING=$(($LINE_LENGTH-${#TITLE}-$LEFT_PADDING))
  printf "\n\n\033[33m%.*s%s%.*s\033[0m\n\n" $((LEFT_PADDING)) "${PAD}" "${TITLE}" $((RIGHT_PADDING)) "${PAD}"
}

function cleanup_container() {
  local CONTAINER_NAME=$1
  echo "Looking for container [ ${CONTAINER_NAME} ]" && local MESSAGE=" ===> Stop running container [ ${CONTAINER_NAME} ]"
  docker ps -a | grep -e "[[:space:]]${CONTAINER_NAME}[[:space:]]" && echo "${MESSAGE}" && docker rm  -f "${CONTAINER_NAME}" || echo
}

function cleanup_image() {
  local IMAGE_NAME=$1
  local TAG_NAME=$2
  if [[ -z "${TAG_NAME}" ]]; then TAG_NAME="latest"; fi
  echo "Looking for image [ ${IMAGE_NAME} ]" && local MESSAGE=" ===> Remove older image [ ${IMAGE_NAME}:${TAG_NAME} ]"
  docker images | grep -e "${IMAGE_NAME}[[:space:]]" | grep -e "[[:space:]]${TAG_NAME}[[:space:]]" && echo "${MESSAGE}" && docker rmi -f "${IMAGE_NAME}:${TAG_NAME}" || echo
}

function cleanup_folder_files() {
  local FOLDER_PATH=$1
  local MESSAGE="Remove legacy files [ ${FOLDER_PATH} ]"
  if [ -d "${FOLDER_PATH}" ]; then echo "${MESSAGE}" && rm -rf "${FOLDER_PATH}" || echo; fi
}