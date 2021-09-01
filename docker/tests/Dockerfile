FROM curation:base-18.04

# copy in our entrypoint script
COPY --chmod=755 docker/tests/entrypoint.sh /entrypoint.sh

# copy in scripts to be used inside test container
COPY --chown=curation:curation --chmod=755 docker/tests/scripts "${CURATION_SCRIPTS_DIR}"

# copy in a few files used by linters and coveragers
COPY --chown=curation --chmod=755 .coveragerc "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation --chmod=755 .coveragerc_integration "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation --chmod=755 .coveragerc_unit "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation --chmod=755 .pylintrc "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation --chmod=755 .style.yapf "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation --chmod=755 .yapfignore "${CIRCLE_WORKING_DIRECTORY}"/

# define entrypoint
ENTRYPOINT [ "bash", "--login", "/entrypoint.sh" ]