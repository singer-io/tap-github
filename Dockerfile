FROM slalomggp/dataops:latest-dev

ENV PROJECT_DIR /projects/my-project
COPY . $PROJECT_DIR
WORKDIR $PROJECT_DIR

RUN ls -la

# Add tap-salesforce config template
RUN mkdir -p /projects/my-project/.secrets

# RUN s-tap install tap-rest-api .
RUN s-tap install target-csv
