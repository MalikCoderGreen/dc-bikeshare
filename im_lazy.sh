#!/bin/bash
# Copy your DAB files into the cloned repo
cp -r ../dc_bikeshare_backup/src .
cp -r ../dc_bikeshare_backup/tests .
cp ../dc_bikeshare_backup/databricks.yml .
cp ../dc_bikeshare_backup/requirements.txt .
cp ../dc_bikeshare_backup/.gitignore . 2>/dev/null || true#!/bin/bash
