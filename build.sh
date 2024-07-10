#!/bin/bash

source venv/bin/activate
rm -rf ./data/*
python3 download_datastore_api.py && python3 merge_and_upload.py
