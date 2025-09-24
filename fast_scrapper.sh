#!/bin/bash

# Run the fast scraper script with provided arguments
python3 scrapper_fast.py https://docs.oracle.com/en-us/iaas/Content/services.htm --max-depth 3 --max-concurrent 20 --delay 0.005 --output service_essentials.md