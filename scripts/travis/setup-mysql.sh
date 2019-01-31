#!/bin/bash

set -e

# Setup mysql before running test
echo setting up mysql
mysql -u root -e "GRANT ALL PRIVILEGES ON *.* TO 'uber'@'localhost' IDENTIFIED BY 'uber';"