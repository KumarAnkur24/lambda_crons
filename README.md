# lambda_crons

# Always sign ur code using below example
# Description: XXXXXXXXX
# Created By : YAA

# Add Layers on Lambda

# Go to Build Folder 
Step 1: cd build 

# Delete all folders
Step 2:
rm -r python/
rm pyarrow.zip

# Create Directories
Step 3: 
mkdir -p python/lib/python3.8/site-packages/

# Install Module
Step 4:
pip3 install pyarrow -t /home/ubuntu/build/python/lib/python3.8/site-packages/ --system

# Zip the installed Modules
Step 5:
zip -r pyarrow.zip .

# Push Module zip file to s3 location
Step 6:
aws s3 cp pyarrow.zip s3://ayu-lambda-cron/libraries/

# Add Layers
Copy Zip Location from S3
Go to Layers
Add Layers
Choose S3 file type
Choose Python Version
Create layer

