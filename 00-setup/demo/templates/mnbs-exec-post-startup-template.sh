#!/bin/bash

#........................................................................
# Purpose: Copy existing notebooks to Workbench server Jupyter home dir
# (Managed notebook server)
#........................................................................

gsutil cp gs://dll-code-bucket-YOUR_PROJECT_NBR/*.ipynb /home/jupyter/ 
gsutil cp -r gs://dll-code-bucket-YOUR_PROJECT_NBR/images /home/jupyter/ 
chown jupyter:jupyter /home/jupyter/*
chown jupyter:jupyter /home/jupyter/images/*
