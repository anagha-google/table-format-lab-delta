#!/bin/bash

#........................................................................
# Purpose: Copy existing notebooks to Workbench server Jupyter home dir
# (Managed notebook server)
#........................................................................

gsutil cp gs://dll-code-bucket-YOUR_PROJECT_NBR/*.ipynb /home/jupyter/ 
chown jupyter:jupyter /home/jupyter/* 