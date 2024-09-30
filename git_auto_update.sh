#!/bin/bash

# Stage all changes
git add .

# Commit the changes with a message
git commit -m "Auto-update after dbt run"

# Pull the latest changes from the remote repository to avoid conflicts
git pull origin main --rebase

# Push your local commits to the remote repository
git push origin main
