#!/bin/bash
cd "$(dirname "$0")/backend"
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
