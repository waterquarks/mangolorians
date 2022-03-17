# Mangolorians

Repository for https://mangolorians.com.

## How is the program organized?

Mangolorians currently provides data visualizations and historical data downloads. In order to do this, there are 3 pipelines extracting Mango Markets order book, trade and funding rate data respectively and loading it into a  database. Then this data is transformed as per need and served through the web app.

## Running the project locally

Instructions assume you've got a UNIX-like OS and Python 3.9.10 installed + SQLite 3.38.1. 

```shell
# Clone the repository
git clone https://github.com/waterquarks/mangolorians && cd mangolorians
# Create a virtual environment
python -m venv .venv && source .venv/bin/activate
# Install dependencies 
pip install -r requirements.txt
# Set up the database
sqlite3 dev.db < schema.sql
# Enable concurrent writes and reads - https://stackoverflow.com/a/10387821/9458208
sqlite3 dev.db 'PRAGMA journal_mode=WAL;'
# Boot the funding rates and order book data as independent processes
# If you choose to use a new shell instead, make sure to be sourcing .venv/bin/active
python etl-funding-rates.py &
python etl-order-books.py &
# Set up the funding rates extractor as a cron job to run every minute
crontab -e
# Inside the interactive editor, add the following line replacing the highlighted path
# * * * * * cd /replace/with/your/path/to/mangalorians && source .venv/bin/activate && python etl-funding-rates.py >> etl-funding-rates.log 2>&1
# Finally boot the server in debug mode
export FLASK_DEBUG=1
flask run
```