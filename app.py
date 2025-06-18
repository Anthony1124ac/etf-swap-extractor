from flask import Flask, render_template, request, send_file, flash, redirect
from etf_swap_extractor_manual import ETFSwapDataExtractor
import os
import tempfile
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import sys
import redis
from rq import Queue
import boto3

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-here')

# Configure logging
if not os.path.exists('logs'):
    os.mkdir('logs')
file_handler = RotatingFileHandler('logs/app.log', maxBytes=10240, backupCount=10)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)
app.logger.info('ETF Swap Extractor startup')

# Print current directory and list files for debugging
app.logger.info(f'Current directory: {os.getcwd()}')
app.logger.info('Files in current directory:')
for file in os.listdir('.'):
    app.logger.info(f'  {file}')

# Redis and RQ setup
redis_url = os.environ.get('REDIS_URL')
redis_conn = redis.from_url(redis_url)
q = Queue(connection=redis_conn)

# S3 setup
def upload_to_s3(local_path, s3_key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ['AWS_DEFAULT_REGION']
    )
    bucket = os.environ['S3_BUCKET_NAME']
    s3.upload_file(local_path, bucket, s3_key)
    url = s3.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': bucket,
            'Key': s3_key,
            'ResponseContentDisposition': f'attachment; filename={s3_key}'
        },
        ExpiresIn=3600
    )
    return url

# Try multiple possible locations for the CSV file
possible_paths = [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etf_tickers.csv'),
    os.path.join(os.getcwd(), 'etf_tickers.csv'),
    '/opt/render/project/src/etf_tickers.csv',
    'etf_tickers.csv'
]

csv_path = None
for path in possible_paths:
    app.logger.info(f'Trying path: {path}')
    if os.path.exists(path):
        csv_path = path
        app.logger.info(f'Found CSV file at: {path}')
        break

if csv_path is None:
    app.logger.error('CSV file not found in any of the expected locations')
    sys.exit(1)

try:
    app.logger.info(f'Loading ticker mappings from: {csv_path}')
    ticker_mappings = pd.read_csv(csv_path)
    # Convert CIK numbers to 10-digit strings with leading zeros
    ticker_mappings['CIK'] = ticker_mappings['CIK'].astype(str).str.zfill(10)
    ticker_to_cik = dict(zip(ticker_mappings['Ticker'], ticker_mappings['CIK']))
    ticker_to_series = dict(zip(ticker_mappings['Ticker'], ticker_mappings['Series']))
    app.logger.info(f'Successfully loaded {len(ticker_to_cik)} ticker mappings')
except Exception as e:
    app.logger.error(f'Error loading CSV file: {str(e)}')
    sys.exit(1)

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

# Background job function
def run_etf_extraction(ticker, cik, series_id):
    extractor = ETFSwapDataExtractor()
    extractor.process_ticker(ticker, cik, series_id=series_id)
    local_csv_path = f"{ticker.lower()}_swap_data.csv"
    extractor.export_to_csv(local_csv_path, ticker)
    s3_key = os.path.basename(local_csv_path)
    s3_url = upload_to_s3(local_csv_path, s3_key)
    return s3_url

@app.route('/process', methods=['POST'])
def process_ticker():
    ticker = request.form.get('ticker', '').strip().upper()
    
    if not ticker:
        flash('Please enter a ticker symbol')
        return render_template('index.html')
    
    if ticker not in ticker_to_cik:
        flash(f'No CIK found for ticker {ticker}. Please make sure the ticker is in the database.')
        return render_template('index.html')
    
    cik = ticker_to_cik[ticker]
    series_id = ticker_to_series[ticker]
    app.logger.info(f'Found CIK {cik} and series ID {series_id} for ticker {ticker}')
    
    # Enqueue the background job
    job = q.enqueue(run_etf_extraction, ticker, cik, series_id)
    return render_template('processing.html', job_id=job.get_id())

@app.route('/status/<job_id>')
def job_status(job_id):
    job = q.fetch_job(job_id)
    if job is None:
        return 'Job not found', 404
    if job.is_finished:
        # Redirect to S3 URL if job is done
        return redirect(job.result)
    elif job.is_failed:
        return 'Job failed', 500
    else:
        return 'Job is still running. Please refresh this page in a moment.'

# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return render_template('index.html'), 404

@app.errorhandler(500)
def internal_error(error):
    app.logger.error(f'Server Error: {error}')
    return render_template('index.html'), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port) 