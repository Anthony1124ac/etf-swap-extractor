from flask import Flask, render_template, request, send_file, flash
from flask_cors import CORS
from etf_swap_extractor_manual import ETFSwapDataExtractor
import os
from datetime import datetime
import logging
import tempfile
from logging.handlers import RotatingFileHandler

# Configure logging
if not os.path.exists('logs'):
    os.mkdir('logs')
file_handler = RotatingFileHandler('logs/app.log', maxBytes=10240, backupCount=10)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
))
file_handler.setLevel(logging.INFO)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-here')

# Initialize the ETF Swap Data Extractor
extractor = ETFSwapDataExtractor()

# Define ETF information
etf_info = {
    "TSLL": {
        "cik": "0001424958",
        "series_id": "S000072483",
        "issuer": "Direxion",
        "description": "Direxion Daily TSLA Bull 2X Shares"
    },
    "TQQQ": {
        "cik": "0001424958",
        "series_id": "S000072483",
        "issuer": "ProShares",
        "description": "ProShares UltraPro QQQ"
    },
    "NDVU": {
        "cik": "0001424958",
        "series_id": "S000072483",
        "issuer": "Direxion",
        "description": "Direxion Daily NVDA Bull 2X Shares"
    }
}

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_ticker():
    ticker = request.form.get('ticker', '').strip().upper()
    
    if not ticker:
        flash('Please enter a ticker symbol')
        return render_template('index.html')
    
    try:
        app.logger.info(f'Processing request for ticker: {ticker}')
        
        # Create a temporary directory for the database and CSV
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, 'etf_swaps.db')
            csv_path = os.path.join(temp_dir, f'{ticker.lower()}_swap_data.csv')
            
            # Initialize the extractor with the temporary database
            extractor = ETFSwapDataExtractor(db_path=db_path)
            
            # Process the ticker
            extractor.process_ticker(ticker)
            
            # Export to CSV
            extractor.export_to_csv(csv_path, ticker)
            
            app.logger.info(f'Successfully processed {ticker}')
            
            # Send the file to the user
            return send_file(
                csv_path,
                as_attachment=True,
                download_name=f'{ticker.lower()}_swap_data.csv',
                mimetype='text/csv'
            )
            
    except Exception as e:
        app.logger.error(f'Error processing {ticker}: {str(e)}')
        flash(f'Error processing ticker: {str(e)}')
        return render_template('index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port) 