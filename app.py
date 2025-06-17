from flask import Flask, render_template, request, send_file, flash
from etf_swap_extractor_manual import ETFSwapDataExtractor
import os
import tempfile
import logging
from logging.handlers import RotatingFileHandler

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
            
            # Initialize the extractor with the temporary database
            extractor = ETFSwapDataExtractor(db_path=db_path)
            
            # Get the CIK from the database
            ticker_info = extractor.get_ticker_mapping(ticker)
            if not ticker_info:
                flash(f'No CIK found for ticker {ticker}. Please make sure the ticker is in the database.')
                return render_template('index.html')
            
            cik = ticker_info['cik']
            app.logger.info(f'Found CIK {cik} for ticker {ticker}')
            
            # Process the ticker
            extractor.process_ticker(ticker, cik)
            
            # Export to CSV
            csv_path = os.path.join(temp_dir, f'{ticker.lower()}_swap_data.csv')
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