# ETF Swap Data Extractor

A web-based tool for extracting swap data from ETF filings. This tool allows users to easily download swap data for any ETF by simply entering its ticker symbol.

## Features

- Simple web interface
- Instant CSV downloads
- No installation required
- Works with any ETF ticker
- Extracts comprehensive swap data including:
  - Index information
  - Counterparty details
  - Notional amounts
  - Floating rate information
  - Filing dates and URLs

## Live Demo

The application is hosted at: [https://etf-swap-extractor.onrender.com](https://etf-swap-extractor.onrender.com)

## Usage

1. Open the web application
2. Enter an ETF ticker symbol (e.g., GOOX)
3. Click "Download Swap Data"
4. The CSV file will automatically download to your computer

## Technical Details

- Built with Python and Flask
- Uses SEC EDGAR API for data retrieval
- Processes XML filings
- Generates clean CSV output
- Hosted on Render

## Development

If you want to run this locally:

1. Clone the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/etf-swap-extractor.git
   cd etf-swap-extractor
   ```

2. Create a virtual environment:
   ```bash
   python3.11 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Run the application:
   ```bash
   python app.py
   ```

5. Open http://localhost:5000 in your browser

## Deployment

This application is deployed on Render. For deployment instructions, see [DEPLOY.md](DEPLOY.md).

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 