# ETF Swap Data Extractor

A web-based tool for extracting swap data from ETF filings.

## Setup Instructions

1. Make sure you have Python 3.11 installed on your computer
   - You can download it from https://www.python.org/downloads/
   - Choose Python 3.11.x (latest 3.11 version)
2. Download all the files in this folder to your computer
3. Open Terminal (Mac) or Command Prompt (Windows)
4. Navigate to the folder containing these files
5. Create a virtual environment and activate it:
   ```bash
   # On Mac/Linux:
   python3.11 -m venv venv
   source venv/bin/activate

   # On Windows:
   python3.11 -m venv venv
   venv\Scripts\activate
   ```
6. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Running the Application

1. Make sure you're in the virtual environment (you should see `(venv)` at the start of your command prompt)
2. Run the application:
   ```bash
   python app.py
   ```
3. Open your web browser and go to: http://localhost:5000

## Using the Tool

1. Enter an ETF ticker symbol (e.g., GOOX) in the input field
2. Click "Download Swap Data"
3. The CSV file will automatically download to your computer
4. The file will be named `[ticker]_swap_data.csv` (e.g., `goox_swap_data.csv`)

## Troubleshooting

If you encounter any issues:
1. Make sure all files are in the same folder
2. Ensure you're running the commands from the correct folder
3. Check that you have an active internet connection
4. Verify that you're using Python 3.11
5. If you get any package installation errors, try:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

For additional help, please contact the development team. 