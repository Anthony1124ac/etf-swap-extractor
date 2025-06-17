# Deploying the ETF Swap Data Extractor

This guide will help you deploy the ETF Swap Data Extractor to a free hosting service.

## Option 1: Deploy to Render (Recommended)

1. Create a free account at https://render.com
2. Click "New +" and select "Web Service"
3. Connect your GitHub repository
4. Configure the service:
   - Name: etf-swap-extractor
   - Environment: Python
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `gunicorn app:app`
   - Plan: Free

## Option 2: Deploy to Heroku

1. Create a free account at https://heroku.com
2. Install the Heroku CLI
3. Run these commands:
   ```bash
   heroku login
   heroku create etf-swap-extractor
   git push heroku main
   ```

## After Deployment

1. The application will be available at:
   - Render: https://etf-swap-extractor.onrender.com
   - Heroku: https://etf-swap-extractor.herokuapp.com

2. Share the URL with your boss - they can now:
   - Open the URL in their browser
   - Enter a ticker symbol
   - Click "Download Swap Data"
   - Get the CSV file automatically

## Monitoring

- The application logs are available in the hosting platform's dashboard
- Check the logs if you encounter any issues

## Security Notes

1. The application uses a secret key for session management
2. All data processing happens in temporary directories
3. No data is stored permanently on the server
4. Each request is isolated in its own environment

## Troubleshooting

If you encounter issues:
1. Check the application logs in the hosting platform's dashboard
2. Verify that all dependencies are correctly listed in requirements.txt
3. Ensure the Procfile is present and correct
4. Check that the application is using the correct Python version 