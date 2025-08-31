# Email AI Agent Setup Guide

This guide will help you set up your Gmail AI Agent that can delete emails using natural language commands like "delete all amazon ads".

## Prerequisites

- Python 3.7 or higher
- A Google account
- (Optional) Gemini API key for better AI understanding

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Set Up Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Gmail API:
   - Go to "APIs & Services" > "Library"
   - Search for "Gmail API"
   - Click on it and press "Enable"

## Step 3: Create Credentials

1. Go to "APIs & Services" > "Credentials"
2. Click "Create Credentials" > "OAuth 2.0 Client IDs"
3. Choose "Desktop application" as the application type
4. Give it a name (e.g., "Email AI Agent")
5. Click "Create"
6. Download the credentials file (JSON format)
7. Rename it to `credentials.json` and place it in your project folder

## Step 4: (Optional) Set Up Gemini AI

For better natural language understanding:

1. Go to [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Create a new API key
3. Set it as an environment variable:
   ```bash
   # On Windows:
   set GEMINI_API_KEY=your_api_key_here
   
   # On Mac/Linux:
   export GEMINI_API_KEY=your_api_key_here
   ```

## Step 5: Run the Agent

```bash
python email_ai_agent.py
```

On first run, you'll be prompted to authorize the application in your browser.

## Available Commands

The agent understands natural language commands like:

- `"delete all amazon ads"` - Deletes emails with "amazon" and "ad/ads" in the subject
- `"delete emails from amazon.com"` - Deletes all emails from amazon.com domain
- `"delete all promotions"` - Deletes emails with promotional keywords
- `"list recent emails"` - Shows your recent emails
- `"exit"` - Quits the program

## How It Works

1. **AI Parsing**: If you have a Gemini API key, the agent uses AI to understand your commands
2. **Manual Parsing**: As a fallback, it uses pattern matching for common commands
3. **Safety**: The agent asks for confirmation before deleting emails
4. **Flexible**: Can delete by domain, sender, or subject keywords

## Troubleshooting

### "credentials.json not found"
- Make sure you downloaded the credentials file from Google Cloud Console
- Rename it to exactly `credentials.json`
- Place it in the same folder as `email_ai_agent.py`

### "Gmail API error"
- Make sure you enabled the Gmail API in Google Cloud Console
- Check that your credentials are correct
- Try deleting `token.pickle` and re-authenticating

### "Command not understood"
- Try the exact command examples shown above
- If you have a Gemini API key, it will understand more natural language
- The agent works best with clear, specific commands

## Security Notes

- The agent only has access to read and modify your emails (not send)
- It asks for confirmation before deleting anything
- Your credentials are stored locally in `token.pickle`
- Never share your `credentials.json` or `token.pickle` files

## Example Usage

```
🤖 Gmail AI Agent
==================================================

You: delete all amazon ads
🤖 Processing command: 'delete all amazon ads'
✅ Parsed command: {'action': 'delete', 'target_type': 'subject_keywords', 'target': ['amazon', 'ad', 'ads'], 'confirmation_required': True}
Found 5 emails with keywords: amazon, ad, ads
Do you want to delete these emails? (y/n): y
✅ Deleted 5 emails with keywords: amazon, ad, ads
```

Enjoy your AI-powered email management! 🚀 