import os
import hashlib 
import pickle
import base64
import email
import json
import re
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from email.mime.text import MIMEText
from email.utils import getaddresses, parseaddr
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.generativeai as genai
from thefuzz import fuzz
from django.utils.translation import gettext as _

# Gmail API setup
# We need the .compose scope to send emails
SCOPES = ['https://www.googleapis.com/auth/gmail.modify', 'https://www.googleapis.com/auth/gmail.compose']

class GmailAIAgent:
    def __init__(self, gmail_api_key=None, gemini_api_key=None):
        self.service = None
        self.gemini_model = None
        self._undo_store = {}
        self._undo_counter = 0
        self.default_max_results = 50
        # Cache for recent contacts suggestions (to avoid frequent Gmail API calls)
        self._contacts_cache = None
        self._contacts_cache_ts = 0
        self.setup_gemini_api(gemini_api_key)
    
    def setup_gmail_api(self):
        """Setup Gmail API authentication"""
        try:
            creds = None
            # Load existing credentials if available
            if os.path.exists('token.pickle'):
                try:
                    with open('token.pickle', 'rb') as token:
                        creds = pickle.load(token)
                except Exception:
                    creds = None
            # If no valid credentials, get new ones
            if not creds or not getattr(creds, 'valid', False):
                if creds and getattr(creds, 'expired', False) and getattr(creds, 'refresh_token', None):
                    try:
                        creds.refresh(Request())
                    except Exception:
                        # Refresh failed (expired/revoked). Remove token and fail gracefully.
                        try:
                            os.remove('token.pickle')
                        except Exception:
                            pass
                        return False
                else:
                    # Only attempt interactive auth if credentials.json exists
                    if not os.path.exists('credentials.json'):
                        return False
                    try:
                        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                        creds = flow.run_local_server(port=0)
                    except Exception:
                        return False
            # Save credentials for next run
            try:
                with open('token.pickle', 'wb') as token:
                    pickle.dump(creds, token)
            except Exception:
                pass
            # Build service
            try:
                # Mitigate intermittent SSL/proxy issues by disabling proxy/env SSL overrides
                for var in [
                    'HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy',
                    'ALL_PROXY','all_proxy','REQUESTS_CA_BUNDLE','SSL_CERT_FILE'
                ]:
                    if os.environ.get(var):
                        os.environ.pop(var, None)
                # Bypass proxies specifically for Google domains
                existing_no_proxy = os.environ.get('NO_PROXY') or os.environ.get('no_proxy') or ''
                google_no_proxy = '.googleapis.com,.googleusercontent.com,googleapis.com,googleusercontent.com'
                merged_no_proxy = (
                    google_no_proxy if not existing_no_proxy else f"{existing_no_proxy},{google_no_proxy}"
                )
                os.environ['NO_PROXY'] = merged_no_proxy
                os.environ['no_proxy'] = merged_no_proxy

                self.service = build('gmail', 'v1', credentials=creds)
            except Exception:
                return False
            return True
        except Exception:
            return False
    
    def setup_gemini_api(self, api_key):
        """Setup Gemini AI for natural language processing"""
        if api_key:
            genai.configure(api_key=api_key)
            self.gemini_model = genai.GenerativeModel('models/gemini-2.0-flash')
            return True
        else:
            # Using enhanced command parsing is the fallback.
            return False
    
    def list_recent_emails(self, max_results=None, page_token=None):
        """List recent emails with pagination support"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                if max_results is None:
                    max_results = self.default_max_results
                kwargs = {"userId": 'me', "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
                if page_token:
                    kwargs["pageToken"] = page_token
                results = self.service.users().messages().list(**kwargs).execute()
                messages = results.get('messages', [])
                next_token = results.get('nextPageToken')
                email_list = []
                for message in messages:
                    msg = self.service.users().messages().get(
                        userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                        fields='payload/headers,id').execute()
                    headers = msg['payload']['headers']
                    subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                    sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
                    email_list.append({
                        'id': message['id'],
                        'subject': subject,
                        'sender': sender,
                        'snippet': ''
                    })
                return {"emails": email_list, "next_page_token": next_token}
            except Exception as e:
                error_msg = str(e)
                print(f"Error in list_recent_emails (attempt {attempt + 1}): {error_msg}")
                
                # Check if it's an SSL error or connection issue
                if "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "timeout" in error_msg.lower():
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                
                # If it's not a retryable error or we've exhausted retries
                return {"emails": [], "next_page_token": None, "error": error_msg}
        
        return {"emails": [], "next_page_token": None, "error": "Max retries exceeded"}
    
    def list_emails_by_category(self, category_id, max_results=None, older_than_days=None, page_token=None):
        """List emails from a specific Gmail category (e.g., CATEGORY_PROMOTIONS) with pagination."""
        try:
            if max_results is None:
                max_results = self.default_max_results
            query_parts = [f"category:{category_id}"]
            if older_than_days:
                from datetime import datetime, timedelta
                cutoff_date = (datetime.utcnow() - timedelta(days=older_than_days)).strftime('%Y/%m/%d')
                query_parts.append(f"before:{cutoff_date}")
            query = " ".join(query_parts)

            kwargs = {"userId": 'me', "q": query, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.service.users().messages().list(**kwargs).execute()
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')

            email_list = []
            for message in messages:
                msg = self.service.users().messages().get(
                    userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                    fields='payload/headers,id').execute()
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
                email_list.append({
                    'id': message['id'],
                    'subject': subject,
                    'sender': sender,
                    'snippet': ''
                })

            return {"emails": email_list, "next_page_token": next_token}

        except HttpError as error:
            print(f"❌ Error listing emails by category: {error}")
            return []

    def list_emails_by_domain(self, domain, max_results=None, page_token=None, older_than_days=None, date_range=None):
        """List emails from a specific domain with pagination"""
        try:
            if max_results is None: 
                max_results = self.default_max_results
            query = f"from:*@{domain}"
            if older_than_days:
                cutoff_date = (datetime.utcnow() - timedelta(days=int(older_than_days))).strftime('%Y/%m/%d')
                query = f"{query} before:{cutoff_date}"
            if date_range:
                try:
                    start_date, end_date = self._compute_date_range_window(date_range)
                    if start_date and end_date:
                        query = f"{query} after:{start_date.strftime('%Y/%m/%d')} before:{end_date.strftime('%Y/%m/%d')}"
                except Exception:
                    pass
            kwargs = {"userId": 'me', "q": query, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.service.users().messages().list(**kwargs).execute()
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')
            email_list = []
            for message in messages:
                msg = self.service.users().messages().get(
                    userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                    fields='payload/headers,id').execute()
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
                email_list.append({
                    'id': message['id'],
                    'subject': subject,
                    'sender': sender,
                    'snippet': ''
                })
            return {"emails": email_list, "next_page_token": next_token}
        except HttpError as error:
            return {"emails": [], "next_page_token": None}

    def list_emails_by_sender(self, sender_keyword, max_results=None, page_token=None, older_than_days=None, date_range=None):
        """List emails from a sender containing specific keyword with pagination."""
        try:
            if max_results is None:
                max_results = self.default_max_results
            query = f"from:{sender_keyword}"
            if older_than_days:
                cutoff_date = (datetime.utcnow() - timedelta(days=int(older_than_days))).strftime('%Y/%m/%d')
                query = f"{query} before:{cutoff_date}"
            if date_range:
                try:
                    start_date, end_date = self._compute_date_range_window(date_range)
                    if start_date and end_date:
                        query = f"{query} after:{start_date.strftime('%Y/%m/%d')} before:{end_date.strftime('%Y/%m/%d')}"
                except Exception:
                    pass
            kwargs = {"userId": 'me', "q": query, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.service.users().messages().list(**kwargs).execute()
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')
            email_list = []
            for message in messages:
                msg = self.service.users().messages().get(
                    userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                    fields='payload/headers,id').execute()
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
                email_list.append({
                    'id': message['id'],
                    'subject': subject,
                    'sender': sender,
                    'snippet': ''
                })
            return {"emails": email_list, "next_page_token": next_token}
        except HttpError as error:
            return {"emails": [], "next_page_token": None}

    def delete_emails_by_age_only(self, older_than_days, confirm=False):
        """Delete all emails older than specified days (bulk cleanup)."""
        try:
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=older_than_days)
            cutoff_str = cutoff_date.strftime('%Y/%m/%d')
            
            # Gmail query for all emails before cutoff date
            query = f"before:{cutoff_str}"
            
            # Get all matching messages with pagination
            all_messages = []
            next_page_token = None
            
            while True:
                if next_page_token:
                    results = self.service.users().messages().list(
                        userId='me', q=query, pageToken=next_page_token).execute()
                else:
                    results = self.service.users().messages().list(
                        userId='me', q=query).execute()
                messages = results.get('messages', [])
                all_messages.extend(messages)
                next_page_token = results.get('nextPageToken')
                if not next_page_token:
                    break

            if not all_messages:
                return {"status": "success", "message": _("No emails found older than %(days)d days") % {"days": older_than_days}, "deleted_count": 0}

            if not confirm:
                return {
                    "status": "confirmation_required",
                    "message": _("Found %(count)d emails older than %(days)d days. Do you want to move them to Trash?") % {"count": len(all_messages), "days": older_than_days},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {"action": "delete_by_age", "older_than_days": older_than_days}
                }

            # Process in batches for better performance
            batch_size = 100
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            
            for i in range(0, len(all_messages), batch_size):
                batch = all_messages[i:i + batch_size]
                message_ids_batch = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                        userId='me',
                        body={
                            'ids': message_ids_batch,
                            'addLabelIds': ['TRASH'],
                            'removeLabelIds': ['INBOX']
                        }
                    ).execute()
                    total_processed += len(batch)
                except HttpError as e:
                    for m in batch:
                        try:
                            self.service.users().messages().trash(userId='me', id=m['id']).execute()
                            total_processed += 1
                        except HttpError:
                            continue

            action_id = self._record_undo('trash', message_ids)
            return {"status": "success", "message": _("Trashed %(count)d emails older than %(days)d days") % {"count": total_processed, "days": older_than_days}, "deleted_count": total_processed, "undo_action_id": action_id}

        except HttpError as error:
            return {"status": "error", "message": f"Error deleting emails by age: {error}"}
    
    def archive_emails_by_age_only(self, older_than_days, confirm=False):
        """Archive all emails older than specified days (bulk cleanup)."""
        try:
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=older_than_days)
            cutoff_str = cutoff_date.strftime('%Y/%m/%d')
            
            # Gmail query for all emails before cutoff date
            query = f"before:{cutoff_str}"
            
            # Get all matching messages with pagination
            all_messages = []
            next_page_token = None
            
            while True:
                request_body = {'userId': 'me', 'q': query}
                if next_page_token:
                    request_body['pageToken'] = next_page_token
                
                results = self.service.users().messages().list(**request_body).execute()
                messages = results.get('messages', [])
                all_messages.extend(messages)
                
                next_page_token = results.get('nextPageToken')
                if not next_page_token:
                    break

            if not all_messages:
                return {"status": "success", "message": _("No emails found older than %(days)d days to archive.") % {"days": older_than_days}, "archived_count": 0}

            if not confirm:
                return {
                    "status": "confirmation_required",
                    "message": _("Found %(count)d emails older than %(days)d days. Do you want to archive them?") % {"count": len(all_messages), "days": older_than_days},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {"action": "archive_by_age", "older_than_days": older_than_days}
                }

            # Process in batches for better performance
            batch_size = 100
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            
            for i in range(0, len(all_messages), batch_size):
                batch = all_messages[i:i + batch_size]
                message_ids_batch = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                        userId='me',
                        body={
                            'ids': message_ids_batch,
                            'removeLabelIds': ['INBOX']
                        }
                    ).execute()
                    total_processed += len(batch)
                except HttpError as e:
                    # Fallback to individual calls for this batch
                    for msg_id in message_ids_batch:
                        try:
                            self.service.users().messages().modify(
                                userId='me', id=msg_id, body={'removeLabelIds': ['INBOX']}
                            ).execute()
                            total_processed += 1
                        except HttpError:
                            continue

            action_id = self._record_undo('archive', message_ids)
            return {"status": "success", "message": _("Archived %(count)d emails older than %(days)d days.") % {"count": total_processed, "days": older_than_days}, "archived_count": total_processed, "undo_action_id": action_id}

        except HttpError as error:
            return {"status": "error", "message": f"Error archiving emails by age: {error}"}
    
    def show_email_stats(self, full=False):
        """Show comprehensive email statistics.
        If full is True, analyze the entire mailbox (may take longer). Otherwise analyze a recent sample.
        """
        try:
            # Get total inbox count by actually counting all inbox emails
            print("Counting total inbox emails...")
            all_inbox_messages = []
            next_token = None
            while True:
                kwargs = {"userId": 'me', "q": "in:inbox", "maxResults": 500, "fields": 'nextPageToken,messages/id'}
                if next_token:
                    kwargs["pageToken"] = next_token
                
                # Retry on transient SSL errors without noisy logging
                page = None
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        page = self.service.users().messages().list(**kwargs).execute()
                        break
                    except Exception as e:
                        err_text = str(e)
                        if ("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text) and attempt < max_retries - 1:
                            time.sleep(1)
                            continue
                        # On final failure, abort listing gracefully
                        page = None
                        break
                if page is None:
                    # Stop counting further to avoid raising; proceed with what we have
                    break

                msgs = page.get('messages', [])
                if msgs:
                    all_inbox_messages.extend(msgs)
                next_token = page.get('nextPageToken')
                if not next_token:
                    break
            total_inbox = len(all_inbox_messages)
            print(f"Found {total_inbox} total inbox emails")
            
            # Collect message ids to analyze
            if full:
                # Try to estimate total messages for truncation note
                try:
                    estimate_resp = self.service.users().messages().list(userId='me', maxResults=1, fields='resultSizeEstimate').execute()
                    total_estimate = estimate_resp.get('resultSizeEstimate', 0)
                except HttpError:
                    total_estimate = 0
                all_messages = []
                next_token = None
                # Analyze INBOX only, up to a hard cap to prevent extremely long wait times
                max_to_analyze = 1000
                while True:
                    # Use 'in:inbox' query instead of labelIds for more comprehensive inbox coverage
                    kwargs = {"userId": 'me', "q": "in:inbox", "maxResults": 500, "fields": 'nextPageToken,messages/id'}
                    if next_token:
                        kwargs["pageToken"] = next_token
                    
                    # Retry on transient SSL errors without noisy logging
                    page = None
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            page = self.service.users().messages().list(**kwargs).execute()
                            break
                        except Exception as e:
                            err_text = str(e)
                            if ("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text) and attempt < max_retries - 1:
                                time.sleep(1)
                                continue
                            # On final failure, abort listing gracefully
                            page = None
                            break
                    
                    if page is None:
                        break
                        
                    msgs = page.get('messages', [])
                    if msgs:
                        all_messages.extend(msgs)
                        if len(all_messages) >= max_to_analyze:
                            all_messages = all_messages[:max_to_analyze]
                            next_token = None
                            break
                    next_token = page.get('nextPageToken')
                    if not next_token:
                        break
            else:
                # Get sample of messages for analysis
                sample_results = self.service.users().messages().list(
                    userId='me', q='in:inbox', maxResults=200, fields='messages/id').execute()
                sample_messages = sample_results.get('messages', [])
                
                # Use sample messages for analysis, total_inbox already calculated above
                all_messages = sample_messages
            
            if not all_messages:
                return {"error": _("No emails found for analysis")}
            
            # Analyze sender, domain, labels, hour of day, subject terms
            sender_counts = {}
            domain_counts = {}
            label_counts = {}
            hour_counts = [0]*24
            term_counts = {}
            stopwords = set(["re", "fw", "fwd", "the", "and", "for", "to", "of", "in", "on", "a", "an", "your", "you", "is", "are", "with", "from"]) 
            
            sample_ids = [m['id'] for m in all_messages] if full else [m['id'] for m in all_messages[:120]]  # limit to avoid rate limits
            
            # Use smart batch processing with Gmail's search API for much faster execution
            sender_counts = {}
            domain_counts = {}
            label_counts = {}
            hour_counts = [0]*24
            term_counts = {}
            
            start_time = time.time()
            
            # Use optimized sequential processing for maximum speed while analyzing ALL emails
            try:
                # Build the processing list directly from the chosen sample IDs
                batch_messages = [{'id': mid} for mid in sample_ids]
                if not batch_messages:
                    return {"error": _("No emails found for analysis")}
                
                # Process ALL emails sequentially with NO delays for maximum speed
                completed = 0
                
                # Set total emails for progress tracking
                if hasattr(self, 'current_command_id') and self.current_command_id:
                    from .views import update_email_progress
                    update_email_progress(self.current_command_id, 0, len(batch_messages))
                
                for i, msg in enumerate(batch_messages):
                    try:
                        # Progress update frequency: every 5 emails for regular stats, every 15 for full analysis
                        update_frequency = 15 if full else 5
                        if (i + 1) % update_frequency == 0:
                            # Update real progress for full analysis commands (no console spam)
                            if hasattr(self, 'current_command_id') and self.current_command_id:
                                from .views import update_email_progress
                                update_email_progress(self.current_command_id, i+1, len(batch_messages))
                        
                        # Retry on transient SSL errors for per-message fetch; skip on final failure
                        msg_data = None
                        max_retries = 3
                        for attempt in range(max_retries):
                            try:
                                msg_data = self.service.users().messages().get(
                                    userId='me', id=msg['id'],
                                    format='metadata', metadataHeaders=['From','Subject'],
                                    fields='id,internalDate,labelIds,payload/headers'
                                ).execute()
                                break
                            except Exception as e:
                                err_text = str(e)
                                if ("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text) and attempt < max_retries - 1:
                                    time.sleep(0.5)
                                    continue
                                # Skip this message on persistent failure
                                msg_data = None
                                break
                        if msg_data is None:
                            continue
                        
                        headers = msg_data.get('payload', {}).get('headers', [])
                        sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown')
                        subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), '')
                        
                        # Sender and domain
                        if '<' in sender and '>' in sender:
                            email_part = sender.split('<')[1].split('>')[0]
                        else:
                            email_part = sender
                        sender_counts[email_part] = sender_counts.get(email_part, 0) + 1
                        if '@' in email_part:
                            domain = email_part.split('@')[1]
                            domain_counts[domain] = domain_counts.get(domain, 0) + 1
                        
                        # Labels
                        for lid in msg_data.get('labelIds', []):
                            label_counts[lid] = label_counts.get(lid, 0) + 1
                        
                        # Hour of day (local time)
                        try:
                            ts_ms = int(msg_data.get('internalDate', '0'))
                            if ts_ms:
                                hour = datetime.fromtimestamp(ts_ms/1000).hour
                                hour_counts[hour] += 1
                        except Exception:
                            pass
                        
                        # Subject terms
                        # Extract subject terms with stricter noise filtering
                        for raw in re.split(r'[^A-Za-z0-9]+', subject.lower()):
                            if not raw:
                                continue
                            # Skip pure numbers
                            if raw.isdigit():
                                continue
                            # Skip very short tokens
                            if len(raw) <= 2:
                                continue
                            # Skip common reply/forward markers or noise
                            extra_stopwords = {
                                're', 'fw', 'fwd', 'no', 'noreply', 'do', 'not', 'auto', 'reply',
                                'ref', 'id', 'cid', 'msg', 'mail', 'email', 'update', 'notification',
                                'info', 'news', 'new', 'hi', 'hey'
                            }
                            if raw in stopwords or raw in extra_stopwords:
                                continue
                            # Skip hex-like or tracking-code-like strings
                            if re.fullmatch(r'[a-f0-9]{6,}', raw):
                                continue
                            # Skip tokens that are mostly digits
                            num_digits = sum(1 for ch in raw if ch.isdigit())
                            if num_digits / len(raw) >= 0.5:
                                continue
                            # Skip long mixed alphanumeric that likely represent codes
                            if len(raw) >= 12 and any(ch.isalpha() for ch in raw) and any(ch.isdigit() for ch in raw):
                                continue
                            term_counts[raw] = term_counts.get(raw, 0) + 1
                        
                        completed += 1
                        
                    except Exception as e:
                        print(f"Error processing message {msg['id']}: {e}")
                        continue
                
                # Sequential processing completed
                
            except Exception as e:
                # Sequential processing failed, falling back to original approach
                
                # Fallback to the original approach
                completed = 0
                for msg_id in sample_ids:
                    try:
                        msg = self.service.users().messages().get(
                            userId='me', id=msg_id,
                            format='metadata', metadataHeaders=['From','Subject'],
                            fields='id,internalDate,labelIds,payload/headers'
                        ).execute()
                        
                        headers = msg.get('payload', {}).get('headers', [])
                        sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown')
                        subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), '')
                        
                        # Process the data
                        if '<' in sender and '>' in sender:
                            email_part = sender.split('<')[1].split('>')[0]
                        else:
                            email_part = sender
                        sender_counts[email_part] = sender_counts.get(email_part, 0) + 1
                        if '@' in email_part:
                            domain = email_part.split('@')[1]
                            domain_counts[domain] = domain_counts.get(domain, 0) + 1
                        
                        for lid in msg.get('labelIds', []):
                            label_counts[lid] = label_counts.get(lid, 0) + 1
                        
                        try:
                            ts_ms = int(msg.get('internalDate', '0'))
                            if ts_ms:
                                hour = datetime.fromtimestamp(ts_ms/1000).hour
                                hour_counts[hour] += 1
                        except Exception:
                            pass
                        
                        for raw in re.split(r'[^A-Za-z0-9]+', subject.lower()):
                            if not raw or raw.isdigit():
                                continue
                            if raw in stopwords:
                                continue
                            if len(raw) <= 2:
                                continue
                            term_counts[raw] = term_counts.get(raw, 0) + 1
                        
                        completed += 1
                        
                    except Exception as e:
                        print(f"Error processing message {msg_id}: {e}")
                        continue
                
                # Fallback processing completed
            total_time = time.time() - start_time
            
            top_senders = sorted(sender_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            top_domains = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            top_terms = sorted(term_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            
            # Map label IDs to friendly names
            try:
                labels_resp = self.service.users().labels().list(userId='me').execute()
                id_to_name = {l['id']: l['name'] for l in labels_resp.get('labels', [])}
            except HttpError:
                id_to_name = {}
            def human_label(name):
                if not name:
                    return _('Unknown')
                mapping = {
                    'INBOX':_('Inbox'),'SENT':_('Sent'),'STARRED':_('Starred'),'IMPORTANT':_('Important'),'DRAFT':_('Drafts'),'TRASH':_('Trash'),'SPAM':_('Spam'),'UNREAD':_('Unread'),
                    'CATEGORY_PERSONAL':_('Personal'),'CATEGORY_SOCIAL':_('Social'),'CATEGORY_PROMOTIONS':_('Promotions'),'CATEGORY_UPDATES':_('Updates'),'CATEGORY_FORUMS':_('Forums')
                }
                if name in mapping:
                    return mapping[name]
                if name.startswith('CATEGORY_'):
                    return _(name.split('CATEGORY_')[1].title())
                return name
            # Build display list sorted by count desc
            label_counts_display = []
            for lid, count in label_counts.items():
                pretty = human_label(id_to_name.get(lid, lid))
                label_counts_display.append((pretty, count))
            label_counts_display.sort(key=lambda x: x[1], reverse=True)

            insights = []
            if top_senders:
                insights.append(_("Most active sender: %(sender)s (%(count)d emails)") % {"sender": top_senders[0][0], "count": top_senders[0][1]})
            if top_domains:
                insights.append(_("Most active domain: %(domain)s (%(count)d emails)") % {"domain": top_domains[0][0], "count": top_domains[0][1]})
            total_analyzed = sum(sender_counts.values())
            if total_analyzed > 0 and top_senders:
                percentage = (top_senders[0][1] / total_analyzed) * 100
                insights.append(_("Top sender accounts for %(pct).1f%% of analyzed emails") % {"pct": percentage})

            # Compute maxima for bar visualizations
            max_hour_count = max(hour_counts) if hour_counts else 0
            max_label_count = max([c for _, c in label_counts_display]) if label_counts_display else 0

            # Build a color map for labels (friendly names)
            predefined_colors = {
                'Inbox': '#1a73e8',
                'Promotions': '#34a853',
                'Social': '#1da1f2',
                'Updates': '#fbbc05',
                'Forums': '#a142f4',
                'Sent': '#5f6368',
                'Starred': '#f4b400',
                'Important': '#ea8600',
                'Drafts': '#9aa0a6',
                'Trash': '#9aa0a6',
                'Spam': '#ea4335',
                'Unread': '#00acc1',
                'Personal': '#607d8b'
            }

            def hsl_to_hex(h: float, s: float, l: float) -> str:
                c = (1 - abs(2 * l - 1)) * s
                x = c * (1 - abs(((h / 60.0) % 2) - 1))
                m = l - c / 2
                if 0 <= h < 60:
                    r1, g1, b1 = c, x, 0
                elif 60 <= h < 120:
                    r1, g1, b1 = x, c, 0
                elif 120 <= h < 180:
                    r1, g1, b1 = 0, c, x
                elif 180 <= h < 240:
                    r1, g1, b1 = 0, x, c
                elif 240 <= h < 300:
                    r1, g1, b1 = x, 0, c
                else:
                    r1, g1, b1 = c, 0, x
                r = int(round((r1 + m) * 255))
                g = int(round((g1 + m) * 255))
                b = int(round((b1 + m) * 255))
                return f"#{r:02x}{g:02x}{b:02x}"

            def color_for_label(label_name: str) -> str:
                friendly = label_name or 'Label'
                if friendly in predefined_colors:
                    return predefined_colors[friendly]
                # Deterministic hue from md5 hash
                digest = hashlib.md5(friendly.lower().encode('utf-8')).hexdigest()
                hue = int(digest[:2], 16) / 255.0 * 360.0  # 0..360
                sat = 0.60
                lig = 0.55
                return hsl_to_hex(hue, sat, lig)

            label_color_map = {name: color_for_label(name) for name, _ in label_counts_display}

            # Precompute bar models for template simplicity
            hour_bars = []
            for idx, cnt in enumerate(hour_counts):
                pct = int(round((cnt * 100 / max_hour_count))) if max_hour_count else 0
                hour_bars.append({"hour": idx, "count": cnt, "pct": pct})

            label_bars = []
            for name, cnt in label_counts_display:
                pct = int(round((cnt * 100 / max_label_count))) if max_label_count else 0
                label_bars.append({"name": name, "count": cnt, "pct": pct, "color": label_color_map.get(name, '#4a90e2')})

            # Always show the actual total inbox count, regardless of analysis scope
            final_total_inbox = total_inbox
            
            # Check if full analysis was limited due to the 1000 email cap
            analysis_limited = full and len(all_messages) >= 1000 and total_inbox > 1000
            
            return {
                "total_inbox": final_total_inbox,
                "sample_analyzed": completed,
                "analysis_scope": "full" if full else "sample",
                "analysis_truncated": True if full and total_estimate and completed < total_estimate else False,
                "analysis_total_estimate": total_estimate if full else None,
                "analysis_limited": analysis_limited,
                "analysis_limit_note": _("Analysis limited to most recent 1000 emails (out of %(total)d total) to ensure reasonable processing time") % {"total": total_inbox} if analysis_limited else None,
                "top_senders": top_senders,
                "top_domains": top_domains,
                "top_subject_terms": top_terms,
                "label_counts": label_counts,
                "label_counts_display": label_counts_display,
                "emails_by_hour": hour_counts,
                "insights": insights,
                "max_hour_count": max_hour_count,
                "max_label_count": max_label_count,
                "label_color_map": label_color_map,
                "hour_bars": hour_bars,
                "label_bars": label_bars
            }
            
        except HttpError as error:
            return {"error": f"Error getting email stats: {error}"}

    def list_emails_by_date_range(self, date_range_str, max_results=None, page_token=None):
        """
        Lists emails from a specific date range with intelligent window sizing.
        """
        today = datetime.now()
        if max_results is None:
            max_results = self.default_max_results
        start_date, end_date = None, None
        description = ""
        # Simple, predefined ranges
        if date_range_str == "today":
            start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
            description = _("for today")
        elif date_range_str == "yesterday":
            start_date = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
            description = _("for yesterday")
        elif date_range_str == "last week":
            start_of_this_week = today - timedelta(days=today.weekday())
            start_date = (start_of_this_week - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=7)
            description = _("from last week")
        elif date_range_str in ["last month", "last year"]:
            # Handle "last month" and "last year" specifically
            if date_range_str == "last month":
                # Inclusive last month: from the first day of last month to the first day of this month (exclusive)
                start_date = (today.replace(day=1) - relativedelta(months=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = today.replace(day=1).replace(hour=0, minute=0, second=0, microsecond=0)
                description = _("from last month")
            elif date_range_str == "last year":
                # Inclusive last year: from Jan 1 last year to Jan 1 this year (exclusive)
                start_date = today.replace(month=1, day=1) - relativedelta(years=1)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = today.replace(month=1, day=1).replace(hour=0, minute=0, second=0, microsecond=0)
                description = _("from last year")
        else:
            parts = date_range_str.split()
            if len(parts) == 2 and parts[0].isdigit():
                quantity = int(parts[0])
                unit = parts[1]
                delta = None
                if "day" in unit:
                    delta = timedelta(days=quantity)
                elif "week" in unit:
                    delta = timedelta(weeks=quantity)
                elif "month" in unit:
                    delta = relativedelta(months=quantity)
                elif "year" in unit:
                    delta = relativedelta(years=quantity)
                if delta:
                    start_date = (today - delta)
                    if "year" in unit:
                        end_date = start_date + relativedelta(months=3)
                        description = _("in the 3 months starting around %(date)s") % {"date": start_date.strftime('%Y/%m/%d')}
                        max_results = 20
                    elif "month" in unit:
                        end_date = start_date + relativedelta(months=1)
                    elif "week" in unit:
                        end_date = start_date + timedelta(days=7)
                    elif "day" in unit:
                        end_date = start_date + timedelta(days=1)

        if not start_date or not end_date:
            return {"error": f"Could not determine date range for '{date_range_str}'"}
        query = f"after:{start_date.strftime('%Y/%m/%d')} before:{end_date.strftime('%Y/%m/%d')}"
        result = self._execute_list_query(query, description, max_results, page_token=page_token)
        # pass through next page token
        return result

    def list_emails_older_than(self, older_than_str, max_results=None, page_token=None):
        """
        Lists emails older than a certain duration (e.g., '1 year', '6 months').
        """
        if max_results is None:
            max_results = self.default_max_results
        parts = older_than_str.split()
        if len(parts) == 2 and parts[0].isdigit():
            quantity = int(parts[0])
            unit = parts[1]
            delta = None
            if "day" in unit: delta = timedelta(days=quantity)
            elif "week" in unit: delta = timedelta(weeks=quantity)
            elif "month" in unit: delta = relativedelta(months=quantity)
            elif "year" in unit: delta = relativedelta(years=quantity)
            if delta:
                cutoff_date = datetime.now() - delta
                query = f"before:{cutoff_date.strftime('%Y/%m/%d')}"
                try:
                    days_equiv = (datetime.now() - cutoff_date).days
                except Exception:
                    days_equiv = quantity * (30 if 'month' in unit else 7 if 'week' in unit else 365 if 'year' in unit else 1)
                desc = _("older than %(days)d days") % {"days": days_equiv}
                return self._execute_list_query(query, desc, max_results, page_token=page_token)
            else:
                return {"error": f"Unknown unit in 'older than' command: {unit}"}
        else:
            return {"error": f"Invalid format for 'older than' command: {older_than_str}"}

    def _execute_list_query(self, query, description, max_results=None, page_token=None):
        """A helper function to execute list queries and print results."""
        try:
            if max_results is None:
                max_results = self.default_max_results
            kwargs = {"userId":"me", "q": query, "maxResults": max_results}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.service.users().messages().list(**kwargs).execute()
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')
            if not messages:
                return {"message": _("No emails found %(description)s.") % {"description": description}, "emails": [], "next_page_token": None}
            email_list = []
            for message in messages:
                msg = self.service.users().messages().get(
                    userId='me', id=message['id'], format='metadata', metadataHeaders=['From', 'Subject']).execute()
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
                email_list.append({"sender": sender, "subject": subject})
            return {"message": _("Found %(count)d emails %(description)s.") % {"count": len(messages), "description": description}, "emails": email_list, "next_page_token": next_token}
        except HttpError as error:
            return {"error": f"Error executing search query: {error}"}

    def _build_preview(self, messages, limit=10):
        """Build up to 'limit' preview rows for confirmation dialogs."""
        preview_items = []
        try:
            subset = messages[:limit] if isinstance(messages, list) else []
            for m in subset:
                try:
                    msg = self.service.users().messages().get(
                        userId='me', id=m['id'], format='metadata', metadataHeaders=['From','Subject'],
                        fields='id,internalDate,snippet,payload/headers'
                    ).execute()
                    headers = msg.get('payload', {}).get('headers', [])
                    subject = next((h.get('value') for h in headers if h.get('name') == 'Subject'), 'No Subject')
                    sender = next((h.get('value') for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                    date_str = ''
                    try:
                        ts_ms = int(msg.get('internalDate', '0') or '0')
                        if ts_ms:
                            date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                    except Exception:
                        date_str = ''
                    preview_items.append({
                        'id': m['id'],
                        'sender': sender,
                        'subject': subject,
                        'date': date_str,
                        'snippet': msg.get('snippet', '')
                    })
                except HttpError:
                    continue
        except Exception:
            pass
        return preview_items

    def _build_custom_category_q(self, category_key, older_than_days=None):
        """Return a Gmail search query string for a custom category key."""
        category_key = (category_key or '').lower()
        if category_key == 'verification_codes':
            base = 'in:anywhere -category:promotions -category:social -category:forums'
            subject_terms = [
                '"verification code"', '"one time code"', '"one-time code"', 'OTP', '"2FA"', '"two-factor"',
                '"2-step"', '"security code"', '"login code"', 'passcode', '"authentication code"',
                '"sign-in code"', '"sign in code"', '"your verification code"',
                # Hebrew and other locales
                '"קוד אימות"', '"סיסמה חד-פעמית"', '验证码', '"код подтверждения"', '"code de vérification"', '"código de verificación"'
            ]
            positive = 'subject:(' + ' OR '.join(subject_terms) + ')'
            negative = '-subject:(promo OR promotion OR promotional OR coupon OR discount OR voucher OR offer OR sale)'
            q = f"{base} ( {positive} ) {negative}"
        elif category_key == 'shipping_delivery':
            # Search across All Mail (exclude obvious ad categories)
            base = 'in:anywhere -category:promotions -category:social -category:forums'
            # Positive shipping subject phrases (strict), including Hebrew variants
            subject_core = 'subject:("your order has shipped" OR "order shipped" OR "has shipped" OR "out for delivery" OR "tracking number" OR "track your package" OR "in transit" OR "estimated delivery" OR "delivery estimate" OR "expected delivery" OR "ready for pickup" OR "ready for collection" OR "משלוח" OR "נשלחה" OR "נשלח" OR "מספר מעקב" OR "בהובלה" OR "נמסרה" OR "בהגעה" OR "איסוף" OR "מוכן לאיסוף")'
            # Require context when using broader words
            arrive_clause = '(subject:(arriving OR arrives) subject:(order OR package OR delivery OR parcel))'
            delivered_clause = '(subject:delivered subject:(order OR package OR delivery OR parcel))'
            # Known carriers (kept as a supplement, not brand ads)
            carrier_domains = ['ups.com','fedex.com','dhl.com','usps.com','canadapost.ca','royalmail.com','israelpost.co.il','17track.net','aftership.com','cainiao.com']
            carriers_clause = '(' + ' OR '.join([f'from:*@{d}' for d in carrier_domains]) + ')'
            # Negatives to drop ads
            negative = '-subject:(sale OR discount OR coupon OR promo OR promotional OR offer OR deal OR wishlist OR cart OR recommendations OR recommendation)'
            q = f"{base} ( {subject_core} OR {arrive_clause} OR {delivered_clause} OR {carriers_clause} ) {negative}"
        elif category_key == 'account_security':
            # Security/account alerts across All Mail; match strict subject phrases and exclude common noise
            base = 'in:anywhere -category:promotions -category:social -category:forums'
            subjects = [
                '"security alert"', '"security notification"', '"account security"',
                '"new sign in"', '"new sign-in"', '"new login"', '"sign-in attempt"', '"login attempt"',
                '"suspicious activity"', '"unusual activity"', '"verify it\'s you"', '"verify your identity"',
                '"password changed"', '"password reset"', '"reset your password"',
                '"2-step verification"', '"two-step verification"', '"two-factor authentication"'
            ]
            subject_clause = 'subject:(' + ' OR '.join(subjects) + ')'
            # Exclude purchases and school-related subjects that can cause false positives
            negative = '-subject:(sale OR discount OR coupon OR promo OR promotional OR offer OR deal OR newsletter OR receipt OR order OR purchase OR invoice OR payment OR transaction OR confirmation OR classroom OR course OR class OR assignment OR homework OR grade OR exam OR university OR school OR student OR tuition)'
            q = f"{base} {subject_clause} {negative}"
        else:
            q = ''
        if older_than_days:
            try:
                cutoff_date = (datetime.utcnow() - timedelta(days=int(older_than_days))).strftime('%Y/%m/%d')
                q = (q + f' before:{cutoff_date}').strip()
            except Exception:
                pass
        return q

    def _pretty_category_name(self, category_key: str) -> str:
        """Return a localized, human-friendly name for a custom category key."""
        key = (category_key or '').lower()
        mapping = {
            'verification_codes': _('verification codes'),
            'shipping_delivery': _('shipping emails'),
            'account_security': _('account security emails'),
        }
        return mapping.get(key, key.replace('_', ' '))

    def _pretty_gmail_label_or_category(self, raw: str) -> str:
        """Return a localized human name for Gmail system labels/categories and common aliases."""
        token = (raw or '').upper()
        alias = {
            'PROMOTIONS': 'CATEGORY_PROMOTIONS',
            'SOCIAL': 'CATEGORY_SOCIAL',
            'UPDATES': 'CATEGORY_UPDATES',
            'FORUMS': 'CATEGORY_FORUMS',
            'PERSONAL': 'CATEGORY_PERSONAL',
        }.get(token, token)
        mapping = {
            'INBOX': _('Inbox'),
            'SENT': _('Sent'),
            'STARRED': _('Starred'),
            'IMPORTANT': _('Important'),
            'DRAFT': _('Drafts'),
            'TRASH': _('Trash'),
            'SPAM': _('Spam'),
            'UNREAD': _('Unread'),
            'CATEGORY_PERSONAL': _('Personal'),
            'CATEGORY_SOCIAL': _('Social'),
            'CATEGORY_PROMOTIONS': _('Promotions'),
            'CATEGORY_UPDATES': _('Updates'),
            'CATEGORY_FORUMS': _('Forums'),
        }
        return mapping.get(alias, raw)

    def _compute_date_range_window(self, date_range_str):
        """Compute (start_date, end_date) datetimes for simple phrases like 'last month', 'last week', etc."""
        if not date_range_str:
            return None, None
        today = datetime.now()
        start_date, end_date = None, None
        key = date_range_str.strip().lower()
        if key == "today":
            start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
        elif key == "yesterday":
            start_date = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
        elif key == "last week":
            start_of_this_week = today - timedelta(days=today.weekday())
            start_date = (start_of_this_week - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=7)
        elif key == "last month":
            # First day of current month is the exclusive upper bound
            end_date = today.replace(day=1)
            # First day of last month as inclusive lower bound
            start_date = (end_date - relativedelta(months=1))
        elif key == "last year":
            # Jan 1 of current year as exclusive upper bound
            end_date = today.replace(month=1, day=1)
            # Jan 1 of last year as inclusive lower bound
            start_date = end_date - relativedelta(years=1)
        else:
            # from N units ago (we will treat it as a 1-unit window)
            parts = key.split()
            if len(parts) == 3 and parts[0].isdigit() and parts[2] == 'ago':
                qty = int(parts[0])
                unit = parts[1]
                if 'day' in unit:
                    start_date = today - timedelta(days=qty)
                    end_date = start_date + timedelta(days=1)
                elif 'week' in unit:
                    start_date = today - timedelta(weeks=qty)
                    end_date = start_date + timedelta(days=7)
                elif 'month' in unit:
                    start_date = today - relativedelta(months=qty)
                    end_date = start_date + relativedelta(months=1)
                elif 'year' in unit:
                    start_date = today - relativedelta(years=qty)
                    end_date = start_date + relativedelta(months=3)
        return start_date, end_date

    def list_emails_by_custom_category(self, category_key, max_results=None, older_than_days=None, page_token=None, date_range=None):
        """List emails matching a custom category definition with pagination."""
        try:
            if max_results is None:
                max_results = self.default_max_results
            q = self._build_custom_category_q(category_key, older_than_days=older_than_days)
            # Optional explicit date window for 'לפני ...' (mapped to 'from N <unit> ago')
            if date_range:
                try:
                    start_dt, end_dt = self._compute_date_range_window(f"{date_range} ago")
                    if start_dt and end_dt:
                        q = f"({q}) after:{start_dt.strftime('%Y/%m/%d')} before:{end_dt.strftime('%Y/%m/%d')}"
                except Exception:
                    pass
            if not q:
                return {"emails": [], "next_page_token": None}
            kwargs = {"userId": 'me', "q": q, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.service.users().messages().list(**kwargs).execute()
            messages = results.get('messages', []) or []
            next_token = results.get('nextPageToken')
            email_list = []
            for message in messages:
                msg = self.service.users().messages().get(
                    userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                    fields='payload/headers,id,internalDate,snippet'
                ).execute()
                headers = msg.get('payload', {}).get('headers', [])
                subject = next((h.get('value') for h in headers if h.get('name') == 'Subject'), 'No Subject')
                sender = next((h.get('value') for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                email_list.append({'id': message['id'], 'subject': subject, 'sender': sender, 'snippet': msg.get('snippet','')})
            return {"emails": email_list, "next_page_token": next_token}
        except HttpError:
            return {"emails": [], "next_page_token": None}

    def delete_emails_by_custom_category(self, category_key, confirm=False, older_than_days=None):
        """Delete (trash) messages matching a custom category definition."""
        try:
            q = self._build_custom_category_q(category_key, older_than_days=older_than_days)
            if not q:
                return {"status": "error", "message": "Unknown category."}
            all_messages = []
            page_token = None
            while True:
                kwargs = {"userId": 'me', "q": q, "maxResults": 500}
                if page_token:
                    kwargs["pageToken"] = page_token
                results = self.service.users().messages().list(**kwargs).execute()
                msgs = results.get('messages', []) or []
                all_messages.extend(msgs)
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            if not all_messages:
                return {"status": "success", "message": _("No emails found for %(what)s.") % {"what": self._pretty_category_name(category_key)}, "deleted_count": 0}
            if not confirm:
                age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                return {
                    "status": "confirmation_required",
                    "message": _("Found %(count)d emails in %(what)s%(age)s. Do you want to move them to Trash?") % {"count": len(all_messages), "what": self._pretty_category_name(category_key), "age": age_txt},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {"action": "delete_by_custom_category", "category_key": category_key, "older_than_days": older_than_days}
                }
            # Trash in batches
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            for i in range(0, len(all_messages), 100):
                batch = all_messages[i:i+100]
                batch_ids = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                        userId='me', body={'ids': batch_ids, 'addLabelIds': ['TRASH'], 'removeLabelIds': ['INBOX']}
                    ).execute()
                    total_processed += len(batch)
                except HttpError:
                    for m in batch:
                        try:
                            self.service.users().messages().trash(userId='me', id=m['id']).execute()
                            total_processed += 1
                        except HttpError:
                            continue
            action_id = self._record_undo('trash', message_ids)
            return {"status": "success", "message": _("Trashed %(count)d emails from %(what)s.") % {"count": total_processed, "what": self._pretty_category_name(category_key)}, "deleted_count": total_processed, "undo_action_id": action_id}
        except HttpError as error:
            return {"status": "error", "message": f"Error deleting by category: {error}"}

    def archive_emails_by_custom_category(self, category_key, confirm=False, older_than_days=None):
        """Archive messages matching a custom category definition."""
        try:
            q = self._build_custom_category_q(category_key, older_than_days=older_than_days)
            if not q:
                return {"status": "error", "message": "Unknown category."}
            all_messages = []
            page_token = None
            while True:
                kwargs = {"userId": 'me', "q": q, "maxResults": 500}
                if page_token:
                    kwargs["pageToken"] = page_token
                results = self.service.users().messages().list(**kwargs).execute()
                msgs = results.get('messages', []) or []
                all_messages.extend(msgs)
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            if not all_messages:
                return {"status": "success", "message": _("No emails found for %(what)s.") % {"what": self._pretty_category_name(category_key)}, "archived_count": 0}
            if not confirm:
                age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                return {
                    "status": "confirmation_required",
                    "message": _("Found %(count)d emails in %(what)s%(age)s. Do you want to archive them?") % {"count": len(all_messages), "what": self._pretty_category_name(category_key), "age": age_txt},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {"action": "archive_by_custom_category", "category_key": category_key, "older_than_days": older_than_days}
                }
            # Archive in batches
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            for i in range(0, len(all_messages), 100):
                batch = all_messages[i:i+100]
                batch_ids = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                        userId='me', body={'ids': batch_ids, 'removeLabelIds': ['INBOX']}
                    ).execute()
                    total_processed += len(batch)
                except HttpError:
                    for m in batch:
                        try:
                            self.service.users().messages().modify(userId='me', id=m['id'], body={'removeLabelIds': ['INBOX']}).execute()
                            total_processed += 1
                        except HttpError:
                            continue
            action_id = self._record_undo('archive', message_ids)
            return {"status": "success", "message": _("Archived %(count)d emails from %(what)s.") % {"count": total_processed, "what": self._pretty_category_name(category_key)}, "archived_count": total_processed, "undo_action_id": action_id}
        except HttpError as error:
            return {"status": "error", "message": f"Error archiving by category: {error}"}

    def search_emails_by_subject(self, search_term, max_results=50):
        """Search for emails with a specific term in the subject."""
        try:
            query = f"subject:({search_term})"
            results = self.service.users().messages().list(
                userId='me', q=query, maxResults=max_results).execute()
            messages = results.get('messages', [])

            if not messages:
                return []

            email_list = []
            for message in messages:
                msg = self.service.users().messages().get(
                    userId='me', id=message['id'], format='metadata', metadataHeaders=['From', 'Subject']).execute()
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
                email_list.append({'id': message['id'], 'sender': sender, 'subject': subject})
                
            return email_list

        except HttpError as error:
            print(f"❌ Error searching emails by subject: {error}")
            return []

    def delete_emails_by_sender(self, sender_email, confirm=False, older_than_days=None):
        """Delete emails from a specific sender by moving them to Trash.
        Optionally filter by age using older_than_days.
        """
        try:
            # Search for emails from the sender
            query_parts = [f"from:{sender_email}"]
            if older_than_days:
                from datetime import datetime, timedelta
                cutoff_date = (datetime.utcnow() - timedelta(days=older_than_days)).strftime('%Y/%m/%d')
                query_parts.append(f"before:{cutoff_date}")
            query = " ".join(query_parts)
            results = self.service.users().messages().list(
                userId='me', q=query, maxResults=500).execute()
            messages = results.get('messages', [])
            page_token = results.get('nextPageToken')
            all_messages = []
            if messages:
                all_messages.extend(messages)
            while page_token:
                results = self.service.users().messages().list(
                    userId='me', q=query, maxResults=500, pageToken=page_token).execute()
                messages = results.get('messages', [])
                if messages:
                    all_messages.extend(messages)
                page_token = results.get('nextPageToken')
            
            if not all_messages:
                age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                return {"status": "success", "message": _("No emails found from %(sender)s%(age)s.") % {"sender": sender_email, "age": age_txt}, "deleted_count": 0}
            
            if not confirm:
                age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                return {
                    "status": "confirmation_required",
                    "message": _("Found %(count)d emails from %(sender)s%(age)s. Do you want to move them to Trash?") % {"count": len(all_messages), "sender": sender_email, "age": age_txt},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {
                        "action": "delete_by_sender", 
                        "sender": sender_email, 
                        "older_than_days": older_than_days
                    }
                }
            
            # Process in batches for better performance
            batch_size = 100
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            
            for i in range(0, len(all_messages), batch_size):
                batch = all_messages[i:i + batch_size]
                message_ids_batch = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                        userId='me',
                        body={
                            'ids': message_ids_batch,
                            'addLabelIds': ['TRASH'],
                            'removeLabelIds': ['INBOX']
                        }
                    ).execute()
                    total_processed += len(batch)
                except HttpError as e:
                    for message in batch:
                        try:
                            self.service.users().messages().trash(userId='me', id=message['id']).execute()
                            total_processed += 1
                        except HttpError:
                            continue
            
            action_id = self._record_undo('trash', message_ids)
            return {"status": "success", "message": _("Trashed %(count)d emails from %(sender)s.") % {"count": total_processed, "sender": sender_email}, "deleted_count": total_processed, "undo_action_id": action_id}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error deleting emails: {error}"}
    
    def delete_emails_by_domain(self, domain, confirm=False, older_than_days=None):
        """Delete emails from a specific domain by moving them to Trash.
        Optionally filter by age using older_than_days.
        """
        try:
            query_parts = [f"from:*@{domain}"]
            if older_than_days:
                from datetime import datetime, timedelta
                cutoff_date = (datetime.utcnow() - timedelta(days=older_than_days)).strftime('%Y/%m/%d')
                query_parts.append(f"before:{cutoff_date}")
            query = " ".join(query_parts)
            results = self.service.users().messages().list(
                userId='me', q=query, maxResults=500).execute()
            messages = results.get('messages', [])
            page_token = results.get('nextPageToken')
            all_messages = []
            if messages:
                all_messages.extend(messages)
            while page_token:
                results = self.service.users().messages().list(
                    userId='me', q=query, maxResults=500, pageToken=page_token).execute()
                messages = results.get('messages', [])
                if messages:
                    all_messages.extend(messages)
                page_token = results.get('nextPageToken')
            
            if not all_messages:
                age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                return {"status": "success", "message": _("No emails found from domain: %(domain)s%(age)s.") % {"domain": domain, "age": age_txt}, "deleted_count": 0}
            
            if not confirm:
                age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                return {
                    "status": "confirmation_required",
                    "message": _("Found %(count)d emails from domain %(domain)s%(age)s. Do you want to move them to Trash?") % {"count": len(all_messages), "domain": domain, "age": age_txt},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {
                        "action": "delete_by_domain", 
                        "domain": domain, 
                        "older_than_days": older_than_days
                    }
                }
            
            # Process in batches for better performance
            batch_size = 100
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            
            for i in range(0, len(all_messages), batch_size):
                batch = all_messages[i:i + batch_size]
                message_ids_batch = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                        userId='me',
                        body={
                            'ids': message_ids_batch,
                            'addLabelIds': ['TRASH'],
                            'removeLabelIds': ['INBOX']
                        }
                    ).execute()
                    total_processed += len(batch)
                except HttpError as e:
                    for message in batch:
                        try:
                            self.service.users().messages().trash(userId='me', id=message['id']).execute()
                            total_processed += 1
                        except HttpError:
                            continue

            action_id = self._record_undo('trash', message_ids)
            return {"status": "success", "message": _("Trashed %(count)d emails from %(domain)s.") % {"count": total_processed, "domain": domain}, "deleted_count": total_processed, "undo_action_id": action_id}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error deleting emails: {error}"}
    
    def delete_emails_by_subject_keywords(self, keywords, confirm=False, older_than_days=None):
        """Delete emails containing specific keywords in subject by moving them to Trash.
        Optionally filter by age using older_than_days.
        """
        try:
            keyword_query = " OR ".join([f'subject:"{keyword}"' for keyword in keywords])
            query = f"({keyword_query})"
            if older_than_days:
                from datetime import datetime, timedelta
                cutoff_date = (datetime.utcnow() - timedelta(days=older_than_days)).strftime('%Y/%m/%d')
                query = f"{query} before:{cutoff_date}"
            results = self.service.users().messages().list(
                userId='me', q=query, maxResults=500).execute()
            messages = results.get('messages', [])
            page_token = results.get('nextPageToken')
            all_messages = []
            if messages:
                all_messages.extend(messages)
            while page_token:
                results = self.service.users().messages().list(
                    userId='me', q=query, maxResults=500, pageToken=page_token).execute()
                messages = results.get('messages', [])
                if messages:
                    all_messages.extend(messages)
                page_token = results.get('nextPageToken')
            
            if not all_messages:
                return {"status": "success", "message": f"No emails found with keywords: {', '.join(keywords)}", "deleted_count": 0}
            
            if not confirm:
                return {
                    "status": "confirmation_required",
                    "message": f"Found {len(all_messages)} emails with keywords: {', '.join(keywords)}. Do you want to move them to Trash?",
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {
                        "action": "delete_by_subject", 
                        "keywords": keywords, 
                        "older_than_days": older_than_days
                    }
                }
            
            # Process in batches for better performance
            batch_size = 100
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            for i in range(0, len(all_messages), batch_size):
                batch = all_messages[i:i + batch_size]
                message_ids_batch = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                        userId='me',
                        body={
                            'ids': message_ids_batch,
                            'addLabelIds': ['TRASH'],
                            'removeLabelIds': ['INBOX']
                        }
                    ).execute()
                    total_processed += len(batch)
                except HttpError as e:
                    for message in batch:
                        try:
                            self.service.users().messages().trash(userId='me', id=message['id']).execute()
                            total_processed += 1
                        except HttpError:
                            continue

            action_id = self._record_undo('trash', message_ids)
            return {"status": "success", "message": f"Trashed {total_processed} emails with keywords: {', '.join(keywords)}", "deleted_count": total_processed, "undo_action_id": action_id}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error deleting emails: {error}"}

    def delete_emails_by_category(self, category_id, confirm=False, older_than_days=None):
        """Delete emails by Gmail category (e.g., CATEGORY_PROMOTIONS, CATEGORY_SOCIAL)."""
        try:
            query_parts = [f"category:{category_id}"]
            if older_than_days:
                from datetime import datetime, timedelta
                cutoff_date = (datetime.utcnow() - timedelta(days=older_than_days)).strftime('%Y/%m/%d')
                query_parts.append(f"before:{cutoff_date}")
            query = " ".join(query_parts)
            results = self.service.users().messages().list(
                userId='me', q=query, maxResults=500).execute()
            messages = results.get('messages', [])
            page_token = results.get('nextPageToken')
            all_messages = []
            if messages:
                all_messages.extend(messages)
            while page_token:
                results = self.service.users().messages().list(
                    userId='me', q=query, maxResults=500, pageToken=page_token).execute()
                messages = results.get('messages', [])
                if messages:
                    all_messages.extend(messages)
                page_token = results.get('nextPageToken')

            if not all_messages:
                pretty_cat = self._pretty_gmail_label_or_category(category_id)
                return {"status": "success", "message": _("No emails found in category: %(category)s.") % {"category": pretty_cat}, "deleted_count": 0}

            if not confirm:
                age_text = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                pretty_cat = self._pretty_gmail_label_or_category(category_id)
                return {
                    "status": "confirmation_required",
                    "message": _("Found %(count)d emails in %(what)s%(age)s. Do you want to move them to Trash?") % {"count": len(all_messages), "what": pretty_cat, "age": age_text},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {
                        "action": "delete_by_category",
                        "category": category_id,
                        "older_than_days": older_than_days
                    }
                }

            # Process in batches for better performance
            batch_size = 100
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            for i in range(0, len(all_messages), batch_size):
                batch = all_messages[i:i + batch_size]
                message_ids_batch = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                        userId='me',
                        body={
                            'ids': message_ids_batch,
                            'addLabelIds': ['TRASH'],
                            'removeLabelIds': ['INBOX']
                        }
                    ).execute()
                    total_processed += len(batch)
                except HttpError as e:
                    for message in batch:
                        try:
                            self.service.users().messages().trash(
                                userId='me', id=message['id']).execute()
                            total_processed += 1
                        except HttpError:
                            continue

            action_id = self._record_undo('trash', message_ids)
            pretty_cat = self._pretty_gmail_label_or_category(category_id)
            return {"status": "success", "message": _("Trashed %(count)d emails from %(what)s.") % {"count": total_processed, "what": pretty_cat}, "deleted_count": total_processed, "undo_action_id": action_id}

        except HttpError as error:
            return {"status": "error", "message": f"Error deleting by category: {error}"}


    
    def parse_command_with_ai(self, command):
        """Use AI to parse natural language commands"""
        if not self.gemini_model:
            return None
        
        try:
            prompt = f"""
            Analyze this email management command and extract the action and target:
            Command: "{command}"
            
            Return ONLY a JSON object with:
            - action: "delete", "list", "archive", "mark_read"
            - target_type: "sender", "domain", "subject_keywords", "recent"
            - target: the specific target (email, domain, keywords array, or "recent")
            - confirmation_required: true/false
            
            Examples:
            - "delete all amazon ads" → {{"action": "delete", "target_type": "subject_keywords", "target": ["amazon", "ad", "ads"], "confirmation_required": true}}
            - "delete emails from amazon.com" → {{"action": "delete", "target_type": "domain", "target": "amazon.com", "confirmation_required": true}}
            - "list recent emails" → {{"action": "list", "target_type": "recent", "target": "recent", "confirmation_required": false}}
            """
            
            response = self.gemini_model.generate_content(prompt)
            # Try to extract JSON from the response
            try:
                # Look for JSON in the response
                json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group())
            except:
                pass
            
            return None
            
        except Exception as error:
            print(f"❌ Error parsing command with AI: {error}")
            return None
    
    def parse_command_manually(self, command):
        """Manual command parsing as fallback with typo tolerance"""
        command_lower = command.lower()
        parts = command_lower.split()
        if not parts:
            return None

        action_word = parts[0]
        
        # Early route for listing archived emails (avoid fuzzy 'archive' collision)
        try:
            if re.search(r'\b(list|show|view|get)\b', command_lower) and (
                'archived' in command_lower or 'not in inbox' in command_lower or 'hidden' in command_lower
            ):
                return {
                    "action": "list",
                    "target_type": "archived",
                    "target": "archived",
                    "confirmation_required": False
                }
            # All mail variants
            if re.search(r'\b(list|show|view|get)\b', command_lower) and (
                'all mail' in command_lower or 'all emails' in command_lower or 'everything' in command_lower
            ):
                return {
                    "action": "list",
                    "target_type": "all_mail",
                    "target": "all",
                    "confirmation_required": False
                }
        except Exception:
            pass

        # Early routes for custom categories (archive/delete) with optional age filter
        def _parse_age_days(cmd: str):
            m = re.search(r'older\s+(?:than|then)\s+(\d+)\s*(day|days|d|week|weeks|w|month|months|m|year|years|y)', cmd)
            if not m:
                return None
            try:
                qty = int(m.group(1)); unit = m.group(2).lower()
                if unit in ['day','days','d']: return qty
                if unit in ['week','weeks','w']: return qty*7
                if unit in ['month','months','m']: return qty*30
                if unit in ['year','years','y']: return qty*365
            except Exception:
                return None
            return None

        if 'archive' in command_lower and (('verification' in command_lower and 'code' in command_lower) or 'משלוח' in command_lower or 'shipping' in command_lower or 'delivery' in command_lower or 'shipped' in command_lower):
            older = _parse_age_days(command_lower)
            if ('verification' in command_lower and 'code' in command_lower):
                return {"action": "archive", "target_type": "custom_category", "target": "verification_codes", "confirmation_required": True, "older_than_days": older}
            else:
                return {"action": "archive", "target_type": "custom_category", "target": "shipping_delivery", "confirmation_required": True, "older_than_days": older}

        if 'delete' in command_lower and (('verification' in command_lower and 'code' in command_lower) or 'משלוח' in command_lower or 'shipping' in command_lower or 'delivery' in command_lower or 'shipped' in command_lower):
            older = _parse_age_days(command_lower)
            if ('verification' in command_lower and 'code' in command_lower):
                return {"action": "delete", "target_type": "custom_category", "target": "verification_codes", "confirmation_required": True, "older_than_days": older}
            else:
                return {"action": "delete", "target_type": "custom_category", "target": "shipping_delivery", "confirmation_required": True, "older_than_days": older}

        # Define known actions and their keywords/aliases
        actions = {
            "delete": ["delete", "remove", "trash", "del"],
            "archive": ["archive", "hide"],
            "list": ["list", "show", "get", "view"],
            "stats": ["stats", "statistics", "report"],
            "search": ["search", "find"],
            "send": ["send", "email", "compose"],
            "restore": ["restore", "unarchive"],
            "label": ["label", "tag"],
            "list_labels": ["list labels", "show labels"], # multi-word needs special handling
            "info_only": ["empty trash"]
        }

        # Handle multi-word label listing/showing first
        if "list label" in command_lower or "show label" in command_lower:
            label_match = re.search(r'(?:list|show) label\s+["\']?([^"\']+)["\']?', command_lower)
            if label_match:
                label_name = label_match.group(1).strip()
                return {
                    "action": "show_label",
                    "target_type": "label",
                    "target": label_name,
                    "confirmation_required": False
                }
            else:
                return {
                    "action": "list_labels",
                    "target_type": "labels",
                    "target": "labels",
                    "confirmation_required": False
                }

        # Early handling for general label commands to avoid misclassification as 'send'
        if "label" in command_lower and (" from " in command_lower) and (" as " in command_lower or " with " in command_lower):
            # Try domain first
            domain_match = re.search(r'from\s+([a-zA-Z0-9.-]+\.[a-zA-Z0-9.-]+)', command_lower)
            label_match = re.search(r'(?:as|with)\s+["\']?([^"\']+)["\']?', command_lower)
            if domain_match and label_match:
                return {
                    "action": "label",
                    "target_type": "domain",
                    "target": domain_match.group(1),
                    "label": label_match.group(1).strip(),
                    "confirmation_required": True
                }
            sender_match = re.search(r'from\s+([a-zA-Z0-9\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff]+)', command_lower)
            if sender_match and label_match:
                return {
                    "action": "label",
                    "target_type": "sender",
                    "target": sender_match.group(1),
                    "label": label_match.group(1).strip(),
                    "confirmation_required": True
                }

        # Early handling for restore commands
        if "restore" in command_lower and " from " in command_lower:
            # Extract the token immediately after 'from' tolerating typos before it
            sender_any_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff]+)', command_lower)
            if sender_any_match:
                return {
                    "action": "restore",
                    "target_type": "sender",
                    "target": sender_any_match.group(1),
                    "confirmation_required": True
                }

        # Find the best matching action using fuzzy matching
        best_match_action = None
        highest_score = 0
        
        # Handle specific phrases
        if "empty trash" in command_lower:
            best_match_action = "info_only"
            highest_score = 100  # High confidence for specific phrases
        elif "stats" in command_lower or "statistics" in command_lower:
            best_match_action = "stats"
            highest_score = 100  # High confidence for specific phrases
        elif "full analysis" in command_lower:
            best_match_action = "stats"
            highest_score = 100  # High confidence for specific phrases
        elif ("list" in command_lower and "recent" in command_lower) or command_lower.strip() == "list recent emails":
            best_match_action = "list"
            highest_score = 100  # High confidence for specific phrases
        else:
            # Check for keywords within the command, not just the start
            best_score = 0
            for action, keywords in actions.items():
                for keyword in keywords:
                    score = fuzz.partial_ratio(keyword, command_lower)
                    if score > best_score:
                        best_score = score
                        best_match_action = action
            highest_score = best_score
        
        # If score is too low, command is not understood
        if highest_score < 75 and best_match_action not in ["list_labels", "info_only", "show_label", "label"]:
            if "stats" in command_lower:
                best_match_action = "stats"
                highest_score = 100
            else:
                return {"debug_info": f"No action matched with enough confidence. Best guess was '{best_match_action}' with a score of {highest_score}."}
        
        # Info-only
        if best_match_action == "info_only":
            return {
                "action": "info_only",
                "target_type": "info",
                "target": "trash_info",
                "confirmation_required": False
            }
        
        # Bulk cleanup by age only (skip if a category/custom-category is mentioned)
        if (best_match_action in ["delete", "archive"]) and "all" in command_lower and "older" in command_lower:
            category_tokens_present = any(tok in command_lower for tok in [
                "promotion","promotions","social","updates","forums","personal",
                "verification","code","shipping","delivery","shipped","משלוח",
                "security","account"
            ])
            if category_tokens_present:
                pass
            else:
                age_match = re.search(r'older\s+(?:than|then)\s+(\d+)\s*(day|days|d|week|weeks|w|month|months|m|year|years|y)', command_lower)
            if age_match:
                try:
                    qty = int(age_match.group(1))
                    unit = age_match.group(2).lower()
                    if unit in ['week', 'weeks', 'w']:
                        older_than_days = qty * 7
                    elif unit in ['month', 'months', 'm']:
                        older_than_days = qty * 30
                    elif unit in ['year', 'years', 'y']:
                        older_than_days = qty * 365
                    else:
                        older_than_days = qty
                    return {
                        "action": best_match_action,
                        "target_type": "bulk_age",
                        "target": "all_emails",
                        "confirmation_required": True,
                        "older_than_days": older_than_days
                    }
                except ValueError:
                    pass
        
        # Stats
        if best_match_action == "stats":
            return {
                "action": "stats",
                "target_type": "stats",
                "target": "email_stats",
                "confirmation_required": False
            }

        # List filters
        if best_match_action == "list":
            # Custom category listing should be checked early so it doesn't get overridden by generic date parsing
            # Detect optional age filter
            custom_older_days = None
            age_m = re.search(r'older\s+(?:than|then)\s+(\d+)\s*(day|days|d|week|weeks|w|month|months|m|year|years|y)', command_lower)
            if age_m:
                try:
                    qty = int(age_m.group(1)); unit = age_m.group(2)
                    if unit in ["day","days","d"]: custom_older_days = qty
                    elif unit in ["week","weeks","w"]: custom_older_days = qty*7
                    elif unit in ["month","months","m"]: custom_older_days = qty*30
                    elif unit in ["year","years","y"]: custom_older_days = qty*365
                except Exception:
                    custom_older_days = None
            # Detect 'from N <unit> ago' to produce a bounded date window for custom categories
            custom_date_range = None
            from_ago_m = re.search(r'from\s+(a|\d+)\s+(day|week|month|year)s?\s+ago', command_lower)
            if from_ago_m:
                qty_str = from_ago_m.group(1)
                qty = 1 if qty_str == 'a' else int(qty_str)
                unit = from_ago_m.group(2)
                custom_date_range = f"{qty} {unit}"
            if ("verification" in command_lower and "code" in command_lower) and ("list" in command_lower or "show" in command_lower or "view" in command_lower or "get" in command_lower):
                return {"action": "list", "target_type": "custom_category", "target": "verification_codes", "older_than_days": custom_older_days, "date_range": custom_date_range, "confirmation_required": False}
            if ("shipping" in command_lower or "delivery" in command_lower or "shipped" in command_lower or "משלוח" in command_lower) and ("list" in command_lower or "show" in command_lower or "view" in command_lower or "get" in command_lower):
                return {"action": "list", "target_type": "custom_category", "target": "shipping_delivery", "older_than_days": custom_older_days, "date_range": custom_date_range, "confirmation_required": False}
            if ("security" in command_lower or "account" in command_lower or "sign in" in command_lower or "login" in command_lower) and ("list" in command_lower or "show" in command_lower or "view" in command_lower or "get" in command_lower):
                return {"action": "list", "target_type": "custom_category", "target": "account_security", "older_than_days": custom_older_days, "date_range": custom_date_range, "confirmation_required": False}
            # Archived
            if "archived" in command_lower or "not in inbox" in command_lower or "hidden" in command_lower:
                return {"action": "list", "target_type": "archived", "target": "archived", "confirmation_required": False}
            # All mail plain
            if "all mail" in command_lower or "all emails" in command_lower or "everything" in command_lower:
                return {"action": "list", "target_type": "all_mail", "target": "all", "confirmation_required": False}
        if best_match_action == "list":
            # Initialize optional date filters to avoid UnboundLocalError when not set
            older_than_match = re.search(r'(older than|before)\s+(a|\d+)\s+(day|week|month|year)s?', command_lower)
            from_ago_match = re.search(r'from\s+(a|\d+)\s+(day|week|month|year)s?\s+ago', command_lower)
            simple_date_match = re.search(r'from\s+(today|yesterday|last week|last month|last year)', command_lower)
            
            if older_than_match:
                quantity = older_than_match.group(2)
                unit = older_than_match.group(3)
                if quantity == 'a': quantity = '1'
                return {"action": "list", "target_type": "older_than", "target": f"{quantity} {unit}", "confirmation_required": False}
            elif from_ago_match:
                quantity = from_ago_match.group(1)
                unit = from_ago_match.group(2)
                if quantity == 'a': quantity = '1'
                return {"action": "list", "target_type": "date_range", "target": f"{quantity} {unit}", "confirmation_required": False}
            elif simple_date_match:
                date_range = simple_date_match.group(1)
                return {"action": "list", "target_type": "date_range", "target": date_range, "confirmation_required": False}
            
            # Fallbacks: detect common time phrases anywhere, even without 'from'
            for key in ["today", "yesterday", "last week", "last month", "last year"]:
                if key in command_lower:
                    return {"action": "list", "target_type": "date_range", "target": key, "confirmation_required": False}
            
            # Fallback: detect 'older than' anywhere
            any_older = re.search(r'older\s+than\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)', command_lower)
            if any_older:
                qty = any_older.group(1)
                unit = any_older.group(2)
                unit = unit[:-1] if unit.endswith('s') else unit
                return {"action": "list", "target_type": "older_than", "target": f"{qty} {unit}", "confirmation_required": False}
        
            # Detect recent
            if "recent" in command_lower:
                return {"action": "list", "target_type": "recent", "target": "recent", "confirmation_required": False}
            
            # (Date-range for custom categories intentionally removed; use 'older than' for these.)
            # Detect domain/sender after 'from'
            domain_match = re.search(r'from\s+([a-z0-9.-]+\.[a-z]{2,})', command_lower)
            if domain_match:
                return {"action": "list", "target_type": "domain", "target": domain_match.group(1), "confirmation_required": False}
            
            sender_match = re.search(r'from\s+([a-zA-Z0-9\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff]+)', command_lower)
            if sender_match:
                candidate = sender_match.group(1)
                skip = {"emails","email","last","week","month","year","today","yesterday","older","than","then"}
                if candidate not in skip:
                    return {"action": "list", "target_type": "sender", "target": candidate, "confirmation_required": False}
        
            # Final fallback for list intent: treat as recent
            return {"action": "list", "target_type": "recent", "target": "recent", "confirmation_required": False}

        # Send email
        send_match = None
        if best_match_action == "send":
            send_match = re.search(r'to ([\w\.\-]+@[\w\.\-]+)(?: with subject ["\']([^"\']+)["\'])?(?: and message ["\']([^"\']+)["\'])?', command_lower)
        if send_match:
            to = send_match.group(1)
            subject = send_match.group(2) if send_match.group(2) else "No Subject"
            message = send_match.group(3) if send_match.group(3) else ""
            return {"action": "send", "to": to, "subject": subject, "message": message, "confirmation_required": True}

        # Search
        if best_match_action == "search":
            search_match = re.search(r'(?:for )?emails? (?:with subject|about|containing)\s+["\']?([^"\']+)["\']?', command_lower)
            if search_match:
                search_term = search_match.group(1)
                return {"action": "search", "target_type": "subject", "target": search_term, "confirmation_required": False}

        # Delete parsing
        if best_match_action == "delete":
            older_than_days = None
            age_match = re.search(r'older\s+(?:than|then)\s+(\d+)\s*(day|days|d|week|weeks|w|month|months|m|year|years|y)', command_lower)
            if age_match:
                try:
                    qty = int(age_match.group(1))
                    unit = age_match.group(2)
                    if unit in ["day", "days", "d"]: older_than_days = qty
                    elif unit in ["week", "weeks", "w"]: older_than_days = qty * 7
                    elif unit in ["month", "months", "m"]: older_than_days = qty * 30
                    elif unit in ["year", "years", "y"]: older_than_days = qty * 365
                except Exception:
                    older_than_days = None
            domain_match = re.search(r'from\s+([a-zA-Z0-9.-]+\.(?:com|org|net|edu|gov|co\.[a-z.]+|[a-z]{2}))', command_lower)
            if domain_match:
                return {"action": "delete", "target_type": "domain", "target": domain_match.group(1), "confirmation_required": True, "older_than_days": older_than_days}
            flexible_sender_match = re.search(r'from\s+([a-zA-Z0-9\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff]+)', command_lower)
            if flexible_sender_match:
                sender_keyword = flexible_sender_match.group(1)
                if sender_keyword not in ['emails','all','the','my','any','this','that','these','those','older','than','then','before','after']:
                    return {"action": "delete", "target_type": "sender", "target": sender_keyword, "confirmation_required": True, "older_than_days": older_than_days}
            if "promotion" in command_lower or "promotions" in command_lower:
                return {"action": "delete", "target_type": "category", "target": "promotions", "confirmation_required": True, "older_than_days": older_than_days}
            # Custom categories: verification codes, shipping
            if "verification" in command_lower and "code" in command_lower:
                return {"action": "delete", "target_type": "custom_category", "target": "verification_codes", "confirmation_required": True, "older_than_days": older_than_days}
            if "shipping" in command_lower or "delivery" in command_lower or "shipped" in command_lower or "משלוח" in command_lower:
                return {"action": "delete", "target_type": "custom_category", "target": "shipping_delivery", "confirmation_required": True, "older_than_days": older_than_days}
            if "security" in command_lower or "account" in command_lower:
                return {"action": "delete", "target_type": "custom_category", "target": "account_security", "confirmation_required": True, "older_than_days": older_than_days}

        # Archive parsing (mirror of delete)
        if best_match_action == "archive":
            # Optional age filter: "older than N (days|weeks|months|years)"
            older_than_days = None
            age_match = re.search(r'older\s+(?:than|then)\s+(\d+)\s*(day|days|d|week|weeks|w|month|months|m|year|years|y)', command_lower)
            if age_match:
                try:
                    qty = int(age_match.group(1))
                    unit = age_match.group(2)
                    if unit in ["day", "days", "d"]: older_than_days = qty
                    elif unit in ["week", "weeks", "w"]: older_than_days = qty * 7
                    elif unit in ["month", "months", "m"]: older_than_days = qty * 30
                    elif unit in ["year", "years", "y"]: older_than_days = qty * 365
                except Exception:
                    older_than_days = None
            # Gmail categories (e.g., promotions)
            # (Drop Gmail category mappings per user request; focus on custom categories only)

            domain_match = re.search(r'from\s+([a-zA-Z0-9.-]+\.(?:com|org|net|edu|gov|co\.[a-z.]+|[a-z]{2}))', command_lower)
            if domain_match:
                return {"action": "archive", "target_type": "domain", "target": domain_match.group(1), "confirmation_required": True, "older_than_days": older_than_days}
            flexible_sender_match = re.search(r'from\s+([a-zA-Z0-9\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff]+)', command_lower)
            if flexible_sender_match:
                sender_keyword = flexible_sender_match.group(1)
                if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                    return {"action": "archive", "target_type": "sender", "target": sender_keyword, "confirmation_required": True, "older_than_days": older_than_days}
            # Custom categories
            if "verification" in command_lower and "code" in command_lower:
                return {"action": "archive", "target_type": "custom_category", "target": "verification_codes", "confirmation_required": True, "older_than_days": older_than_days}
            if "shipping" in command_lower or "delivery" in command_lower or "shipped" in command_lower or "משלוח" in command_lower:
                return {"action": "archive", "target_type": "custom_category", "target": "shipping_delivery", "confirmation_required": True, "older_than_days": older_than_days}
            if "security" in command_lower or "account" in command_lower:
                return {"action": "archive", "target_type": "custom_category", "target": "account_security", "confirmation_required": True, "older_than_days": older_than_days}

        return {"debug_info": f"Action '{best_match_action}' was recognized, but no specific pattern was matched for the rest of the command."}

    def process_natural_language_command(self, command, confirmation_data=None):
        """Process natural language commands using AI or manual parsing"""
        
        # If confirmation data is provided, execute the confirmed action
        if confirmation_data:
            action = confirmation_data.get("action")
            if action == "delete_by_age":
                return self.delete_emails_by_age_only(
                    confirmation_data["older_than_days"], 
                    confirm=True
                )
            elif action == "delete_by_sender":
                return self.delete_emails_by_sender(
                    confirmation_data["sender"], 
                    confirm=True, 
                    older_than_days=confirmation_data.get("older_than_days")
                )
            elif action == "delete_by_domain":
                    return self.delete_emails_by_domain(
                    confirmation_data["domain"],
                    confirm=True,
                    older_than_days=confirmation_data.get("older_than_days")
                )
            elif action == "delete_by_subject":
                    return self.delete_emails_by_subject_keywords(
                    confirmation_data["keywords"],
                    confirm=True,
                    older_than_days=confirmation_data.get("older_than_days")
                )
            elif action == "delete_by_category":
                    return self.delete_emails_by_category(
                    confirmation_data["category"],
                    confirm=True,
                    older_than_days=confirmation_data.get("older_than_days")
                )
            elif action == "send_email":
                return self.send_email(
                    confirmation_data["to"],
                    confirmation_data["subject"],
                    confirmation_data["message"]
                )
            elif action == "archive_by_sender":
                return self.archive_emails_by_sender(
                    confirmation_data["sender"], confirm=True, older_than_days=confirmation_data.get("older_than_days")
                )
            elif action == "archive_by_domain":
                return self.archive_emails_by_domain(
                    confirmation_data["domain"], confirm=True, older_than_days=confirmation_data.get("older_than_days")
                )
            elif action == "archive_by_subject":
                return self.archive_emails_by_subject_keywords(confirmation_data["keywords"], confirm=True)
            elif action == "archive_by_custom_category":
                return self.archive_emails_by_custom_category(
                    confirmation_data["category_key"], confirm=True, older_than_days=confirmation_data.get("older_than_days")
                )
            elif action == "restore_by_sender":
                return self.restore_emails_from_sender(confirmation_data["sender"], confirm=True)
            elif action == "label_by_sender":
                return self.label_emails_by_sender(
                    confirmation_data["sender"], confirmation_data["label"], confirm=True
                )
            elif action == "label_by_domain":
                return self.label_emails_by_domain(
                    confirmation_data["domain"], confirmation_data["label"], confirm=True
                )
            elif action == "label_by_keywords":
                return self.label_emails_by_keywords(
                    confirmation_data["keywords"], confirmation_data["label"], confirm=True
                )
            elif action == "archive_by_age":
                return self.archive_emails_by_age_only(
                    confirmation_data["older_than_days"], 
                    confirm=True
                )
            elif action == "delete_by_custom_category":
                return self.delete_emails_by_custom_category(
                    confirmation_data["category_key"], confirm=True, older_than_days=confirmation_data.get("older_than_days")
                )

        # Regular command processing
        # Use manual parsing
        parsed = self.parse_command_manually(command)
        
        if not parsed or parsed.get("debug_info"):
            return {"status": "error", "message": _("Command not understood."), "debug_info": parsed.get("debug_info", "Parser returned empty.")}
        
        action = parsed.get("action")
        target_type = parsed.get("target_type")
        target = parsed.get("target")
        older_than_days = parsed.get("older_than_days")
        confirm_required = parsed.get("confirmation_required", False)

        try:
            if action == "delete":
                # For web, we don't ask for confirmation here. We return a confirmation request.
                if target_type == "domain":
                    return self.delete_emails_by_domain(target, confirm=not confirm_required, older_than_days=older_than_days)
                elif target_type == "subject_keywords":
                    return self.delete_emails_by_subject_keywords(target, confirm=not confirm_required, older_than_days=older_than_days)
                elif target_type == "category":
                    return self.delete_emails_by_category(target, confirm=not confirm_required, older_than_days=older_than_days)
                elif target_type == "sender":
                    return self.delete_emails_by_sender(target, confirm=not confirm_required, older_than_days=older_than_days)
                elif target_type == "bulk_age":
                    return self.delete_emails_by_age_only(older_than_days, confirm=not confirm_required)
                elif target_type == "custom_category":
                    return self.delete_emails_by_custom_category(target, confirm=not confirm_required, older_than_days=older_than_days)
            
            elif action == "info_only":
                return {"status": "info", "message": "To empty trash: Go to Gmail → Trash folder → 'Empty Trash now' button"}
            
            elif action == "list":
                if target_type == "recent":
                    res = self.list_recent_emails()
                    emails = res.get("emails", [])
                    if "error" in res:
                        return {"status": "error", "message": f"Error fetching emails: {res['error']}"}
                    if not emails: return {"status": "success", "message": "No recent emails found."}
                    return {"status": "success", "data": emails, "type": "email_list", "next_page_token": res.get("next_page_token"), "list_context": {"mode": "recent"}}
                elif target_type == "archived":
                    res = self.list_archived_emails()
                    emails = res.get("emails", []) if isinstance(res, dict) else res
                    next_token = res.get("next_page_token") if isinstance(res, dict) else None
                    if not emails: return {"status": "success", "message": _("No archived emails found.")}
                    return {"status": "success", "data": emails, "type": "email_list", "next_page_token": next_token, "list_context": {"mode": "archived"}}
                elif target_type == "all_mail":
                    res = self.list_all_emails()
                    emails = res.get("emails", [])
                    if not emails: return {"status": "success", "message": _("No emails found in All Mail.")}
                    return {"status": "success", "data": emails, "type": "email_list", "next_page_token": res.get("next_page_token"), "list_context": {"mode": "all_mail"}}
                elif target_type == "labels" or parsed.get("target") == "labels":
                    labels = self.list_labels()
                    if not labels: return {"status": "success", "message": _("You have no custom labels.")}
                    return {"status": "success", "data": labels, "type": "label_list"}
                elif target_type == "category":
                    res = self.list_emails_by_category(target, older_than_days=older_than_days)
                    emails = res.get("emails", []) if isinstance(res, dict) else res
                    next_token = res.get("next_page_token") if isinstance(res, dict) else None
                    if not emails:
                        return {"status": "success", "message": _("No emails found in category: %(category)s.") % {"category": target}}
                    pretty_cat = self._pretty_gmail_label_or_category(target)
                    age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                    msg = _("Found %(count)d emails%(age)s in %(what)s.") % {"count": len(emails), "age": age_txt, "what": pretty_cat} if older_than_days else _("Found %(count)d emails in %(what)s.") % {"count": len(emails), "what": pretty_cat}
                    payload = {"status": "success", "data": emails, "type": "email_list", "next_page_token": next_token, "list_context": {"mode": "category", "category": target}, "message": msg}
                    return payload
                elif target_type == "domain":
                    # Support phrases like 'from 2 weeks ago' by mapping to date_range window
                    res = self.list_emails_by_domain(target, older_than_days=older_than_days)
                    emails = res.get("emails", [])
                    if not emails: return {"status": "success", "message": _("No emails found from domain: %(domain)s.") % {"domain": target}}
                    lc = {"mode": "domain", "target": target}
                    if older_than_days is not None: lc["older_than_days"] = older_than_days
                    return {"status": "success", "data": emails, "type": "email_list", "next_page_token": res.get("next_page_token"), "list_context": lc}
                elif target_type == "sender":
                    res = self.list_emails_by_sender(target, older_than_days=older_than_days)
                    emails = res.get("emails", [])
                    if not emails: return {"status": "success", "message": _("No emails found from sender: %(sender)s.") % {"sender": target}}
                    lc = {"mode": "sender", "target": target}
                    if older_than_days is not None: lc["older_than_days"] = older_than_days
                    return {"status": "success", "data": emails, "type": "email_list", "next_page_token": res.get("next_page_token"), "list_context": lc}
                elif target_type == "date_range":
                    result = self.list_emails_by_date_range(target)
                    if not result.get("emails"): return {"status": "success", "message": result.get("message")}
                    return {"status": "success", "data": result.get("emails"), "type": "email_list", "message": result.get("message"), "next_page_token": result.get("next_page_token"), "list_context": {"mode": "date_range", "target": target}}
                elif target_type == "older_than":
                    result = self.list_emails_older_than(target)
                    if not result.get("emails"): return {"status": "success", "message": result.get("message")}
                    return {"status": "success", "data": result.get("emails"), "type": "email_list", "message": result.get("message"), "next_page_token": result.get("next_page_token"), "list_context": {"mode": "older_than", "target": target}}
                elif target_type == "custom_category":
                    res = self.list_emails_by_custom_category(target, older_than_days=older_than_days, date_range=parsed.get("date_range"))
                    emails = res.get("emails", [])
                    next_token = res.get("next_page_token")
                    pretty = self._pretty_category_name(target)
                    if not emails:
                        return {"status": "success", "message": _("No emails found for %(what)s.") % {"what": pretty}}
                    lc = {"mode": "custom_category", "category": pretty}
                    if older_than_days is not None:
                        lc["older_than_days"] = older_than_days
                    if parsed.get("date_range"):
                        lc["date_range"] = parsed.get("date_range")
                    # Always include a Hebrew message for list results
                    age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                    msg = _("Found %(count)d emails%(age)s in %(what)s.") % {"count": len(emails), "age": age_txt, "what": pretty} if older_than_days else _("Found %(count)d emails in %(what)s.") % {"count": len(emails), "what": pretty}
                    payload = {"status": "success", "data": emails, "type": "email_list", "next_page_token": next_token, "list_context": lc, "message": msg}
                    return payload
            
            elif action == "search":
                if target_type == "subject":
                    emails = self.search_emails_by_subject(target)
                    if not emails: return {"status": "success", "message": _("No emails found with subject: '%(subject)s'.") % {"subject": target}}
                    return {"status": "success", "data": emails, "type": "email_list"}

            elif action == "send":
                to = parsed.get("to")
                subject = parsed.get("subject")
                message = parsed.get("message")
                
                if not to:
                    return {"status": "error", "message": "Could not determine recipient."}

                if confirm_required:
                     return {
                        "status": "confirmation_required",
                        "message": f"You are about to send an email to {to} with subject '{subject}'. Confirm?",
                        "action_details": {"action": "send_email", "to": to, "subject": subject, "message": message}
                    }
                else:
                    return self.send_email(to, subject, message)

            elif action == "stats":
                # Check if this is a full analysis command
                if "full analysis" in command.lower():
                    return self.show_email_stats(full=True)
                else:
                    return self.show_email_stats()
            
            elif action == "archive":
                if target_type == "bulk_age":
                    return self.archive_emails_by_age_only(older_than_days, confirm=not confirm_required)
                elif target_type == "sender":
                    return self.archive_emails_by_sender(target, confirm=not confirm_required, older_than_days=older_than_days)
                elif target_type == "domain":
                    return self.archive_emails_by_domain(target, confirm=not confirm_required, older_than_days=older_than_days)
                elif target_type == "custom_category":
                    return self.archive_emails_by_custom_category(target, confirm=not confirm_required, older_than_days=older_than_days)

            elif action == "restore":
                if target_type == "sender":
                    return self.restore_emails_from_sender(target, confirm=not confirm_required)

            elif action == "list_labels":
                labels = self.list_labels()
                if not labels: return {"status": "success", "message": "You have no custom labels."}
                return {"status": "success", "data": labels, "type": "label_list"}

            elif action == "show_label":
                res = self.get_emails_by_label(target)
                emails = res.get("emails", []) if isinstance(res, dict) else res
                next_token = res.get("next_page_token") if isinstance(res, dict) else None
                if not emails: return {"status": "success", "message": f"No emails found with label: '{target}'."}
                return {"status": "success", "data": emails, "type": "email_list", "next_page_token": next_token, "list_context": {"mode": "label", "label": target}}

            elif action == "label":
                if "label" not in parsed:
                    return {"status": "error", "message": "Label command needs a label name (e.g., 'as Shopping')."}
                label_name = parsed.get("label")
                if target_type == "sender":
                    return self.label_emails_by_sender(target, label_name, confirm=not confirm_required)
                elif target_type == "domain":
                    return self.label_emails_by_domain(target, label_name, confirm=not confirm_required)
                elif target_type == "subject_keywords":
                    return self.label_emails_by_keywords(target, label_name, confirm=not confirm_required)
                else:
                    return {"status": "error", "message": "Label command needs a target (sender, domain, or keywords)."}

            # ... other actions
            
            return {"status": "error", "message": "Command executed, but no return value."}
            
        except Exception as error:
            return {"status": "error", "message": f"Error executing command: {error}"}

    def test_permissions(self):
        """Test what permissions we have"""
        try:
            # Try to get profile info
            profile = self.service.users().getProfile(userId='me').execute()
            email = profile.get('emailAddress', 'Unknown')
            
            # Try to list messages (read permission)
            self.service.users().messages().list(userId='me', maxResults=1).execute()
            
            return {"success": True, "email": email}
            
        except HttpError as error:
            return {"success": False, "error": str(error)}
    
    def archive_emails_by_sender(self, sender_email, confirm=False, older_than_days=None):
        """Archive emails from a specific sender, optionally filtered by age."""
        try:
            # Build search query
            query_parts = [f"from:{sender_email}"]
            if older_than_days:
                cutoff_date = (datetime.utcnow() - timedelta(days=older_than_days)).strftime('%Y/%m/%d')
                query_parts.append(f"before:{cutoff_date}")
            query = " ".join(query_parts)

            # Fetch all matching messages with pagination
            all_messages = []
            page_token = None
            while True:
                kwargs = {"userId": 'me', "q": query, "maxResults": 500}
                if page_token:
                    kwargs["pageToken"] = page_token
                results = self.service.users().messages().list(**kwargs).execute()
                messages = results.get('messages', []) or []
                all_messages.extend(messages)
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            if not all_messages:
                age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                return {"status": "success", "message": _("No emails found from %(sender)s%(age)s.") % {"sender": sender_email, "age": age_txt}, "archived_count": 0}
            
            if not confirm:
                return {
                    "status": "confirmation_required", 
                    "message": _("Found %(count)d emails from %(sender)s%(age)s. Do you want to archive them?") % {"count": len(all_messages), "sender": sender_email, "age": (_(' older than %(days)d days') % {'days': older_than_days}) if older_than_days else ''},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {"action": "archive_by_sender", "sender": sender_email, "older_than_days": older_than_days}
                }
            
            # Archive in batches
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            for i in range(0, len(all_messages), 100):
                batch = all_messages[i:i+100]
                batch_ids = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                    userId='me', 
                        body={'ids': batch_ids, 'removeLabelIds': ['INBOX']}
                ).execute()
                    total_processed += len(batch)
                except HttpError:
                    # Fallback to single modify
                    for m in batch:
                        try:
                            self.service.users().messages().modify(userId='me', id=m['id'], body={'removeLabelIds': ['INBOX']}).execute()
                            total_processed += 1
                        except HttpError:
                            continue
            action_id = self._record_undo('archive', message_ids)
            return {"status": "success", "message": _("Archived %(count)d emails from %(sender)s.") % {"count": total_processed, "sender": sender_email}, "archived_count": total_processed, "undo_action_id": action_id}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error archiving emails: {error}"}
    
    def archive_emails_by_domain(self, domain, confirm=False, older_than_days=None):
        """Archive emails from a specific domain, optionally filtered by age."""
        try:
            query_parts = [f"from:*@{domain}"]
            if older_than_days:
                cutoff_date = (datetime.utcnow() - timedelta(days=older_than_days)).strftime('%Y/%m/%d')
                query_parts.append(f"before:{cutoff_date}")
            query = " ".join(query_parts)

            all_messages = []
            page_token = None
            while True:
                kwargs = {"userId": 'me', "q": query, "maxResults": 500}
                if page_token:
                    kwargs["pageToken"] = page_token
                results = self.service.users().messages().list(**kwargs).execute()
                messages = results.get('messages', []) or []
                all_messages.extend(messages)
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            if not all_messages:
                age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                return {"status": "success", "message": _("No emails found from %(domain)s%(age)s.") % {"domain": domain, "age": age_txt}, "archived_count": 0}
            
            if not confirm:
                return {
                    "status": "confirmation_required",
                    "message": _("Found %(count)d emails from %(domain)s%(age)s. Do you want to archive them?") % {"count": len(all_messages), "domain": domain, "age": (_(' older than %(days)d days') % {'days': older_than_days}) if older_than_days else ''},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {"action": "archive_by_domain", "domain": domain, "older_than_days": older_than_days}
                }
            
            total_processed = 0
            message_ids = [m['id'] for m in all_messages]
            for i in range(0, len(all_messages), 100):
                batch = all_messages[i:i+100]
                batch_ids = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                    userId='me', 
                        body={'ids': batch_ids, 'removeLabelIds': ['INBOX']}
                ).execute()
                    total_processed += len(batch)
                except HttpError:
                    for m in batch:
                        try:
                            self.service.users().messages().modify(userId='me', id=m['id'], body={'removeLabelIds': ['INBOX']}).execute()
                            total_processed += 1
                        except HttpError:
                            continue
            action_id = self._record_undo('archive', message_ids)
            return {"status": "success", "message": _("Archived %(count)d emails from %(domain)s.") % {"count": total_processed, "domain": domain}, "archived_count": total_processed, "undo_action_id": action_id}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error archiving emails: {error}"}
    
    def archive_emails_by_subject_keywords(self, keywords, confirm=False):
        """Archive emails containing specific keywords in subject (safer than delete)"""
        try:
            # Build search query for subject keywords
            keyword_query = " OR ".join([f'subject:"{keyword}"' for keyword in keywords])
            query = f"({keyword_query})"
            
            results = self.service.users().messages().list(
                userId='me', q=query).execute()
            messages = results.get('messages', [])
            
            if not messages:
                return {"status": "success", "message": f"No emails found with keywords: {', '.join(keywords)}", "archived_count": 0}
            
            if not confirm:
                return {
                    "status": "confirmation_required",
                    "count": len(messages),
                    "total_estimated": len(messages),
                    "preview": self._build_preview(messages),
                    "action_details": {"action": "archive_by_subject", "keywords": keywords}
                }
            
            # Archive the emails (safer than delete) using batch processing
            if len(messages) > 10:  # Only batch if we have enough messages
                batch_size = 100  # Gmail batch API limit
                for i in range(0, len(messages), batch_size):
                    batch_messages = messages[i:i + batch_size]
                    batch_ids = [m['id'] for m in batch_messages]
                    
                    try:
                        # Use batchModify for multiple emails at once
                        self.service.users().messages().batchModify(
                            userId='me', 
                            body={'ids': batch_ids, 'removeLabelIds': ['INBOX']}
                        ).execute()
                    except HttpError:
                        # Fallback to individual requests for this batch
                        for message in batch_messages:
                            try:
                                self.service.users().messages().modify(
                                    userId='me', 
                                    id=message['id'],
                                    body={'removeLabelIds': ['INBOX']}
                                ).execute()
                            except HttpError:
                                continue
            else:
                # For small numbers, use individual requests
                for message in messages:
                    self.service.users().messages().modify(
                        userId='me', 
                        id=message['id'],
                        body={'removeLabelIds': ['INBOX']}
                    ).execute()
            
            return {"status": "success", "message": f"Archived {len(messages)} emails with keywords {', '.join(keywords)}.", "archived_count": len(messages)}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error archiving emails: {error}"}
    
    def list_archived_emails(self, max_results=None, page_token=None):
        """List archived emails (messages not in Inbox, excluding Sent/Drafts/Spam/Trash)."""
        try:
            if max_results is None:
                max_results = self.default_max_results
            # Archived = messages not in Inbox. Also exclude Spam/Trash/Sent/Drafts/Chats.
            # Note: default Gmail search already excludes Spam/Trash, but we keep them explicit.
            q = '-in:inbox -in:spam -in:trash -in:chats -in:sent -in:drafts'
            kwargs = {"userId": 'me', "q": q, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.service.users().messages().list(**kwargs).execute()
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')
            if not messages:
                return {"emails": [], "next_page_token": None}
            archived_emails = []
            
            # Use batch processing for much faster execution
            if len(messages) > 10:  # Only batch if we have enough messages
                batch_size = 100  # Gmail batch API limit
                for i in range(0, len(messages), batch_size):
                    batch_messages = messages[i:i + batch_size]
                    
                    # Create batch request
                    batch = self.service.new_batch_http_request()
                    for message in batch_messages:
                        batch.add(
                            self.service.users().messages().get(
                                userId='me', 
                                id=message['id'], 
                                format='metadata', 
                                metadataHeaders=['From','Subject'],
                                fields='payload/headers,id,labelIds'
                            )
                        )
                    
                    try:
                        # Execute batch request
                        batch_responses = batch.execute()
                        
                        # Process batch responses
                        for message, response in zip(batch_messages, batch_responses):
                            try:
                                if isinstance(response, Exception):
                                    continue  # Skip failed requests
                                    
                                msg = response
                                headers = msg.get('payload', {}).get('headers', [])
                                subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                                sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                                archived_emails.append({
                                    'id': message['id'],
                                    'subject': subject,
                                    'sender': sender,
                                    'snippet': ''
                                })
                                
                            except Exception as e:
                                print(f"Error processing message {message['id']}: {e}")
                                continue
                                
                    except Exception as e:
                        print(f"Batch request failed: {e}")
                        # Fallback to individual requests for this batch
                        for message in batch_messages:
                            try:
                                msg = self.service.users().messages().get(
                                    userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                                    fields='payload/headers,id,labelIds'
                                ).execute()
                                headers = msg.get('payload', {}).get('headers', [])
                                subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                                sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                                archived_emails.append({
                                    'id': message['id'],
                                    'subject': subject,
                                    'sender': sender,
                                    'snippet': ''
                                })
                            except HttpError:
                                continue
            else:
                # For small numbers, use individual requests
                for message in messages:
                    msg = self.service.users().messages().get(
                        userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                        fields='payload/headers,id,labelIds').execute()
                    headers = msg.get('payload', {}).get('headers', [])
                    subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                    sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                    archived_emails.append({
                        'id': message['id'],
                        'subject': subject,
                        'sender': sender,
                        'snippet': ''
                    })
            return {"emails": archived_emails, "next_page_token": next_token}
        except HttpError as error:
            return {"emails": [], "next_page_token": None}

    def list_all_emails(self, max_results=None, page_token=None):
        """List all emails in All Mail (excludes Spam/Trash/Chats)."""
        try:
            if max_results is None:
                max_results = self.default_max_results
            q = '-in:spam -in:trash -in:chats'
            kwargs = {"userId": 'me', "q": q, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.service.users().messages().list(**kwargs).execute()
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')
            if not messages:
                return {"emails": [], "next_page_token": None}
            emails = []
            
            # Use batch processing for much faster execution
            if len(messages) > 10:  # Only batch if we have enough messages
                batch_size = 100  # Gmail batch API limit
                for i in range(0, len(messages), batch_size):
                    batch_messages = messages[i:i + batch_size]
                    
                    # Create batch request
                    batch = self.service.new_batch_http_request()
                    for message in batch_messages:
                        batch.add(
                            self.service.users().messages().get(
                                userId='me', 
                                id=message['id'], 
                                format='metadata', 
                                metadataHeaders=['From','Subject'],
                                fields='payload/headers,id'
                            )
                        )
                    
                    try:
                        # Execute batch request
                        batch_responses = batch.execute()
                        
                        # Process batch responses
                        for message, response in zip(batch_messages, batch_responses):
                            try:
                                if isinstance(response, Exception):
                                    continue  # Skip failed requests
                                    
                                msg = response
                                headers = msg.get('payload', {}).get('headers', [])
                                subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                                sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                                emails.append({'id': message['id'], 'subject': subject, 'sender': sender, 'snippet': ''})
                                
                            except Exception as e:
                                print(f"Error processing message {message['id']}: {e}")
                                continue
                                
                    except Exception as e:
                        print(f"Batch request failed: {e}")
                        # Fallback to individual requests for this batch
                        for message in batch_messages:
                            try:
                                msg = self.service.users().messages().get(
                                    userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                                    fields='payload/headers,id'
                                ).execute()
                                headers = msg.get('payload', {}).get('headers', [])
                                subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                                sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                                emails.append({'id': message['id'], 'subject': subject, 'sender': sender, 'snippet': ''})
                            except HttpError:
                                continue
            else:
                # For small numbers, use individual requests
                for message in messages:
                    msg = self.service.users().messages().get(
                        userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                        fields='payload/headers,id').execute()
                    headers = msg.get('payload', {}).get('headers', [])
                    subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                    sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                    emails.append({'id': message['id'], 'subject': subject, 'sender': sender, 'snippet': ''})
            return {"emails": emails, "next_page_token": next_token}
        except HttpError:
            return {"emails": [], "next_page_token": None}
    
    def restore_emails_from_sender(self, sender_email, confirm=False):
        """Restore archived emails from a specific sender back to inbox"""
        try:
            # Search for emails from the sender
            query = f"from:{sender_email}"
            results = self.service.users().messages().list(
                userId='me', q=query, maxResults=100).execute()
            messages = results.get('messages', [])
            
            if not messages:
                return {"status": "success", "message": _("No emails found from %(sender)s to restore.") % {"sender": sender_email}, "restored_count": 0}
            
            # Filter for emails that are not in inbox (archived)
            archived_messages = []
            
            # Use batch processing for much faster execution
            if len(messages) > 10:  # Only batch if we have enough messages
                batch_size = 100  # Gmail batch API limit
                for i in range(0, len(messages), batch_size):
                    batch_messages = messages[i:i + batch_size]
                    
                    # Create batch request
                    batch = self.service.new_batch_http_request()
                    for message in batch_messages:
                        batch.add(
                            self.service.users().messages().get(
                                userId='me', 
                                id=message['id']
                            )
                        )
                    
                    try:
                        # Execute batch request
                        batch_responses = batch.execute()
                        
                        # Process batch responses
                        for message, response in zip(batch_messages, batch_responses):
                            try:
                                if isinstance(response, Exception):
                                    continue  # Skip failed requests
                                    
                                msg = response
                                labels = msg.get('labelIds', [])
                                if 'INBOX' not in labels:
                                    archived_messages.append(message)
                                
                            except Exception as e:
                                print(f"Error processing message {message['id']}: {e}")
                                continue
                                
                    except Exception as e:
                        print(f"Batch request failed: {e}")
                        # Fallback to individual requests for this batch
                        for message in batch_messages:
                            try:
                                msg = self.service.users().messages().get(
                                    userId='me', id=message['id']).execute()
                                labels = msg.get('labelIds', [])
                                if 'INBOX' not in labels:
                                    archived_messages.append(message)
                            except HttpError:
                                continue
            else:
                # For small numbers, use individual requests
                for message in messages:
                    msg = self.service.users().messages().get(
                        userId='me', id=message['id']).execute()
                    labels = msg.get('labelIds', [])
                    if 'INBOX' not in labels:
                        archived_messages.append(message)
            
            if not archived_messages:
                return {"status": "success", "message": _("No archived emails found from %(sender)s.") % {"sender": sender_email}, "restored_count": 0}
            
            if not confirm:
                return {
                    "status": "confirmation_required",
                    "message": _("Found %(count)d archived emails from %(sender)s. Restore to Inbox?") % {"count": len(archived_messages), "sender": sender_email},
                    "count": len(archived_messages),
                    "total_estimated": len(archived_messages),
                    "preview": self._build_preview(archived_messages),
                    "action_details": {"action": "restore_by_sender", "sender": sender_email}
                }
            
            # Restore the emails to inbox using batch processing
            if len(archived_messages) > 10:  # Only batch if we have enough messages
                batch_size = 100  # Gmail batch API limit
                for i in range(0, len(archived_messages), batch_size):
                    batch_messages = archived_messages[i:i + batch_size]
                    
                    # Create batch request
                    batch = self.service.new_batch_http_request()
                    for message in batch_messages:
                        batch.add(
                            self.service.users().messages().modify(
                                userId='me', 
                                id=message['id'],
                                body={'addLabelIds': ['INBOX']}
                            )
                        )
                    
                    try:
                        # Execute batch request
                        batch.execute()
                    except Exception as e:
                        print(f"Batch restore failed: {e}")
                        # Fallback to individual requests for this batch
                        for message in batch_messages:
                            try:
                                self.service.users().messages().modify(
                                    userId='me', 
                                    id=message['id'],
                                    body={'addLabelIds': ['INBOX']}
                                ).execute()
                            except HttpError:
                                continue
            else:
                # For small numbers, use individual requests
             for message in archived_messages:
                self.service.users().messages().modify(
                    userId='me', 
                    id=message['id'],
                    body={'addLabelIds': ['INBOX']}
                ).execute()
            
            return {"status": "success", "message": _("Restored %(count)d emails from %(sender)s to Inbox.") % {"count": len(archived_messages), "sender": sender_email}, "restored_count": len(archived_messages)}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error restoring emails: {error}"}

    def create_label(self, label_name):
        """Create a new Gmail label"""
        try:
            # Check if label already exists
            labels = self.service.users().labels().list(userId='me').execute()
            existing_labels = [label['name'] for label in labels.get('labels', [])]
            
            if label_name in existing_labels:
                return label_name
            
            # Create new label
            label_object = {
                'name': label_name,
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show'
            }
            
            created_label = self.service.users().labels().create(
                userId='me', body=label_object).execute()
            
            return label_name
            
        except HttpError as error:
            print(f"❌ Error creating label: {error}")
            return None

    def get_label_id(self, label_name):
        """Get the ID of a label by name"""
        try:
            labels = self.service.users().labels().list(userId='me').execute()
            for label in labels.get('labels', []):
                if label['name'].lower() == label_name.lower():
                    return label['id']
            return None
        except HttpError as error:
            print(f"❌ Error getting label ID: {error}")
            return None

    def label_emails_by_sender(self, sender_email, label_name, confirm=False):
        """Label all emails from a specific sender"""
        try:
            # Search for emails from the sender first
            query = f"from:{sender_email}"
            results = self.service.users().messages().list(
                userId='me', q=query).execute()
            messages = results.get('messages', [])
            
            if not messages:
                return {"status": "success", "message": f"No emails found from {sender_email}.", "labeled_count": 0}
            
            if not confirm:
                return {
                    "status": "confirmation_required",
                    "message": f"Found {len(messages)} emails from {sender_email}. Do you want to label them as '{label_name}'?",
                    "count": len(messages),
                    "total_estimated": len(messages),
                    "preview": self._build_preview(messages),
                    "action_details": {"action": "label_by_sender", "sender": sender_email, "label": label_name}
                }

            # Create label if it doesn't exist (only on confirm)
            self.create_label(label_name)
            label_id = self.get_label_id(label_name)
            if not label_id:
                return {"status": "error", "message": f"Could not create or find label '{label_name}'"}
            
            # Label the emails
            for message in messages:
                self.service.users().messages().modify(
                    userId='me', 
                    id=message['id'],
                    body={'addLabelIds': [label_id]}
                ).execute()
            action_id = self._record_undo('label_add', [m['id'] for m in messages], extra={'label_id': label_id})
            return {"status": "success", "message": f"Labeled {len(messages)} emails from {sender_email} as '{label_name}'.", "labeled_count": len(messages), "undo_action_id": action_id}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error labeling emails: {error}"}

    def label_emails_by_domain(self, domain, label_name, confirm=False):
        """Label all emails from a specific domain"""
        try:
            # Search for emails from the domain first
            query = f"from:*@{domain}"
            results = self.service.users().messages().list(
                userId='me', q=query).execute()
            messages = results.get('messages', [])
            
            if not messages:
                return {"status": "success", "message": f"No emails found from {domain}.", "labeled_count": 0}
            
            if not confirm:
                return {
                    "status": "confirmation_required",
                    "message": f"Found {len(messages)} emails from {domain}. Do you want to label them as '{label_name}'?",
                    "count": len(messages),
                    "total_estimated": len(messages),
                    "preview": self._build_preview(messages),
                    "action_details": {"action": "label_by_domain", "domain": domain, "label": label_name}
                }

            # Create label only on confirm
            self.create_label(label_name)
            label_id = self.get_label_id(label_name)
            if not label_id:
                return {"status": "error", "message": f"Could not create or find label '{label_name}'"}
            
            # Label the emails
            for message in messages:
                self.service.users().messages().modify(
                    userId='me', 
                    id=message['id'],
                    body={'addLabelIds': [label_id]}
                ).execute()
            action_id = self._record_undo('label_add', [m['id'] for m in messages], extra={'label_id': label_id})
            return {"status": "success", "message": f"Labeled {len(messages)} emails from {domain} as '{label_name}'.", "labeled_count": len(messages), "undo_action_id": action_id}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error labeling emails: {error}"}

    def label_emails_by_keywords(self, keywords, label_name, confirm=False):
        """Label emails containing specific keywords in subject"""
        try:
            # Create label if it doesn't exist
            self.create_label(label_name)
            label_id = self.get_label_id(label_name)
            
            if not label_id:
                return {"status": "error", "message": f"Could not create or find label '{label_name}'"}
            
            # Build search query for subject keywords
            keyword_query = " OR ".join([f'subject:"{keyword}"' for keyword in keywords])
            query = f"({keyword_query})"
            
            results = self.service.users().messages().list(
                userId='me', q=query).execute()
            messages = results.get('messages', [])
            
            if not messages:
                return {"status": "success", "message": f"No emails found with keywords: {', '.join(keywords)}.", "labeled_count": 0}
            
            if not confirm:
                return {
                    "status": "confirmation_required",
                    "count": len(messages),
                    "total_estimated": len(messages),
                    "preview": self._build_preview(messages),
                    "action_details": {"action": "label_by_keywords", "keywords": keywords, "label": label_name}
                }
            
            # Label the emails
            for message in messages:
                self.service.users().messages().modify(
                    userId='me', 
                    id=message['id'],
                    body={'addLabelIds': [label_id]}
                ).execute()
            
            return {"status": "success", "message": f"Labeled {len(messages)} emails with keywords {', '.join(keywords)} as '{label_name}'.", "labeled_count": len(messages)}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error labeling emails: {error}"}

    def list_labels(self):
        """List all Gmail labels"""
        try:
            labels = self.service.users().labels().list(userId='me').execute()
            user_labels = [label for label in labels.get('labels', []) if label['type'] == 'user']
            
            if not user_labels:
                return []
            
            return user_labels
            
        except HttpError as error:
            print(f"❌ Error listing labels: {error}")
            return []

    def get_emails_by_label(self, label_name, max_results=None, page_token=None):
        """Get emails with a specific label"""
        try:
            if max_results is None:
                max_results = self.default_max_results
            label_id = self.get_label_id(label_name)
            if not label_id:
                return []
            kwargs = {"userId":"me", "labelIds":[label_id], "maxResults": max_results}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.service.users().messages().list(**kwargs).execute()
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')
            if not messages:
                return {"emails": [], "next_page_token": None}
            email_list = []
            for message in messages:
                msg = self.service.users().messages().get(
                    userId='me', id=message['id']).execute()
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
                email_list.append({
                    'id': message['id'],
                    'subject': subject,
                    'sender': sender,
                    'snippet': msg.get('snippet', '')
                })
            return {"emails": email_list, "next_page_token": next_token}
        except HttpError as error:
            return {"emails": [], "next_page_token": None}

    def send_email(self, to, subject, message_text):
        """
        Create and send an email.
        Args:
            to: Email address of the receiver.
            subject: The subject of the email.
            message_text: The plain text body of the email.
        """
        try:
            message = MIMEText(message_text)
            message['to'] = to
            message['subject'] = subject
            # The 'from' address is automatically set to the authenticated user's email
            
            # Encode the message in a way that the Gmail API understands
            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            
            body = {'raw': raw_message}
            
            sent_message = self.service.users().messages().send(userId='me', body=body).execute()
            return {"success": True, "message_id": sent_message['id']}

        except HttpError as error:
            return {"success": False, "error": str(error)}

    def _record_undo(self, action_type, message_ids, extra=None):
        self._undo_counter += 1
        action_id = str(self._undo_counter)
        self._undo_store[action_id] = {
            'type': action_type,
            'message_ids': list(message_ids),
            'extra': extra or {}
        }
        return action_id

    def undo_action(self, action_id):
        entry = self._undo_store.get(action_id)
        if not entry:
            return {"status": "error", "message": _("Nothing to undo (expired or invalid).")}
        msg_ids = entry.get('message_ids', [])
        action_type = entry.get('type')
        try:
            if action_type == 'archive':
                # Restore to INBOX
                for i in range(0, len(msg_ids), 100):
                    batch_ids = msg_ids[i:i+100]
                    self.service.users().messages().batchModify(
                        userId='me',
                        body={'ids': batch_ids, 'addLabelIds': ['INBOX']}
                    ).execute()
                del self._undo_store[action_id]
                return {"status": "success", "message": _("Undo complete. Restored %(count)d emails to Inbox.") % {"count": len(msg_ids)}, "undone_count": len(msg_ids)}
            elif action_type == 'trash':
                # Untrash and add INBOX
                restored = 0
                for msg_id in msg_ids:
                    try:
                        self.service.users().messages().untrash(userId='me', id=msg_id).execute()
                        self.service.users().messages().modify(userId='me', id=msg_id, body={'addLabelIds': ['INBOX']}).execute()
                        restored += 1
                    except HttpError:
                        continue
                del self._undo_store[action_id]
                return {"status": "success", "message": _("Undo complete. Untrashed %(count)d emails.") % {"count": restored}, "undone_count": restored}
            elif action_type == 'label_add':
                # Remove label that was added
                label_id = entry['extra'].get('label_id')
                if not label_id:
                    return {"status": "error", "message": _("Cannot undo labels (missing label id).")}
                for i in range(0, len(msg_ids), 100):
                    batch_ids = msg_ids[i:i+100]
                    self.service.users().messages().batchModify(
                        userId='me',
                        body={'ids': batch_ids, 'removeLabelIds': [label_id]}
                    ).execute()
                del self._undo_store[action_id]
                return {"status": "success", "message": _("Removed label from %(count)d emails.") % {"count": len(msg_ids)}, "undone_count": len(msg_ids)}
            else:
                return {"status": "error", "message": _("Undo not supported for this action.")}
        except HttpError as e:
            return {"status": "error", "message": _("Undo failed: %(err)s") % {"err": e}}

    def get_recent_actions(self, limit=10):
        items = []
        for action_id, entry in list(self._undo_store.items())[-limit:]:
            items.append({
                'id': action_id,
                'type': entry.get('type'),
                'count': len(entry.get('message_ids', [])),
                'label_id': entry.get('extra', {}).get('label_id')
            })
        return items

    def _is_valid_email_address(self, address: str) -> bool:
        if not address or '@' not in address:
            return False
        # Basic RFC-like validation
        addr = parseaddr(address)[1]
        if not addr or '@' not in addr:
            return False
        # Exclude common non-address placeholders
        if addr.lower() in {"undisclosed-recipients", "mailer-daemon"}:
            return False
        # Simple domain check
        try:
            local, domain = addr.rsplit('@', 1)
        except ValueError:
            return False
        if not local or not domain or '.' not in domain:
            return False
        if len(domain.split('.')) < 2:
            return False
        # No spaces or angle brackets
        if any(ch in addr for ch in [' ', '<', '>', '"']):
            return False
        # Filter out typical non-human or list/bounce addresses
        lowered = addr.lower()
        undesirable_tokens = [
            'unsubscribe', 'bounce', 'bounces', 'no-reply', 'noreply', 'do-not-reply', 'donotreply'
        ]
        if any(tok in lowered for tok in undesirable_tokens):
            return False
        return True

    def get_recent_contacts(self, max_messages: int = 200, max_results: int = 200, cache_ttl_seconds: int = 600):
        """Return a list of recent contacts from Sent mail.

        Each item: {"email": str, "name": str, "count": int}
        Uses a short-lived cache to avoid repeated Gmail API calls.
        """
        # If Gmail is not set up
        if not self.service:
            return []
        # Serve from cache if fresh
        now = time.time()
        if self._contacts_cache is not None and (now - self._contacts_cache_ts) < cache_ttl_seconds:
            return self._contacts_cache

        try:
            # Collect message IDs from Sent mailbox
            collected_ids = []
            page_token = None
            while len(collected_ids) < max_messages:
                kwargs = {
                    "userId": 'me',
                    "labelIds": ['SENT'],
                    "maxResults": min(500, max_messages - len(collected_ids)),
                    "fields": 'messages/id,nextPageToken'
                }
                if page_token:
                    kwargs["pageToken"] = page_token
                res = self.service.users().messages().list(**kwargs).execute()
                msgs = res.get('messages', [])
                if not msgs:
                    break
                collected_ids.extend([m['id'] for m in msgs])
                page_token = res.get('nextPageToken')
                if not page_token:
                    break
        
            # Parse headers for To/Cc/Bcc to build contacts list
            email_to_count = {}
            email_to_name = {}
            for mid in collected_ids[:max_results]:
                try:
                    msg = self.service.users().messages().get(
                        userId='me', id=mid, format='metadata',
                        metadataHeaders=['To', 'Cc', 'Bcc'],
                        fields='payload/headers,id'
                    ).execute()
                    headers = msg.get('payload', {}).get('headers', [])
                    values = []
                    for nm in ['To', 'Cc', 'Bcc']:
                        v = next((h['value'] for h in headers if h.get('name') == nm), None)
                        if v:
                            values.append(v)
                    if not values:
                        continue
                    addrs = getaddresses(values)
                    for display_name, addr in addrs:
                        addr_norm = addr.strip()
                        if not self._is_valid_email_address(addr_norm):
                            continue
                        # Track name seen for this address
                        if addr_norm not in email_to_count:
                            email_to_count[addr_norm] = 0
                        email_to_count[addr_norm] += 1
                        if display_name and addr_norm not in email_to_name:
                            email_to_name[addr_norm] = display_name
                except HttpError:
                    continue
        
            contacts = []
            for addr, cnt in email_to_count.items():
                contacts.append({
                    "email": addr,
                    "name": email_to_name.get(addr, ''),
                    "count": cnt,
                })
            # Sort by frequency desc, then name/email
            contacts.sort(key=lambda x: (-x.get('count', 0), (x.get('name') or '').lower(), x['email'].lower()))

            # Cache and return
            self._contacts_cache = contacts
            self._contacts_cache_ts = now
            return contacts
        except HttpError:
            return []

# The main function is for command-line use and is not needed for the Django app.
# It will be replaced by Django views.
# def main():
#     ... (rest of the main function is removed) ...

# if __name__ == "__main__":
#     main() 
