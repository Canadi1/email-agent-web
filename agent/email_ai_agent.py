import os
import hashlib 
import pickle
import base64
import email
import json
import re
import time
import random
from datetime import datetime, timedelta
import re
try:
    from django.utils import translation as _dj_translation
except Exception:
    _dj_translation = None
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

# Minimal SSL configuration to reduce SSL errors without breaking authentication
import ssl
import urllib3

# Disable SSL warnings globally (safe)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    
    def _is_retryable_network_error(self, error):
        """Detect retryable SSL/connection errors from googleapiclient/requests."""
        try:
            message = str(error)
        except Exception:
            message = ""
        text = message.lower()
        retry_tokens = [
            "ssl",
            "certificate",
            "wrong_version_number",
            "wrong version number",
            "timed out",
            "timeout",
            "read timed out",
            "connection reset",
            "connection aborted",
            "connection refused",
            "remote end closed connection",
            "max retries exceeded",
            "temporarily unavailable",
            "name or service not known",
            "gaierror",
            "dns",
            "proxy",
        ]
        return any(token in text for token in retry_tokens)

    def _execute_with_retries(self, request, max_retries=5, base_delay=1.0):
        """Execute a googleapiclient request with retries on SSL/connection failures.
        Uses exponential backoff with jitter.
        """
        last_exc = None
        for attempt in range(max_retries):
            try:
                # Avoid library-internal retries to keep control here
                return request.execute(num_retries=0)
            except Exception as exc:
                last_exc = exc
                if attempt < max_retries - 1 and self._is_retryable_network_error(exc):
                    delay = base_delay * (2 ** attempt)
                    jitter = random.uniform(0, max(0.1, 0.25 * delay))
                    time.sleep(delay + jitter)
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Unknown error executing Gmail request with retries")

    # Centralized Gmail API wrappers (use retries consistently)
    def api_list_messages(self, user_id='me', **kwargs):
        # Avoid passing userId twice if provided in kwargs
        if 'userId' in kwargs:
            user_id = kwargs.pop('userId') or user_id
        return self._execute_with_retries(
            self.service.users().messages().list(userId=user_id, **kwargs),
            max_retries=5,
            base_delay=1.0,
        )

    def api_get_message(self, message_id, user_id='me', **kwargs):
        # Avoid passing userId twice if provided in kwargs
        if 'userId' in kwargs:
            user_id = kwargs.pop('userId') or user_id
        return self._execute_with_retries(
            self.service.users().messages().get(userId=user_id, id=message_id, **kwargs),
            max_retries=4,
            base_delay=0.8,
        )

    def api_batch_modify(self, message_ids, add_label_ids=None, remove_label_ids=None, user_id='me'):
        body = {
            'ids': list(message_ids) if message_ids else [],
        }
        if add_label_ids:
            body['addLabelIds'] = list(add_label_ids)
        if remove_label_ids:
            body['removeLabelIds'] = list(remove_label_ids)
        return self._execute_with_retries(
            self.service.users().messages().batchModify(userId=user_id, body=body),
            max_retries=5,
            base_delay=1.0,
        )

    def api_modify(self, message_id, add_label_ids=None, remove_label_ids=None, user_id='me'):
        body = {}
        if add_label_ids:
            body['addLabelIds'] = list(add_label_ids)
        if remove_label_ids:
            body['removeLabelIds'] = list(remove_label_ids)
        return self._execute_with_retries(
            self.service.users().messages().modify(userId=user_id, id=message_id, body=body),
            max_retries=5,
            base_delay=1.0,
        )

    def api_trash(self, message_id, user_id='me'):
        return self._execute_with_retries(
            self.service.users().messages().trash(userId=user_id, id=message_id),
            max_retries=5,
            base_delay=1.0,
        )

    def api_untrash(self, message_id, user_id='me'):
        return self._execute_with_retries(
            self.service.users().messages().untrash(userId=user_id, id=message_id),
            max_retries=5,
            base_delay=1.0,
        )

    def api_list_labels(self, user_id='me'):
        return self._execute_with_retries(
            self.service.users().labels().list(userId=user_id),
            max_retries=5,
            base_delay=1.0,
        )

    def api_create_label(self, label_object, user_id='me'):
        return self._execute_with_retries(
            self.service.users().labels().create(userId=user_id, body=label_object),
            max_retries=5,
            base_delay=1.0,
        )

    def api_send_message(self, body, user_id='me'):
        return self._execute_with_retries(
            self.service.users().messages().send(userId=user_id, body=body),
            max_retries=5,
            base_delay=1.0,
        )
    
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

                # Apply basic retry configuration
                try:
                    import requests
                    requests.adapters.DEFAULT_RETRIES = 3
                except Exception:
                    pass

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
        try:
            if max_results is None:
                max_results = self.default_max_results
            kwargs = {"userId": 'me', "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.api_list_messages(**kwargs)
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')
            email_list = []
            total_emails = len(messages)
            
            # Update progress with total emails
            if hasattr(self, 'command_id') and self.command_id:
                from agent.views import update_email_progress
                update_email_progress(self.command_id, 0, total_emails)
            
            for i, message in enumerate(messages):
                msg = self.api_get_message(
                    message['id'],
                    format='metadata', metadataHeaders=['From','Subject'],
                    fields='payload/headers,id,internalDate'
                )
                headers = msg['payload']['headers']
                
                # Update progress
                if hasattr(self, 'command_id') and self.command_id:
                    update_email_progress(self.command_id, i + 1, total_emails)
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
                
                # Add date information like in other list functions
                date_str = ''
                try:
                    ts_ms = int(msg.get('internalDate', '0') or '0')
                    if ts_ms:
                        date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                except Exception:
                    date_str = ''
                
                email_list.append({
                    'id': message['id'],
                    'subject': subject,
                    'sender': sender,
                    'date': date_str,
                    'snippet': ''
                })
            return {"emails": email_list, "next_page_token": next_token}
        except Exception as e:
            return {"emails": [], "next_page_token": None, "error": str(e)}
    
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
            results = self.api_list_messages(**kwargs)
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')

            email_list = []
            for message in messages:
                msg = self.api_get_message(
                    message['id'],
                    format='metadata', metadataHeaders=['From','Subject'],
                    fields='payload/headers,id'
                )
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
        print(f"DEBUG: list_emails_by_domain called with domain={domain}, command_id={getattr(self, 'command_id', None)}")
        print(f"DEBUG: hasattr command_id: {hasattr(self, 'command_id')}")
        print(f"DEBUG: self.command_id value: {getattr(self, 'command_id', 'NOT_SET')}")
        
        try:
            if max_results is None: 
                max_results = self.default_max_results
            # Use Gmail domain search with wildcard to match any local part
            query = f"from:*@{domain}"
            if older_than_days:
                cutoff_date = (datetime.utcnow() - timedelta(days=int(older_than_days))).strftime('%Y/%m/%d')
                query = f"{query} before:{cutoff_date}"
            if date_range:
                try:
                    # Use precise calendar window logic for "from [duration] ago" commands
                    start_date, end_date = self._compute_precise_date_range_window(date_range)
                    if start_date and end_date:
                        query = f"{query} after:{start_date.strftime('%Y/%m/%d')} before:{end_date.strftime('%Y/%m/%d')}"
                except Exception:
                    pass
            kwargs = {"userId": 'me', "q": query, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.api_list_messages(**kwargs)
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')
            email_list = []
            total_emails = len(messages)
            
            # Update progress with total emails
            if hasattr(self, 'command_id') and self.command_id:
                from agent.views import update_email_progress
                update_email_progress(self.command_id, 0, total_emails)
            
            for i, message in enumerate(messages):
                msg = self.api_get_message(
                    message['id'],
                    format='metadata', metadataHeaders=['From','Subject'],
                    fields='payload/headers,id,internalDate'
                )
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')

                # Add date information
                date_str = ''
                try:
                    ts_ms = int(msg.get('internalDate', '0') or '0')
                    if ts_ms:
                        date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                except Exception:
                    date_str = ''
                
                email_list.append({
                    'id': message['id'],
                    'subject': subject,
                    'sender': sender,
                    'date': date_str,
                    'snippet': ''
                })
                
                # Update progress
                if hasattr(self, 'command_id') and self.command_id:
                    update_email_progress(self.command_id, i + 1, total_emails)
            
            return {"emails": email_list, "next_page_token": next_token}
        except Exception as e:
            print(f"❌ Error in list_emails_by_domain: {e}")
            return {"emails": [], "next_page_token": None}

    def list_emails_by_sender(self, sender_keyword, max_results=None, page_token=None, older_than_days=None, date_range=None):
        """List emails from a sender containing specific keyword with pagination."""
        try:
            if max_results is None:
                max_results = self.default_max_results
            # Quote multi-word senders for exact match within 'from:' operator
            sender_term = f'"{sender_keyword}"' if ' ' in str(sender_keyword).strip() else sender_keyword
            query = f"from:{sender_term}"
            if older_than_days:
                cutoff_date = (datetime.utcnow() - timedelta(days=int(older_than_days))).strftime('%Y/%m/%d')
                query = f"{query} before:{cutoff_date}"
            if date_range:
                try:
                    # Use precise calendar window logic for "from [duration] ago" commands
                    start_date, end_date = self._compute_precise_date_range_window(date_range)
                    if start_date and end_date:
                        query = f"{query} after:{start_date.strftime('%Y/%m/%d')} before:{end_date.strftime('%Y/%m/%d')}"
                except Exception:
                    pass
            kwargs = {"userId": 'me', "q": query, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
            if page_token:
                kwargs["pageToken"] = page_token
            results = self.api_list_messages(**kwargs)
            messages = results.get('messages', [])
            next_token = results.get('nextPageToken')
            email_list = []

            # Initialize progress with total emails
            total_emails = len(messages)
            if hasattr(self, 'command_id') and self.command_id:
                from agent.views import update_email_progress
                update_email_progress(self.command_id, 0, total_emails)

            for i, message in enumerate(messages):
                msg = self.api_get_message(
                    message['id'],
                    format='metadata', metadataHeaders=['From','Subject'],
                    fields='payload/headers,id,internalDate'
                )
                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')

                # Add date information
                date_str = ''
                try:
                    ts_ms = int(msg.get('internalDate', '0') or '0')
                    if ts_ms:
                        date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                except Exception:
                    date_str = ''
                
                email_list.append({
                    'id': message['id'],
                    'subject': subject,
                    'sender': sender,
                    'date': date_str,
                    'snippet': ''
                })

                # Update progress after processing each message
                if hasattr(self, 'command_id') and self.command_id:
                    update_email_progress(self.command_id, i + 1, total_emails)

            # Build snackbar message with details (older_than/date_range)
            extra_parts = []
            if older_than_days:
                try:
                    extra_parts.append(_("older than %(days)d days") % {"days": int(older_than_days)})
                except Exception:
                    extra_parts.append(_("older than %(text)s") % {"text": str(older_than_days)})
            if date_range:
                try:
                    extra_parts.append(_("in %(range)s") % {"range": str(date_range)})
                except Exception:
                    pass
            extra_text = (" " + " and ".join(extra_parts)) if extra_parts else ""
            message_text = _("Found %(count)d emails from %(who)s%(extra)s.") % {"count": len(messages), "who": sender_keyword, "extra": extra_text}

            return {"message": message_text, "emails": email_list, "next_page_token": next_token}
        except Exception:
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
                    results = self.api_list_messages(q=query, pageToken=next_page_token)
                else:
                    results = self.api_list_messages(q=query)
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
                    self.api_batch_modify(message_ids_batch, add_label_ids=['TRASH'], remove_label_ids=['INBOX'])
                    total_processed += len(batch)
                except HttpError as e:
                    for m in batch:
                        try:
                            self.api_trash(m['id'])
                            total_processed += 1
                        except HttpError:
                            continue

            action_id = self._record_undo('trash', message_ids)
            return {"status": "success", "message": _("Trashed %(count)d emails older than %(days)d days") % {"count": total_processed, "days": older_than_days}, "deleted_count": total_processed, "undo_action_id": action_id}

        except HttpError as error:
            return {"status": "error", "message": f"Error deleting emails by age: {error}"}
    
    def archive_emails_by_age_only(self, older_than_days, confirm=False):
        """Archive all emails older than specified days (bulk cleanup)."""
        max_retries = 5
        retry_delay = 1
        
        for attempt in range(max_retries):
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
                    
                    try:
                        results = self.service.users().messages().list(**request_body).execute()
                        messages = results.get('messages', [])
                        all_messages.extend(messages)
                        
                        next_page_token = results.get('nextPageToken')
                        if not next_page_token:
                            break
                    except Exception as e:
                        err_text = str(e)
                        if (("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in err_text or
                             "timeout" in err_text.lower() or "connection" in err_text.lower() or
                             "WinError 10060" in err_text or "failed to respond" in err_text.lower())
                            and attempt < max_retries - 1):
                            print(f"Connection error in archive_emails_by_age_only (attempt {attempt + 1}): {err_text}")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            break  # Break out of while loop to retry the whole function
                        else:
                            raise e
                
                # If we get here, the operation was successful
                break
                
            except Exception as e:
                error_msg = str(e)
                print(f"Error in archive_emails_by_age_only (attempt {attempt + 1}): {error_msg}")
                if ("timeout" in error_msg.lower() or "connection" in error_msg.lower() or
                    "WinError 10060" in error_msg or "failed to respond" in error_msg.lower() or
                    "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg):
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue  # Continue to next attempt in outer for loop
                return {"error": f"Error archiving emails by age: {error_msg}"}
        
        try:

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
            total_emails = len(all_messages)
            message_ids = [m['id'] for m in all_messages]
            
            # Update progress with total emails
            if hasattr(self, 'command_id') and self.command_id:
                from agent.views import update_email_progress
                update_email_progress(self.command_id, 0, total_emails)
            
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
                    
                    # Update progress
                    if hasattr(self, 'command_id') and self.command_id:
                        update_email_progress(self.command_id, total_processed, total_emails)
                except HttpError as e:
                    # Fallback to individual calls for this batch
                    for msg_id in message_ids_batch:
                        try:
                            self.api_modify(msg_id, remove_label_ids=['INBOX'])
                            total_processed += 1
                            
                            # Update progress
                            if hasattr(self, 'command_id') and self.command_id:
                                update_email_progress(self.command_id, total_processed, total_emails)
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
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        page = self.api_list_messages(**kwargs)
                        break
                    except Exception as e:
                        err_text = str(e)
                        if ("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text) and attempt < max_retries - 1:
                            # Exponential backoff for SSL errors: 1s, 2s, 4s, 8s, 16s
                            backoff_time = 2 ** attempt
                            time.sleep(backoff_time)
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
                    max_retries = 5
                    for attempt in range(max_retries):
                        try:
                            page = self.service.users().messages().list(**kwargs).execute()
                            break
                        except Exception as e:
                            err_text = str(e)
                            if ("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text) and attempt < max_retries - 1:
                                # Exponential backoff for SSL errors: 1s, 2s, 4s, 8s, 16s
                                backoff_time = 2 ** attempt
                                time.sleep(backoff_time)
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
                        max_retries = 5
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
        # Don't set a default max_results - let it be None to fetch all emails
        # The user's page size will be handled by the UI pagination
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
        elif date_range_str == "this week":
            start_of_this_week = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = start_of_this_week
            end_date = (today + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            description = _("from this week")
        elif date_range_str == "last week":
            start_of_this_week = today - timedelta(days=today.weekday())
            start_date = (start_of_this_week - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=7)
            description = _("from last week")
        elif date_range_str in ["this month", "last month", "this year", "last year"]:
            # Handle "last month" and "last year" specifically
            if date_range_str == "this month":
                # From first of this month (inclusive) to tomorrow (exclusive)
                start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                end_date = (today + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                description = _("from this month")
            elif date_range_str == "last month":
                # Inclusive last month: from the first day of last month to the first day of this month (exclusive)
                start_date = (today.replace(day=1) - relativedelta(months=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = today.replace(day=1).replace(hour=0, minute=0, second=0, microsecond=0)
                description = _("from last month")
            elif date_range_str == "this year":
                # Use year chunking to prevent timeouts on large date ranges
                return self._list_emails_this_year_chunked(max_results, page_token)
            elif date_range_str == "last year":
                # Inclusive last year: from Jan 1 last year to Jan 1 this year (exclusive)
                start_date = today.replace(month=1, day=1) - relativedelta(years=1)
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = today.replace(month=1, day=1).replace(hour=0, minute=0, second=0, microsecond=0)
                description = _("from last year")
        else:
            parts = date_range_str.split()
            # Handle "ago" patterns (e.g., "2 months ago", "3 weeks ago", "a month ago")
            if len(parts) == 3 and parts[2] == "ago":
                # Handle "a [unit] ago" and numeric patterns
                if parts[0] == "a":
                    quantity = 1
                    unit = parts[1]
                elif parts[0].isdigit():
                    quantity = int(parts[0])
                    unit = parts[1]
                else:
                    quantity = None
                    unit = None
                
                if quantity is not None and unit:
                    # Compute strict calendar windows with EXCLUSIVE end bounds
                    if "day" in unit:
                        start_date = (today - timedelta(days=quantity)).replace(hour=0, minute=0, second=0, microsecond=0)
                        end_date = start_date + timedelta(days=1)  # exclusive
                        description = _("from %(quantity)d %(unit)s ago") % {"quantity": quantity, "unit": unit}
                    elif "week" in unit:
                        # Weeks start on Monday
                        anchor = today - timedelta(weeks=quantity)
                        start_date = (anchor - timedelta(days=anchor.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
                        end_date = start_date + timedelta(days=7)  # exclusive
                        description = _("from %(quantity)d %(unit)s ago") % {"quantity": quantity, "unit": unit}
                    elif "month" in unit:
                        target_month = today - relativedelta(months=quantity)
                        start_date = target_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        end_date = start_date + relativedelta(months=1)  # exclusive (first day of next month)
                        description = _("from %(quantity)d %(unit)s ago") % {"quantity": quantity, "unit": unit}
                    elif "year" in unit:
                        target_year = today.year - quantity
                        start_date = datetime(target_year, 1, 1, 0, 0, 0)
                        end_date = datetime(target_year + 1, 1, 1, 0, 0, 0)  # exclusive (Jan 1 of next year)
                        description = _("from %(quantity)d %(unit)s ago") % {"quantity": quantity, "unit": unit}
            # Handle patterns without "ago" (e.g., "2 days", "3 weeks")
            elif len(parts) == 2 and parts[0].isdigit():
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

        # Build query using EXCLUSIVE end bound (Gmail before: is exclusive at that date)
        query = f"after:{start_date.strftime('%Y/%m/%d')} before:{end_date.strftime('%Y/%m/%d')}"
        
        # Debug: Print the query being used
        print(f"DEBUG: Date range query: {query}")
        print(f"DEBUG: Max results: {max_results}")
        
        result = self._execute_list_query(query, description, max_results, page_token=page_token)
        # pass through next page token
        return result

    def _compute_precise_date_range_window(self, date_range_str):
        """
        Compute precise calendar windows for "from [duration] ago" and "last [period]" commands.
        Returns (start_date, end_date) with exclusive end bounds.
        """
        today = datetime.now()
        start_date = None
        end_date = None
        
        parts = date_range_str.split()
        
        # Handle "ago" patterns (e.g., "2 months ago", "3 weeks ago", "a month ago")
        if len(parts) == 3 and parts[2] == "ago":
            # Handle "a [unit] ago" and numeric patterns
            if parts[0] == "a":
                quantity = 1
                unit = parts[1]
            elif parts[0].isdigit():
                quantity = int(parts[0])
                unit = parts[1]
            else:
                return None, None
            
            # Compute strict calendar windows with EXCLUSIVE end bounds
            if "day" in unit:
                start_date = (today - timedelta(days=quantity)).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=1)  # exclusive
            elif "week" in unit:
                # Weeks start on Monday
                anchor = today - timedelta(weeks=quantity)
                start_date = (anchor - timedelta(days=anchor.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=7)  # exclusive
            elif "month" in unit:
                target_month = today - relativedelta(months=quantity)
                start_date = target_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + relativedelta(months=1)  # exclusive (first day of next month)
            elif "year" in unit:
                target_year = today.year - quantity
                start_date = datetime(target_year, 1, 1, 0, 0, 0)
                end_date = datetime(target_year + 1, 1, 1, 0, 0, 0)  # exclusive (Jan 1 of next year)
        
        # Handle "last" patterns (e.g., "last week", "last month", "last year")
        elif len(parts) == 2 and parts[0] == "last":
            unit = parts[1]
            
            if "day" in unit:
                # Last day = yesterday
                start_date = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=1)  # exclusive
            elif "week" in unit:
                # Last week = previous Monday to Sunday
                last_week_start = today - timedelta(weeks=1)
                start_date = (last_week_start - timedelta(days=last_week_start.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=7)  # exclusive
            elif "month" in unit:
                # Last month = previous month (1st to last day)
                last_month = today - relativedelta(months=1)
                start_date = last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + relativedelta(months=1)  # exclusive (first day of current month)
            elif "year" in unit:
                # Last year = previous year (Jan 1 to Dec 31)
                last_year = today.year - 1
                start_date = datetime(last_year, 1, 1, 0, 0, 0)
                end_date = datetime(last_year + 1, 1, 1, 0, 0, 0)  # exclusive (Jan 1 of current year)
        
        # Handle "this" patterns (e.g., "this week", "this month", "this year")
        elif len(parts) == 2 and parts[0] == "this":
            unit = parts[1]
            
            if "day" in unit:
                # This day = today
                start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=1)  # exclusive
            elif "week" in unit:
                # This week = current Monday to Sunday
                start_date = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=7)  # exclusive
            elif "month" in unit:
                # This month = current month (1st to last day)
                start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + relativedelta(months=1)  # exclusive (first day of next month)
            elif "year" in unit:
                # This year = current year (Jan 1 to Dec 31)
                start_date = datetime(today.year, 1, 1, 0, 0, 0)
                end_date = datetime(today.year + 1, 1, 1, 0, 0, 0)  # exclusive (Jan 1 of next year)
        
        # Handle single word patterns (e.g., "today", "yesterday")
        elif len(parts) == 1:
            if parts[0] == "today":
                # Today = current day (00:00 to 00:00 exclusive)
                start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=1)  # exclusive
            elif parts[0] == "yesterday":
                # Yesterday = previous day (00:00 to 00:00 exclusive)
                start_date = (today - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=1)  # exclusive
        
        return start_date, end_date

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

    def _list_emails_this_year_chunked(self, max_results=None, page_token=None):
        """
        Lists emails from this year using smart chunking to prevent timeouts.
        """
        if max_results is None:
            max_results = self.default_max_results
        
        today = datetime.now()
        current_year = today.year
        
        # Try the full year first for reasonable page sizes
        if max_results <= 200:
            start_date = datetime(current_year, 1, 1, 0, 0, 0)
            end_date = (today + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            
            query = f"after:{start_date.strftime('%Y/%m/%d')} before:{end_date.strftime('%Y/%m/%d')}"
            description = _("from this year")
            
            print(f"Searching entire year at once...")
            result = self._execute_list_query(query, description, max_results=max_results, page_token=page_token)
            
            if result and "emails" in result:
                return result
            else:
                print("Full year search failed, falling back to monthly chunks...")
        
        # Fallback to monthly chunks for larger page sizes or if full year fails
        # Search in reverse chronological order (most recent first)
        months = [
            (12, 31), (11, 30), (10, 31), (9, 30), (8, 31), (7, 31),
            (6, 30), (5, 31), (4, 30), (3, 31), (2, 28), (1, 31)
        ]
        
        # Handle leap year for February (now at index 10 in reverse order)
        if current_year % 4 == 0 and (current_year % 100 != 0 or current_year % 400 == 0):
            months[10] = (2, 29)  # Feb 29 in leap year
        
        all_emails = []
        
        for month, max_day in months:
            # Skip future months (since we're going in reverse order)
            if month > today.month:
                continue
                
            # For current month, go up to today + 1 to include today's emails
            if month == today.month:
                max_day = today.day + 1
            
            # Create date range for this month
            start_date = datetime(current_year, month, 1, 0, 0, 0)
            end_date = datetime(current_year, month, max_day, 23, 59, 59)
            
            # Skip if we're past today
            if start_date > today:
                continue
                
            # Use inclusive date range to ensure we get today's emails
            query = f"after:{start_date.strftime('%Y/%m/%d')} before:{(end_date + timedelta(days=1)).strftime('%Y/%m/%d')}"
            description = f"from {start_date.strftime('%B %Y')}"
            
            print(f"Searching {description}...")
            # Use the full page size for each month search
            chunk_size = max_results
            result = self._execute_list_query(query, description, max_results=chunk_size, page_token=None)
            
            if result and "emails" in result:
                emails = result["emails"]
                all_emails.extend(emails)
                print(f"Found {len(emails)} emails in {description} (Total: {len(all_emails)})")
                
                # Stop if we've reached the max results
                if len(all_emails) >= max_results:
                    print(f"Reached max results limit ({max_results}), stopping search")
                    break
            else:
                print(f"No emails found in {description}")
        
        # Sort by date (newest first) - handle emails without date field
        def get_sort_key(email):
            try:
                if not isinstance(email, dict):
                    return '9999-12-31'  # Put non-dict items at the end
                
                # Try different possible date field names
                date_str = email.get('date', '') or email.get('timestamp', '') or email.get('received_date', '')
                
                if not date_str:
                    # If no date field, use a default sort order
                    return '9999-12-31'
                elif isinstance(date_str, str):
                    return date_str
                elif hasattr(date_str, 'strftime'):
                    return date_str.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return str(date_str)
            except Exception as e:
                print(f"Error sorting email: {e}, email type: {type(email)}")
                return '9999-12-31'  # Put problematic items at the end
        
        # Only sort if we have emails
        if all_emails:
            all_emails.sort(key=get_sort_key, reverse=True)
        
        # Limit to requested max_results
        if len(all_emails) > max_results:
            all_emails = all_emails[:max_results]
        
        return {
            "emails": all_emails,
            "total_count": len(all_emails),
            "description": _("from this year"),
            "chunked": True
        }

    def _execute_list_query(self, query, description, max_results=None, page_token=None):
        """A helper function to execute list queries and print results."""
        max_retries = 8  # Increased retries
        retry_delay = 1  # Start with shorter delay
        results = None  # Initialize results to avoid UnboundLocalError
        
        for attempt in range(max_retries):
            try:
                # Use the specified max_results or default
                if max_results is None:
                    max_results = self.default_max_results
                kwargs = {"userId":"me", "q": query, "maxResults": max_results}
                if page_token:
                    kwargs["pageToken"] = page_token
                
                # Execute via centralized wrapper
                results = self.api_list_messages(**kwargs)
                
                # If we get here, the operation was successful (inner while loop broke)
                break  # Break from outer for loop
            except Exception as e:
                error_msg = str(e)
                print(f"Error in _execute_list_query (attempt {attempt + 1}): {error_msg}")
                if ("timeout" in error_msg.lower() or "connection" in error_msg.lower() or 
                    "WinError 10060" in error_msg or "failed to respond" in error_msg.lower() or
                    "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg):
                    if attempt < max_retries - 1:
                        # Add randomization to prevent thundering herd
                        import random
                        jitter = random.uniform(0.5, 1.5)
                        actual_delay = retry_delay * jitter
                        print(f"Retrying in {actual_delay:.1f} seconds...")
                        time.sleep(actual_delay)
                        retry_delay *= 2
                        continue  # Continue to next attempt in outer for loop
                    else:
                        print(f"❌ SSL/Connection error after {max_retries} attempts: {error_msg}")
                        return {"error": f"Connection error after {max_retries} attempts: {error_msg}"}
                else:
                    # Non-SSL error, don't retry
                    return {"error": f"Error executing search query: {error_msg}"}
        
        # Check if results was successfully obtained
        if results is None:
            return {"error": "Failed to execute search query after all retries"}
        
        # Process the results
        messages = results.get('messages', [])
        next_token = results.get('nextPageToken')
        if not messages:
            return {"message": _("No emails found %(description)s.") % {"description": description}, "emails": [], "next_page_token": None}
        email_list = []
        total_emails = len(messages)
        
        # Update progress with total emails
        if hasattr(self, 'command_id') and self.command_id:
            from agent.views import update_email_progress
            update_email_progress(self.command_id, 0, total_emails)
        
        for i, message in enumerate(messages):
            msg = self.api_get_message(
                message['id'],
                format='metadata', metadataHeaders=['From', 'Subject'],
                fields='id,internalDate,payload/headers')
            headers = msg['payload']['headers']
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
            sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
            
            # Add date information like in preview function
            date_str = ''
            try:
                ts_ms = int(msg.get('internalDate', '0') or '0')
                if ts_ms:
                    date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
            except Exception:
                date_str = ''
            
            email_list.append({"sender": sender, "subject": subject, "date": date_str})
            
            # Update progress
            if hasattr(self, 'command_id') and self.command_id:
                update_email_progress(self.command_id, i + 1, total_emails)
        return {"message": _("Found %(count)d emails %(description)s.") % {"count": len(messages), "description": description}, "emails": email_list, "next_page_token": next_token}

    def _build_preview(self, messages, limit=10):
        """Build up to 'limit' preview rows for confirmation dialogs."""
        preview_items = []
        try:
            subset = messages[:limit] if isinstance(messages, list) else []
            for m in subset:
                try:
                    msg = self.api_get_message(
                        m['id'],
                        format='metadata', metadataHeaders=['From','Subject'],
                        fields='id,internalDate,snippet,payload/headers'
                    )
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
            # Search across All Mail, but avoid Promotions/Social/Forums and Spam/Trash.
            base = 'in:anywhere -in:spam -in:trash -category:promotions -category:social -category:forums'
            # Strong subject phrases across locales
            subject_terms = [
                '"verification code"', '"security code"', '"authentication code"', '"auth code"',
                '"one time code"', '"one-time code"', '"one-time passcode"', 'passcode',
                'OTP', '"2FA"', '"two-factor"', '"two step"', '"two-step"',
                '"login code"', '"sign-in code"', '"sign in code"', '"your code"', '"your verification code"',
                '"login verification"', '"sign-in verification"', '"sign in verification"',
                'subject:(verification login)', 'subject:(verification "sign-in")', 'subject:(verification "sign in")',
                # Hebrew and other locales
                '"קוד אימות"', '"סיסמה חד-פעמית"', '"קוד חד פעמי"', '"קוד כניסה"', '"קוד אבטחה"', '"אימות דו שלבי"', '"אימות דו-שלבי"',
                '验证码', '"код подтверждения"', '"code de vérification"', '"código de verificación"'
            ]
            # Common content phrases (searches body/subject when not prefixed with subject:)
            content_phrases = [
                '"your code is"', '"use this code"', '"enter this code"', '"is your verification code"',
                '"login verification"', '"sign-in verification"', '"sign in verification"'
            ]
            positive = 'subject:(' + ' OR '.join(subject_terms) + ')' + ' OR (' + ' OR '.join(content_phrases) + ')'
            # Exclude obvious marketing/discount code noise in both subject and body
            negative_subject = '-subject:("promo code" OR "discount code" OR "coupon code" OR "voucher code" OR promo OR promotion OR promotional OR coupon OR discount OR voucher OR offer OR sale)'
            negative_body = '-("promo code" OR "discount code" OR "coupon code" OR "voucher code")'
            q = f"{base} ( {positive} ) {negative_subject} {negative_body}"
        elif category_key == 'shipping_delivery':
            # Search across All Mail (exclude obvious ad categories)
            base = 'in:anywhere -category:promotions -category:social -category:forums'
            # Positive shipping subject phrases (strict), including Hebrew variants
            subject_core = 'subject:("your order has shipped" OR "order shipped" OR "has shipped" OR "out for delivery" OR "tracking number" OR "track your package" OR "in transit" OR "estimated delivery" OR "delivery estimate" OR "expected delivery" OR "ready for pickup" OR "ready for collection" OR "on the way" OR "on its way" OR "dispatched" OR "dispatch" OR "shipment update" OR "tracking update" OR "track order" OR "track shipment" OR "delivery attempt" OR "left the warehouse" OR "package shipped" OR "משלוח" OR "נשלחה" OR "נשלח" OR "מספר מעקב" OR "בהובלה" OR "נמסרה" OR "בהגעה" OR "איסוף" OR "מוכן לאיסוף")'
            # Require context when using broader words
            arrive_clause = '(subject:(arriving OR arrives) subject:(order OR package OR delivery OR parcel))'
            delivered_clause = '(subject:delivered subject:(order OR package OR delivery OR parcel))'
            update_clause = '(subject:(update OR updated) subject:(shipment OR shipping OR delivery OR tracking))'
            # Known carriers (kept as a supplement, not brand ads)
            carrier_domains = ['ups.com','fedex.com','dhl.com','usps.com','canadapost.ca','royalmail.com','israelpost.co.il','17track.net','aftership.com','cainiao.com']
            carriers_clause = '(' + ' OR '.join([f'from:*@{d}' for d in carrier_domains]) + ')'
            # Marketplaces that frequently send shipping updates (paired with shipping context to reduce noise)
            marketplace_domains = ['aliexpress.com','aliexpress.us','amazon.com','ebay.com','shein.com','temu.com','walmart.com','bestbuy.com']
            subject_context = '(subject:(order OR package OR parcel OR delivery OR shipment OR shipped OR tracking))'
            marketplaces_clause = '(' + ' OR '.join([f'(from:*@{d} {subject_context})' for d in marketplace_domains]) + ')'
            # Negatives to drop ads
            negative = '-subject:(sale OR discount OR coupon OR promo OR promotional OR offer OR deal OR wishlist OR cart OR recommendations OR recommendation)'
            q = f"{base} ( {subject_core} OR {arrive_clause} OR {delivered_clause} OR {update_clause} OR {carriers_clause} OR {marketplaces_clause} ) {negative}"
        elif category_key == 'account_security':
            # Security/account alerts across All Mail; match strict subject/body phrases and exclude common noise
            base = 'in:anywhere -in:spam -in:trash -category:promotions -category:social -category:forums'
            subjects = [
                '"security alert"', '"security notification"', '"account security"',
                '"new sign in"', '"new sign-in"', '"new signin"', '"new login"',
                '"sign-in attempt"', '"login attempt"', '"sign in attempt"',
                '"suspicious activity"', '"unusual activity"', '"verify it\'s you"', '"verify your identity"',
                '"password changed"', '"password was changed"', '"reset your password"', '"password reset"',
                '"two-step verification"', '"2-step verification"', '"two-factor authentication"',
                '"new device"', '"new device sign-in"', '"new device login"', '"your account was accessed"',
                '"login verification"', '"sign-in verification"', '"sign in verification"'
            ]
            subject_clause = 'subject:(' + ' OR '.join(subjects) + ')'
            # Also match common body phrases seen in security alerts
            content_phrases = [
                '"we detected a new sign-in"', '"we noticed a new sign-in"', '"if this wasn\'t you"',
                '"your account was accessed"', '"sign-in attempt prevented"', '"we blocked a sign-in attempt"'
            ]
            content_clause = '(' + ' OR '.join(content_phrases) + ')'
            # Exclude purchases/newsletters/school noise; avoid removing legitimate "confirmation" by scoping
            negative_subject = '-subject:(sale OR discount OR coupon OR promo OR promotional OR offer OR deal OR newsletter OR receipt OR order OR purchase OR invoice OR payment OR transaction OR classroom OR course OR class OR assignment OR homework OR grade OR exam OR university OR school OR student OR tuition OR "order confirmation" OR "purchase confirmation" OR "subscription confirmation")'
            negative_body = '-("order confirmation" OR "purchase confirmation" OR "subscription confirmation")'
            q = f"{base} ( {subject_clause} OR {content_clause} ) {negative_subject} {negative_body}"
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
        elif key == "this week":
            start_of_this_week = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = start_of_this_week
            end_date = (today + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif key == "last week":
            start_of_this_week = today - timedelta(days=today.weekday())
            start_date = (start_of_this_week - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=7)
        elif key == "this month":
            start_date = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end_date = (today + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif key == "last month":
            # First day of current month is the exclusive upper bound
            end_date = today.replace(day=1)
            # First day of last month as inclusive lower bound
            start_date = (end_date - relativedelta(months=1))
        elif key == "this year":
            start_date = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_date = (today + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
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
        max_retries = 5
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                if max_results is None:
                    max_results = self.default_max_results
                q = self._build_custom_category_q(category_key, older_than_days=older_than_days)
                # Optional explicit date window for 'from time period' and 'from N <unit> ago'
                if date_range:
                    try:
                        # Normalize to a phrase understood by the precise window calculator
                        dr = str(date_range).strip().lower()
                        if not any(tok in dr for tok in ["ago", "today", "yesterday", "this", "last"]):
                            dr = f"{dr} ago"
                        start_dt, end_dt = self._compute_precise_date_range_window(dr)
                        if start_dt and end_dt:
                            q = f"({q}) after:{start_dt.strftime('%Y/%m/%d')} before:{end_dt.strftime('%Y/%m/%d')}"
                    except Exception:
                        pass
                if not q:
                    return {"emails": [], "next_page_token": None}
                kwargs = {"userId": 'me', "q": q, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
                if page_token:
                    kwargs["pageToken"] = page_token
                
                # Retry logic for the Gmail API call (now via wrapper)
                while True:
                    try:
                        results = self.api_list_messages(**kwargs)
                        break
                    except Exception as e:
                        err_text = str(e)
                        if ("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text) and attempt < max_retries - 1:
                            print(f"SSL error in list_emails_by_custom_category (attempt {attempt + 1}): {err_text}")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            break
                        else:
                            raise e
                
                # If we get here, the operation was successful (inner while loop broke)
                break  # Break from outer for loop
            except Exception as e:
                error_msg = str(e)
                print(f"Error in list_emails_by_custom_category (attempt {attempt + 1}): {error_msg}")
                if "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "timeout" in error_msg.lower():
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue  # Continue to next attempt in outer for loop
                return {"emails": [], "next_page_token": None, "error": error_msg}
        
        # Process the results
        messages = results.get('messages', []) or []
        next_token = results.get('nextPageToken')
        email_list = []
        total_emails = len(messages)
        
        # Update progress with total emails
        if hasattr(self, 'command_id') and self.command_id:
            from agent.views import update_email_progress
            update_email_progress(self.command_id, 0, total_emails)
        
        for i, message in enumerate(messages):
            # Per-message fetch via wrapper with built-in retries
            msg = self.api_get_message(
                message['id'],
                format='metadata', metadataHeaders=['From','Subject'],
                fields='payload/headers,id,internalDate,snippet'
            )
            headers = msg.get('payload', {}).get('headers', [])
            subject = next((h.get('value') for h in headers if h.get('name') == 'Subject'), 'No Subject')
            sender = next((h.get('value') for h in headers if h.get('name') == 'From'), 'Unknown Sender')
            
            # Add date information
            date_str = ''
            try:
                ts_ms = int(msg.get('internalDate', '0') or '0')
                if ts_ms:
                    date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
            except Exception:
                date_str = ''
            
            email_list.append({'id': message['id'], 'subject': subject, 'sender': sender, 'date': date_str, 'snippet': msg.get('snippet','')})
            
            # Update progress
            if hasattr(self, 'command_id') and self.command_id:
                update_email_progress(self.command_id, i + 1, total_emails)
        return {"emails": email_list, "next_page_token": next_token}

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
        max_retries = 5
        retry_delay = 1
        
        for attempt in range(max_retries):
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
                    
                    try:
                        results = self.service.users().messages().list(**kwargs).execute()
                        msgs = results.get('messages', []) or []
                        all_messages.extend(msgs)
                        page_token = results.get('nextPageToken')
                        if not page_token:
                            break
                    except Exception as e:
                        err_text = str(e)
                        if (("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in err_text or
                             "timeout" in err_text.lower() or "connection" in err_text.lower() or
                             "WinError 10060" in err_text or "failed to respond" in err_text.lower())
                            and attempt < max_retries - 1):
                            print(f"Connection error in archive_emails_by_custom_category (attempt {attempt + 1}): {err_text}")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            break  # Break out of while loop to retry the whole function
                        else:
                            raise e
                
                # If we get here, the operation was successful
                break
                
            except Exception as e:
                error_msg = str(e)
                print(f"Error in archive_emails_by_custom_category (attempt {attempt + 1}): {error_msg}")
                
                # Check if it's an SSL error or connection issue
                if ("timeout" in error_msg.lower() or "connection" in error_msg.lower() or
                    "WinError 10060" in error_msg or "failed to respond" in error_msg.lower() or
                    "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg):
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        continue
                
                # If it's not a retryable error or we've exhausted retries
                return {"status": "error", "message": f"Error archiving emails: {error_msg}"}
        
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
        total_emails = len(all_messages)
        message_ids = [m['id'] for m in all_messages]
        
        # Update progress with total emails
        if hasattr(self, 'command_id') and self.command_id:
            from agent.views import update_email_progress
            update_email_progress(self.command_id, 0, total_emails)
        
        for i in range(0, len(all_messages), 100):
            batch = all_messages[i:i+100]
            batch_ids = [m['id'] for m in batch]
            try:
                self.service.users().messages().batchModify(
                    userId='me', body={'ids': batch_ids, 'removeLabelIds': ['INBOX']}
                ).execute()
                total_processed += len(batch)
                
                # Update progress
                if hasattr(self, 'command_id') and self.command_id:
                    update_email_progress(self.command_id, total_processed, total_emails)
            except HttpError:
                for m in batch:
                    try:
                        self.service.users().messages().modify(userId='me', id=m['id'], body={'removeLabelIds': ['INBOX']}).execute()
                        total_processed += 1
                        
                        # Update progress
                        if hasattr(self, 'command_id') and self.command_id:
                            update_email_progress(self.command_id, total_processed, total_emails)
                    except HttpError:
                        continue
        action_id = self._record_undo('archive', message_ids)
        return {"status": "success", "message": _("Archived %(count)d emails from %(what)s.") % {"count": total_processed, "what": self._pretty_category_name(category_key)}, "archived_count": total_processed, "undo_action_id": action_id}

    def search_emails_by_subject(self, search_term, max_results=50):
        """Search for emails with a specific term in the subject."""
        try:
            query = f"subject:({search_term})"
            results = self.api_list_messages(q=query, maxResults=max_results)
            messages = results.get('messages', [])

            if not messages:
                return []

            email_list = []
            for message in messages:
                msg = self.api_get_message(message['id'], format='metadata', metadataHeaders=['From', 'Subject'])
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
            results = self.api_list_messages(q=query, maxResults=500)
            messages = results.get('messages', [])
            page_token = results.get('nextPageToken')
            all_messages = []
            if messages:
                all_messages.extend(messages)
            while page_token:
                results = self.api_list_messages(q=query, maxResults=500, pageToken=page_token)
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
                    self.api_batch_modify(message_ids_batch, add_label_ids=['TRASH'], remove_label_ids=['INBOX'])
                    total_processed += len(batch)
                except HttpError as e:
                    for message in batch:
                        try:
                            self.api_trash(message['id'])
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
            results = self.api_list_messages(q=query, maxResults=500)
            messages = results.get('messages', [])
            page_token = results.get('nextPageToken')
            all_messages = []
            if messages:
                all_messages.extend(messages)
            while page_token:
                results = self.api_list_messages(q=query, maxResults=500, pageToken=page_token)
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
                    self.api_batch_modify(message_ids_batch, add_label_ids=['TRASH'], remove_label_ids=['INBOX'])
                    total_processed += len(batch)
                except HttpError as e:
                    for message in batch:
                        try:
                            self.api_trash(message['id'])
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
            results = self.api_list_messages(q=query, maxResults=500)
            messages = results.get('messages', [])
            page_token = results.get('nextPageToken')
            all_messages = []
            if messages:
                all_messages.extend(messages)
            while page_token:
                results = self.api_list_messages(q=query, maxResults=500, pageToken=page_token)
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
                    self.api_batch_modify(message_ids_batch, add_label_ids=['TRASH'], remove_label_ids=['INBOX'])
                    total_processed += len(batch)
                except HttpError as e:
                    for message in batch:
                        try:
                            self.api_trash(message['id'])
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
        if not command:
            return None
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
            sender_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)(?=\s+(?:as\b|with\b|$))', command_lower)
            if sender_match and label_match:
                return {
                    "action": "label",
                    "target_type": "sender",
                    "target": sender_match.group(1).strip(),
                    "label": label_match.group(1).strip(),
                    "confirmation_required": True
                }

        # Early handling for restore commands
        if "restore" in command_lower and " from " in command_lower:
            # Capture multi-word sender until end of command
            sender_any_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+)\s*$', command_lower)
            if sender_any_match:
                return {
                    "action": "restore",
                    "target_type": "sender",
                    "target": sender_any_match.group(1).strip(),
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
        elif command_lower.startswith("archive"):
            best_match_action = "archive"
            highest_score = 100  # High confidence for archive commands
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
        
        # Archive emails from sender from time period (check this BEFORE older than)
        if best_match_action == "archive" and "from" in command_lower and " from " in command_lower:
            # Check for "archive emails from [sender] from [time period]" pattern (allow multi-word sender)
            sender_from_time_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)\s+from\s+(today|yesterday|last\s+week|last\s+month|last\s+year|\d+\s+(?:day|days|week|weeks|month|months|year|years)\s+ago)', command_lower)
            if sender_from_time_match:
                sender_keyword = sender_from_time_match.group(1).strip()
                time_period = sender_from_time_match.group(2)
                if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                    return {
                        "action": "archive",
                        "target_type": "sender_from_time",
                        "target": sender_keyword,
                        "time_period": time_period,
                        "confirmation_required": True
                    }

        # Archive emails from sender older than duration (check this BEFORE bulk cleanup)
        if best_match_action == "archive" and "from" in command_lower and "older" in command_lower:
            sender_older_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)\s+older\s+(?:than|then)\s+(\d+)\s*(day|days|d|week|weeks|w|month|months|m|year|years|y)', command_lower)
            if sender_older_match:
                sender_keyword = sender_older_match.group(1).strip()
                qty = int(sender_older_match.group(2))
                unit = sender_older_match.group(3).lower()
                if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                    # Convert to days
                    if unit in ['week', 'weeks', 'w']:
                        older_than_days = qty * 7
                    elif unit in ['month', 'months', 'm']:
                        older_than_days = qty * 30
                    elif unit in ['year', 'years', 'y']:
                        older_than_days = qty * 365
                    else:  # days
                        older_than_days = qty
                    return {"action": "archive", "target_type": "sender", "target": sender_keyword, "confirmation_required": True, "older_than_days": older_than_days}
        
        # Delete emails from sender older than duration (handled BEFORE bulk cleanup)
        if best_match_action == "delete" and "from" in command_lower and "older" in command_lower:
            sender_older_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)\s+older\s+(?:than|then)\s+(\d+)\s*(day|days|d|week|weeks|w|month|months|m|year|years|y)', command_lower)
            if sender_older_match:
                sender_keyword = sender_older_match.group(1).strip()
                qty = int(sender_older_match.group(2))
                unit = sender_older_match.group(3).lower()
                if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                    if unit in ['week', 'weeks', 'w']:
                        older_than_days = qty * 7
                    elif unit in ['month', 'months', 'm']:
                        older_than_days = qty * 30
                    elif unit in ['year', 'years', 'y']:
                        older_than_days = qty * 365
                    else:  # days
                        older_than_days = qty
                    return {"action": "delete", "target_type": "sender", "target": sender_keyword, "confirmation_required": True, "older_than_days": older_than_days}

        # Bulk cleanup by age only (skip if a category/custom-category is mentioned and if 'from' is present)
        if (best_match_action in ["delete", "archive"]) and ("all" in command_lower or "emails" in command_lower) and "older" in command_lower and (" from " not in command_lower):
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
            # Hebrew sender+timeframe parsing FIRST (before standalone timeframe)
            # Require the intent phrase 'רשום מיילים' before sender to avoid falling back to global date ranges
            hebrew_sender_time_match = re.search(r'רש(?:ו)?ם\s+מיילים\s+מ-?([a-zA-Z0-9\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)\s+(מהיום|מאתמול|מהשבוע|מהחודש|מהשנה|מהשבוע\s+שעבר|מהחודש\s+שעבר|מהשנה\s+שעברה)', command_lower)
            if hebrew_sender_time_match:
                sender_keyword = hebrew_sender_time_match.group(1).strip()
                hebrew_time = hebrew_sender_time_match.group(2)
                # Map Hebrew to English for internal processing
                hebrew_to_english = {
                    "מהיום": "today",
                    "מאתמול": "yesterday", 
                    "מהשבוע": "this week",
                    "מהחודש": "this month",
                    "מהשנה": "this year",
                    "מהשבוע שעבר": "last week",
                    "מהחודש שעבר": "last month", 
                    "מהשנה שעברה": "last year"
                }
                time_period = hebrew_to_english.get(hebrew_time, hebrew_time)
                if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                    return {"action": "list", "target_type": "sender", "target": sender_keyword, "date_range": time_period, "confirmation_required": False}
            
            # Hebrew: handle standalone "this week/month/year" (e.g., "רשום מיילים מהשבוע/מהחודש/מהשנה")
            try:
                if "מהשבוע" in command_lower:
                    return {"action": "list", "target_type": "date_range", "target": "this week", "confirmation_required": False}
                if "מהחודש" in command_lower:
                    return {"action": "list", "target_type": "date_range", "target": "this month", "confirmation_required": False}
                if "מהשנה" in command_lower:
                    return {"action": "list", "target_type": "date_range", "target": "this year", "confirmation_required": False}
            except Exception:
                pass
            # Custom category listing should be checked early so it doesn't get overridden by generic date parsing
            # Detect optional age filter (support 'a' -> 1)
            custom_older_days = None
            age_m = re.search(r'older\s+(?:than|then)\s+(a|\d+)\s*(day|days|d|week|weeks|w|month|months|m|year|years|y)', command_lower)
            if age_m:
                try:
                    qty_raw = age_m.group(1)
                    qty = 1 if qty_raw == 'a' else int(qty_raw)
                    unit = age_m.group(2)
                    if unit in ["day","days","d"]: custom_older_days = qty
                    elif unit in ["week","weeks","w"]: custom_older_days = qty*7
                    elif unit in ["month","months","m"]: custom_older_days = qty*30
                    elif unit in ["year","years","y"]: custom_older_days = qty*365
                except Exception:
                    custom_older_days = None
            # Detect 'from N <unit> ago' and 'from <time period>' for custom categories
            custom_date_range = None
            from_ago_m = re.search(r'from\s+(a|\d+)\s+(day|week|month|year)s?\s+ago', command_lower)
            if from_ago_m:
                qty_str = from_ago_m.group(1)
                qty = 1 if qty_str == 'a' else int(qty_str)
                unit = from_ago_m.group(2)
                custom_date_range = f"{qty} {unit}"
            else:
                # Support: "from this|last <period>" and today/yesterday
                timephrase_m = re.search(r'from\s+(today|yesterday|this\s+week|this\s+month|this\s+year|last\s+week|last\s+month|last\s+year)', command_lower)
                if timephrase_m:
                    custom_date_range = timephrase_m.group(1)
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
            
            # Detect recent
            if "recent" in command_lower:
                return {"action": "list", "target_type": "recent", "target": "recent", "confirmation_required": False}
            
            # Detect time window or domain/sender after 'from'
            if "from" in command_lower:
                # Check for "from [sender] from [time period]" pattern (English)
                sender_from_time_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)\s+from\s+(today|yesterday|last\s+week|last\s+month|last\s+year|this\s+week|this\s+month|this\s+year|(a|\d+)\s+(?:day|days|week|weeks|month|months|year|years)\s+ago)', command_lower)
                if sender_from_time_match:
                    sender_keyword = sender_from_time_match.group(1).strip()
                    time_period = sender_from_time_match.group(2)
                    if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                        return {"action": "list", "target_type": "sender", "target": sender_keyword, "date_range": time_period, "confirmation_required": False}
                # Alt: "from [sender] [N unit] ago" (without second 'from')
                sender_ago_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)\s+(a|\d+)\s+(day|days|week|weeks|month|months|year|years)\s+ago', command_lower)
                if sender_ago_match:
                    sender_keyword = sender_ago_match.group(1).strip()
                    qty = sender_ago_match.group(2)
                    unit = sender_ago_match.group(3)
                    if qty == 'a': qty = '1'
                    if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                        return {"action": "list", "target_type": "sender", "target": sender_keyword, "date_range": f"{qty} {unit} ago", "confirmation_required": False}
                # Alt: "from [sender] older than a|N <unit>" (attach age filter to sender)
                sender_older_list = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)\s+older\s+(?:than|then)\s+(a|\d+)\s*(day|days|d|week|weeks|w|month|months|m|year|years|y)\b', command_lower)
                if sender_older_list:
                    sender_keyword = sender_older_list.group(1).strip()
                    qty_raw = sender_older_list.group(2)
                    unit = sender_older_list.group(3).lower()
                    qty = 1 if qty_raw == 'a' else int(qty_raw)
                    if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                        if unit in ['week','weeks','w']:
                            older_than_days = qty * 7
                        elif unit in ['month','months','m']:
                            older_than_days = qty * 30
                        elif unit in ['year','years','y']:
                            older_than_days = qty * 365
                        else:  # days
                            older_than_days = qty
                        return {"action": "list", "target_type": "sender", "target": sender_keyword, "confirmation_required": False, "older_than_days": older_than_days}

                # Alt: "from [sender] (today|yesterday|this X|last X)"
                sender_simple_time_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)\s+(today|yesterday|this\s+week|this\s+month|this\s+year|last\s+week|last\s+month|last\s+year)', command_lower)
                if sender_simple_time_match:
                    sender_keyword = sender_simple_time_match.group(1).strip()
                    time_period = sender_simple_time_match.group(2)
                    if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                        return {"action": "list", "target_type": "sender", "target": sender_keyword, "date_range": time_period, "confirmation_required": False}
                
                # Then: explicit time window after 'from' (only if no sender was found)
                time_after_from = re.search(r'from\s+(today|yesterday|last\s+week|last\s+month|last\s+year|this\s+week|this\s+month|this\s+year|(a|\d+)\s+(?:day|days|week|weeks|month|months|year|years)\s+ago)', command_lower)
                if time_after_from:
                    date_phrase = time_after_from.group(1)
                    return {"action": "list", "target_type": "date_range", "target": date_phrase, "confirmation_required": False}
                # Then: domain first (including TLDs and ccTLD chains like .co.il)
                domain_match = re.search(r'from\s+([a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})*)', command_lower)
                if domain_match:
                    print(f"DEBUG: Parsed as domain command: {domain_match.group(1)}")
                    return {"action": "list", "target_type": "domain", "target": domain_match.group(1), "confirmation_required": False, "older_than_days": custom_older_days}
                
                # Also check for common single-word domains
                single_domain_match = re.search(r'from\s+(google|yahoo|microsoft|apple|amazon|facebook|twitter|instagram|linkedin|github|stackoverflow|reddit|youtube|netflix|spotify|dropbox|slack|discord|zoom|teams)', command_lower)
                if single_domain_match:
                    print(f"DEBUG: Parsed as single-word domain command: {single_domain_match.group(1)}")
                    return {"action": "list", "target_type": "domain", "target": single_domain_match.group(1), "confirmation_required": False, "older_than_days": custom_older_days}
                # Then try sender
                sender_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)(?=\s+(?:older\b|from\b|today\b|yesterday\b|this\s+week\b|this\s+month\b|this\s+year\b|last\s+week\b|last\s+month\b|last\s+year\b|\d+\s+(?:day|days|week|weeks|month|months|year|years)\s+ago\b|as\b|with\b)\b|$)', command_lower)
                if sender_match:
                    sender_keyword = sender_match.group(1).strip()
                    if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                        return {"action": "list", "target_type": "sender", "target": sender_keyword, "confirmation_required": False, "older_than_days": custom_older_days}
        
        # Initialize optional date filters to avoid UnboundLocalError when not set
        older_than_match = re.search(r'(older than|before)\s+(a|\d+)\s+(day|week|month|year)s?', command_lower)
        from_ago_match = re.search(r'from\s+(a|\d+)\s+(day|week|month|year)s?\s+ago', command_lower)
        simple_date_match = re.search(r'from\s+(today|yesterday|last week|last month|last year|this week|this month|this year)', command_lower)

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
        for key in ["today", "yesterday", "last week", "last month", "last year", "this week", "this month", "this year"]:
            if key in command_lower:
                return {"action": "list", "target_type": "date_range", "target": key, "confirmation_required": False}
        
        # Fallback: detect 'older than' anywhere
        any_older = re.search(r'older\s+than\s+(\d+)\s+(day|days|week|weeks|month|months|year|years)', command_lower)
        if any_older:
            qty = any_older.group(1)
            unit = any_older.group(2)
            unit = unit[:-1] if unit.endswith('s') else unit
            return {"action": "list", "target_type": "older_than", "target": f"{qty} {unit}", "confirmation_required": False}
        
        # Final fallback for list intent: treat as recent
        if best_match_action == "list":
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
            flexible_sender_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)(?=\s+|$)', command_lower)
            if flexible_sender_match:
                sender_keyword = flexible_sender_match.group(1).strip()
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
            # Archive emails from sender from specific time periods (today/yesterday/this X/last X/N ago)
            from_time_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)\s+from\s+(today|yesterday|last\s+week|last\s+month|last\s+year|this\s+week|this\s+month|this\s+year|(a|\d+)\s+(?:day|days|week|weeks|month|months|year|years)\s+ago)', command_lower)
            if from_time_match:
                sender_keyword = from_time_match.group(1).strip()
                time_period = from_time_match.group(2)
                print(f"DEBUG: from_time_match found - sender='{sender_keyword}', time_period='{time_period}'")
                if sender_keyword not in ['emails','all','the','my','any','this','that','these','those']:
                    return {"action": "archive", "target_type": "sender_from_time", "target": sender_keyword, "time_period": time_period, "confirmation_required": True}
            flexible_sender_match = re.search(r'from\s+([a-zA-Z0-9._\-+@\u0590-\u05FF\u0600-\u06FF\u4e00-\u9fff ]+?)(?=\s+|$)', command_lower)
            if flexible_sender_match:
                sender_keyword = flexible_sender_match.group(1).strip()
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
            elif action == "archive_by_sender_from_time":
                return self.archive_emails_by_sender_from_time(
                    confirmation_data["sender"], confirmation_data["time_period"], confirm=True
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

        # Handle empty or None commands (only if no confirmation data)
        if not command:
            return {"status": "error", "message": _("Command not understood."), "debug_info": "Empty command provided."}
        
        # Regular command processing
        # Use manual parsing
        print(f"DEBUG: Processing command: '{command}'")
        parsed = self.parse_command_manually(command)
        print(f"DEBUG: Parsed result: {parsed}")
        
        if not parsed:
            return {"status": "error", "message": _("Command not understood."), "debug_info": "Parser returned empty."}
        
        if parsed.get("debug_info"):
            return {"status": "error", "message": _("Command not understood."), "debug_info": parsed.get("debug_info")}
        
        action = parsed.get("action")
        target_type = parsed.get("target_type")
        target = parsed.get("target")
        older_than_days = parsed.get("older_than_days")
        confirm_required = parsed.get("confirmation_required", False)

        try:
            # Detect Hebrew mode from Django's current language (more reliable than command text)
            lang_code = (_dj_translation.get_language() if _dj_translation else None) or 'en'
            hebrew_mode = str(lang_code).startswith('he')

            def _hebrew_date_phrase(eng_phrase: str) -> str:
                # Fixed phrases
                mapping = {
                    "this week": "מהשבוע",
                    "this month": "מהחודש",
                    "this year": "מהשנה",
                    "today": "מהיום",
                    "yesterday": "מאתמול",
                    "last week": "מהשבוע שעבר",
                    "last month": "מהחודש שעבר",
                    "last year": "מהשנה שעברה",
                }
                if eng_phrase in mapping:
                    return mapping[eng_phrase]
                # Numeric "N unit" or "N unit ago"
                try:
                    m = re.match(r"^(\d+)\s+(day|days|week|weeks|month|months|year|years)(?:\s+ago)?$", str(eng_phrase).strip())
                    if m:
                        n = int(m.group(1))
                        unit = m.group(2)
                        # Hebrew unit pluralization (simple)
                        if unit.startswith('day'):
                            unit_he = 'יום' if n == 1 else 'ימים'
                        elif unit.startswith('week'):
                            unit_he = 'שבוע' if n == 1 else 'שבועות'
                        elif unit.startswith('month'):
                            unit_he = 'חודש' if n == 1 else 'חודשים'
                        else:
                            unit_he = 'שנה' if n == 1 else 'שנים'
                        return f"מלפני {n} {unit_he}"
                except Exception:
                    pass
                return eng_phrase
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
                    if not emails: 
                        return {"status": "success", "message": "No recent emails found."}
                    return {"status": "success", "data": emails, "type": "email_list", "message": f"Found {len(emails)} recent emails.", "next_page_token": res.get("next_page_token"), "list_context": {"mode": "recent"}}
                elif target_type == "archived":
                    res = self.list_archived_emails()
                    emails = res.get("emails", []) if isinstance(res, dict) else res
                    next_token = res.get("next_page_token") if isinstance(res, dict) else None
                    if not emails: 
                        return {"status": "success", "message": _("No archived emails found.")}
                    return {"status": "success", "data": emails, "type": "email_list", "message": f"Found {len(emails)} archived emails.", "next_page_token": next_token, "list_context": {"mode": "archived"}}
                elif target_type == "all_mail":
                    res = self.list_all_emails()
                    # Check if res is valid before calling .get()
                    if not res or not isinstance(res, dict):
                        return {"status": "error", "message": "Failed to retrieve emails from All Mail"}
                    # Check if res contains an error
                    if "error" in res:
                        return {"status": "error", "message": f"Error: {res['error']}"}
                    emails = res.get("emails", [])
                    if not emails: 
                        return {"status": "success", "message": _("No emails found in All Mail.")}
                    return {"status": "success", "data": emails, "type": "email_list", "message": f"Found {len(emails)} emails in All Mail.", "next_page_token": res.get("next_page_token"), "list_context": {"mode": "all_mail"}}
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
                    date_range = parsed.get("date_range")
                    res = self.list_emails_by_domain(target, older_than_days=older_than_days, date_range=date_range)
                    emails = res.get("emails", [])
                    if not emails:
                        if hebrew_mode:
                            date_txt = (" " + _hebrew_date_phrase(date_range)) if date_range else ""
                            return {"status": "success", "message": f"לא נמצאו מיילים מ-{target}{date_txt}."}
                        if date_range:
                            return {"status": "success", "message": _("No emails found from domain: %(domain)s from %(time)s.") % {"domain": target, "time": date_range}}
                        return {"status": "success", "message": _("No emails found from domain: %(domain)s.") % {"domain": target}}
                    lc = {"mode": "domain", "target": target}
                    if older_than_days is not None: lc["older_than_days"] = older_than_days
                    if date_range is not None: lc["date_range"] = date_range
                    # Localize snackbar fully in Hebrew
                    if hebrew_mode:
                        age_txt = f" ישנים מ-{int(older_than_days)} ימים" if older_than_days else ""
                        date_txt = (" " + _hebrew_date_phrase(date_range)) if date_range else ""
                        msg = f"נמצאו {len(emails)} מיילים מ-{target}{age_txt}{date_txt}."
                    else:
                        age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                        date_txt = f" from {date_range}" if date_range else ""
                        msg = _("Found %(count)d emails from %(who)s%(age)s%(date)s.") % {"count": len(emails), "who": target, "age": age_txt, "date": date_txt}
                    return {"status": "success", "data": emails, "type": "email_list", "message": msg, "next_page_token": res.get("next_page_token"), "list_context": lc}
                elif target_type == "sender":
                    date_range = parsed.get("date_range")
                    res = self.list_emails_by_sender(target, older_than_days=older_than_days, date_range=date_range)
                    emails = res.get("emails", [])
                    if not emails:
                        if hebrew_mode:
                            date_txt = (" " + _hebrew_date_phrase(date_range)) if date_range else ""
                            return {"status": "success", "message": f"לא נמצאו מיילים מ-{target}{date_txt}."}
                        if date_range:
                            return {"status": "success", "message": _("No emails found from %(sender)s from %(time)s.") % {"sender": target, "time": date_range}}
                        return {"status": "success", "message": _("No emails found from %(sender)s.") % {"sender": target}}
                    lc = {"mode": "sender", "target": target}
                    if older_than_days is not None: lc["older_than_days"] = older_than_days
                    if date_range is not None: lc["date_range"] = date_range
                    if hebrew_mode:
                        age_txt = f" ישנים מ-{int(older_than_days)} ימים" if older_than_days else ""
                        date_txt = (" " + _hebrew_date_phrase(date_range)) if date_range else ""
                        msg = f"נמצאו {len(emails)} מיילים מ-{target}{age_txt}{date_txt}."
                    else:
                        age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                        date_txt = f" from {date_range}" if date_range else ""
                        msg = _("Found %(count)d emails from %(who)s%(age)s%(date)s.") % {"count": len(emails), "who": target, "age": age_txt, "date": date_txt}
                    return {"status": "success", "data": emails, "type": "email_list", "message": msg, "next_page_token": res.get("next_page_token"), "list_context": lc}
                elif target_type == "date_range":
                    # Add SSL retry logic for date range commands
                    max_retries = 5
                    retry_delay = 1
                    
                    for attempt in range(max_retries):
                        try:
                            # Use the user's page size setting instead of fetching all emails
                            max_results = getattr(self, 'default_max_results', 50)
                            result = self.list_emails_by_date_range(target, max_results=max_results)
                            # Handle errors gracefully instead of showing None
                            if isinstance(result, dict) and result.get("error"):
                                return {"status": "error", "message": result.get("error")}
                            if not result.get("emails"):
                                # Localize date phrase in message for Hebrew mode
                                msg_out = result.get("message") or ""
                                if hebrew_mode and isinstance(msg_out, str):
                                    # Replace fixed phrases and numeric 'N unit ago'
                                    replacements = {
                                        " from this week": " " + _hebrew_date_phrase("this week"),
                                        " from this month": " " + _hebrew_date_phrase("this month"),
                                        " from this year": " " + _hebrew_date_phrase("this year"),
                                        " from today": " " + _hebrew_date_phrase("today"),
                                        " from yesterday": " " + _hebrew_date_phrase("yesterday"),
                                        " from last week": " " + _hebrew_date_phrase("last week"),
                                        " from last month": " " + _hebrew_date_phrase("last month"),
                                        " from last year": " " + _hebrew_date_phrase("last year"),
                                    }
                                    for k, v in replacements.items():
                                        msg_out = msg_out.replace(k, v)
                                    # Numeric pattern
                                    msg_out = re.sub(r" from (\d+) (day|days|week|weeks|month|months|year|years) ago",
                                                     lambda m: " " + _hebrew_date_phrase(f"{m.group(1)} {m.group(2)} ago"),
                                                     msg_out)
                                return {"status": "success", "message": msg_out}
                            # Localize message fully for Hebrew
                            if hebrew_mode:
                                emails_list = result.get("emails") or []
                                heb_phrase = _hebrew_date_phrase(target)
                                msg_out = f"נמצאו {len(emails_list)} מיילים {heb_phrase}."
                                return {"status": "success", "data": emails_list, "type": "email_list", "message": msg_out, "next_page_token": result.get("next_page_token"), "list_context": {"mode": "date_range", "target": target}}
                            # Non-Hebrew: keep original message (with limited replacements)
                            msg_out = result.get("message")
                            return {"status": "success", "data": result.get("emails"), "type": "email_list", "message": msg_out, "next_page_token": result.get("next_page_token"), "list_context": {"mode": "date_range", "target": target}}
                        except Exception as e:
                            error_msg = str(e)
                            print(f"Error in date_range command (attempt {attempt + 1}): {error_msg}")
                            
                            # Check if it's an SSL error or connection issue
                            if ("SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg or
                                "timeout" in error_msg.lower() or "connection" in error_msg.lower() or
                                "WinError 10060" in error_msg or "failed to respond" in error_msg.lower()):
                                if attempt < max_retries - 1:
                                    print(f"SSL/Connection error, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                                    time.sleep(retry_delay)
                                    retry_delay *= 2  # Exponential backoff
                                    continue
                                else:
                                    print(f"❌ SSL/Connection error after {max_retries} attempts: {error_msg}")
                                    return {"status": "error", "message": f"Connection error after {max_retries} attempts: {error_msg}"}
                            else:
                                # Non-SSL error, don't retry
                                return {"status": "error", "message": f"Error executing command: {error_msg}"}
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
                        # Include timeframe/duration in no-results message when provided
                        date_range = parsed.get("date_range")
                        if date_range:
                            dr = str(date_range).strip().lower()
                            if not any(tok in dr for tok in ["ago", "today", "yesterday", "this", "last"]):
                                dr = f"{dr} ago"
                            if hebrew_mode:
                                date_he = " " + _hebrew_date_phrase(dr)
                                return {"status": "success", "message": f"לא נמצאו מיילים ב-{pretty}{date_he}."}
                            else:
                                # Normalize pluralization for numeric units
                                try:
                                    m = re.match(r"^(\d+)\s+(day|week|month|year)(s)?(?:\s+ago)?$", dr)
                                    if m:
                                        qty = int(m.group(1))
                                        unit = m.group(2)
                                        if qty != 1 and not unit.endswith('s'):
                                            unit = unit + 's'
                                        dr = f"{qty} {unit} ago"
                                except Exception:
                                    pass
                                return {"status": "success", "message": f"No emails found in {pretty} from {dr}."}
                        # Default no-results message without timeframe
                        return {"status": "success", "message": _("No emails found for %(what)s.") % {"what": pretty}}
                    # Include both display name and key for reliable pagination
                    lc = {"mode": "custom_category", "category": pretty, "category_key": target}
                    if older_than_days is not None:
                        lc["older_than_days"] = older_than_days
                    date_range = parsed.get("date_range")
                    if date_range:
                        lc["date_range"] = date_range
                    # Build localized snackbar message including timeframe when provided
                    if hebrew_mode:
                        age_he = f" ישנים מ-{int(older_than_days)} ימים" if older_than_days else ""
                        if date_range:
                            dr = str(date_range).strip().lower()
                            if not any(tok in dr for tok in ["ago", "today", "yesterday", "this", "last"]):
                                dr = f"{dr} ago"
                            date_he = " " + _hebrew_date_phrase(dr)
                        else:
                            date_he = ""
                        msg = f"נמצאו {len(emails)} מיילים ב-{pretty}{age_he}{date_he}."
                    else:
                        age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                        if date_range:
                            dr = str(date_range).strip().lower()
                            if not any(tok in dr for tok in ["ago", "today", "yesterday", "this", "last"]):
                                dr = f"{dr} ago"
                            # Normalize pluralization for numeric units in English
                            try:
                                m = re.match(r"^(\d+)\s+(day|week|month|year)(s)?(?:\s+ago)?$", dr)
                                if m:
                                    qty = int(m.group(1))
                                    unit = m.group(2)
                                    if qty != 1 and not unit.endswith('s'):
                                        unit = unit + 's'
                                    dr = f"{qty} {unit} ago"
                            except Exception:
                                pass
                            # Build the full sentence without stray period
                            msg = f"Found {len(emails)} emails{age_txt} in {pretty} from {dr}."
                        else:
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
                elif target_type == "sender_from_time":
                    return self.archive_emails_by_sender_from_time(target, parsed.get("time_period"), confirm=not confirm_required)
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
        max_retries = 5
        retry_delay = 1
        
        for attempt in range(max_retries):
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
                    
                    try:
                        results = self.service.users().messages().list(**kwargs).execute()
                        messages = results.get('messages', []) or []
                        all_messages.extend(messages)
                        page_token = results.get('nextPageToken')
                        if not page_token:
                            break
                    except Exception as e:
                        err_text = str(e)
                        if (("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in err_text or
                             "timeout" in err_text.lower() or "connection" in err_text.lower() or
                             "WinError 10060" in err_text or "failed to respond" in err_text.lower())
                            and attempt < max_retries - 1):
                            print(f"Connection error in archive_emails_by_sender (attempt {attempt + 1}): {err_text}")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            break  # Break out of while loop to retry the whole function
                        else:
                            raise e
                
                # If we get here, the message fetching was successful, now do the archiving
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
                total_emails = len(all_messages)
                message_ids = [m['id'] for m in all_messages]
                
                # Update progress with total emails
                if hasattr(self, 'command_id') and self.command_id:
                    from agent.views import update_email_progress
                    update_email_progress(self.command_id, 0, total_emails)
                
                for i in range(0, len(all_messages), 100):
                    batch = all_messages[i:i+100]
                    batch_ids = [m['id'] for m in batch]
                    try:
                        self.service.users().messages().batchModify(
                        userId='me', 
                            body={'ids': batch_ids, 'removeLabelIds': ['INBOX']}
                    ).execute()
                        total_processed += len(batch)
                        
                        # Update progress
                        if hasattr(self, 'command_id') and self.command_id:
                            update_email_progress(self.command_id, total_processed, total_emails)
                    except HttpError:
                        # Fallback to single modify
                        for m in batch:
                            try:
                                self.service.users().messages().modify(userId='me', id=m['id'], body={'removeLabelIds': ['INBOX']}).execute()
                                total_processed += 1
                                
                                # Update progress
                                if hasattr(self, 'command_id') and self.command_id:
                                    update_email_progress(self.command_id, total_processed, total_emails)
                            except HttpError:
                                continue
                
                # Record undo action
                action_id = self._record_undo('archive', message_ids)
                age_txt = _(" older than %(days)d days") % {"days": older_than_days} if older_than_days else ""
                return {"status": "success", "message": _("Archived %(count)d emails from %(sender)s%(age)s.") % {"count": total_processed, "sender": sender_email, "age": age_txt}, "archived_count": total_processed, "undo_action_id": action_id}
                
            except Exception as e:
                error_msg = str(e)
                print(f"Error in archive_emails_by_sender (attempt {attempt + 1}): {error_msg}")
                if ("timeout" in error_msg.lower() or "connection" in error_msg.lower() or
                    "WinError 10060" in error_msg or "failed to respond" in error_msg.lower() or
                    "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg):
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue  # Continue to next attempt in outer for loop
                return {"error": f"Error archiving emails by sender: {error_msg}"}
        
        # This should never be reached due to the break statement above
        return {"error": "Unexpected error in archive_emails_by_sender"}
    
    def archive_emails_by_domain(self, domain, confirm=False, older_than_days=None):
        """Archive emails from a specific domain, optionally filtered by age."""
        max_retries = 5
        retry_delay = 1
        
        for attempt in range(max_retries):
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
                    
                    try:
                        results = self.service.users().messages().list(**kwargs).execute()
                        messages = results.get('messages', []) or []
                        all_messages.extend(messages)
                        page_token = results.get('nextPageToken')
                        if not page_token:
                            break
                    except Exception as e:
                        err_text = str(e)
                        if (("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in err_text or
                             "timeout" in err_text.lower() or "connection" in err_text.lower() or
                             "WinError 10060" in err_text or "failed to respond" in err_text.lower())
                            and attempt < max_retries - 1):
                            print(f"Connection error in archive_emails_by_domain (attempt {attempt + 1}): {err_text}")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            break  # Break out of while loop to retry the whole function
                        else:
                            raise e
                
                # If we get here, the operation was successful
                break
                
            except Exception as e:
                error_msg = str(e)
                print(f"Error in archive_emails_by_domain (attempt {attempt + 1}): {error_msg}")
                if ("timeout" in error_msg.lower() or "connection" in error_msg.lower() or
                    "WinError 10060" in error_msg or "failed to respond" in error_msg.lower() or
                    "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg):
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue  # Continue to next attempt in outer for loop
                return {"error": f"Error archiving emails by domain: {error_msg}"}
        
        try:
            
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
            total_emails = len(all_messages)
            message_ids = [m['id'] for m in all_messages]
            
            # Update progress with total emails
            if hasattr(self, 'command_id') and self.command_id:
                from agent.views import update_email_progress
                update_email_progress(self.command_id, 0, total_emails)
            
            for i in range(0, len(all_messages), 100):
                batch = all_messages[i:i+100]
                batch_ids = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                    userId='me', 
                        body={'ids': batch_ids, 'removeLabelIds': ['INBOX']}
                ).execute()
                    total_processed += len(batch)
                    
                    # Update progress
                    if hasattr(self, 'command_id') and self.command_id:
                        update_email_progress(self.command_id, total_processed, total_emails)
                except HttpError:
                    for m in batch:
                        try:
                            self.service.users().messages().modify(userId='me', id=m['id'], body={'removeLabelIds': ['INBOX']}).execute()
                            total_processed += 1
                            
                            # Update progress
                            if hasattr(self, 'command_id') and self.command_id:
                                update_email_progress(self.command_id, total_processed, total_emails)
                        except HttpError:
                            continue
            action_id = self._record_undo('archive', message_ids)
            return {"status": "success", "message": _("Archived %(count)d emails from %(domain)s.") % {"count": total_processed, "domain": domain}, "archived_count": total_processed, "undo_action_id": action_id}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error archiving emails: {error}"}
    
    def archive_emails_by_subject_keywords(self, keywords, confirm=False):
        """Archive emails containing specific keywords in subject (safer than delete)"""
        max_retries = 5
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                # Build search query for subject keywords
                keyword_query = " OR ".join([f'subject:"{keyword}"' for keyword in keywords])
                query = f"({keyword_query})"
                
                try:
                    results = self.service.users().messages().list(
                        userId='me', q=query).execute()
                    messages = results.get('messages', [])
                except Exception as e:
                    err_text = str(e)
                    if (("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in err_text or
                         "timeout" in err_text.lower() or "connection" in err_text.lower() or
                         "WinError 10060" in err_text or "failed to respond" in err_text.lower())
                        and attempt < max_retries - 1):
                        print(f"Connection error in archive_emails_by_subject_keywords (attempt {attempt + 1}): {err_text}")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue  # Continue to next attempt
                    else:
                        raise e
                
                # If we get here, the operation was successful
                break
                
            except Exception as e:
                error_msg = str(e)
                print(f"Error in archive_emails_by_subject_keywords (attempt {attempt + 1}): {error_msg}")
                if ("timeout" in error_msg.lower() or "connection" in error_msg.lower() or
                    "WinError 10060" in error_msg or "failed to respond" in error_msg.lower() or
                    "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg):
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue  # Continue to next attempt in outer for loop
                return {"error": f"Error archiving emails by subject keywords: {error_msg}"}
        
        try:
            
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
            total_emails = len(messages)
            
            # Update progress with total emails
            if hasattr(self, 'command_id') and self.command_id:
                from agent.views import update_email_progress
                update_email_progress(self.command_id, 0, total_emails)
            
            if len(messages) > 10:  # Only batch if we have enough messages
                batch_size = 100  # Gmail batch API limit
                total_processed = 0
                for i in range(0, len(messages), batch_size):
                    batch_messages = messages[i:i + batch_size]
                    batch_ids = [m['id'] for m in batch_messages]
                    
                    try:
                        # Use batchModify for multiple emails at once
                        self.service.users().messages().batchModify(
                            userId='me', 
                            body={'ids': batch_ids, 'removeLabelIds': ['INBOX']}
                        ).execute()
                        total_processed += len(batch_messages)
                        
                        # Update progress
                        if hasattr(self, 'command_id') and self.command_id:
                            update_email_progress(self.command_id, total_processed, total_emails)
                    except HttpError:
                        # Fallback to individual requests for this batch
                        for message in batch_messages:
                            try:
                                self.service.users().messages().modify(
                                    userId='me', 
                                    id=message['id'],
                                    body={'removeLabelIds': ['INBOX']}
                                ).execute()
                                total_processed += 1
                                
                                # Update progress
                                if hasattr(self, 'command_id') and self.command_id:
                                    update_email_progress(self.command_id, total_processed, total_emails)
                            except HttpError:
                                continue
            else:
                # For small numbers, use individual requests
                total_processed = 0
            for message in messages:
                self.service.users().messages().modify(
                    userId='me', 
                    id=message['id'],
                    body={'removeLabelIds': ['INBOX']}
                ).execute()
                total_processed += 1
                
                # Update progress
                if hasattr(self, 'command_id') and self.command_id:
                    update_email_progress(self.command_id, total_processed, total_emails)
            
            return {"status": "success", "message": f"Archived {len(messages)} emails with keywords {', '.join(keywords)}.", "archived_count": len(messages)}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error archiving emails: {error}"}
    
    def archive_emails_by_sender_from_time(self, sender_email, time_period, confirm=False):
        """Archive emails from a specific sender from a specific time period (e.g., 'from today', 'from yesterday')."""
        try:
            # Validate inputs
            if not sender_email or not time_period:
                return {"error": "Missing required parameters: sender_email and time_period"}
            
            # Debug logging
            print(f"DEBUG: archive_emails_by_sender_from_time called with sender='{sender_email}', time_period='{time_period}'")
            
            max_retries = 5
            retry_delay = 1
            
            for attempt in range(max_retries):
                try:
                    # Build search query with sender and time period
                    query_parts = [f"from:{sender_email}"]
                    
                    # Compute precise calendar window using shared helper
                    if not time_period:
                        return {"error": "Time period is None or empty"}
                    start_dt, end_dt = self._compute_precise_date_range_window(time_period)
                    if not start_dt or not end_dt:
                        return {"error": f"Invalid or unsupported time period: {time_period}"}
                    start_date = start_dt.strftime("%Y/%m/%d")
                    end_date = end_dt.strftime("%Y/%m/%d")
                    
                    query_parts.append(f"after:{start_date}")
                    query_parts.append(f"before:{end_date}")
                    
                    query = " ".join(query_parts)
                    print(f"DEBUG: Final Gmail query for {sender_email}: {query}")

                    # Fetch all matching messages with pagination
                    all_messages = []
                    page_token = None
                    results = None  # Initialize results to avoid UnboundLocalError
                    while True:
                        kwargs = {"userId": 'me', "q": query, "maxResults": 500}
                        if page_token:
                            kwargs["pageToken"] = page_token
                        
                        try:
                            results = self.service.users().messages().list(**kwargs).execute()
                            messages = results.get('messages', []) or []
                            print(f"DEBUG: Found {len(messages)} messages in this batch")
                            
                            # Filter messages to only include those from the exact target time period
                            filtered_messages = []
                            target_start = None
                            target_end = None
                            
                            # Use the same precise window for local filtering
                            target_start = start_dt
                            target_end = end_dt
                            
                            if target_start and target_end:
                                for msg in messages:
                                    try:
                                        # Get the message details to check the internal date
                                        msg_details = self.service.users().messages().get(
                                            userId='me', id=msg['id'], format='metadata'
                                        ).execute()
                                        
                                        # Use the internal date (timestamp in milliseconds)
                                        internal_date = int(msg_details.get('internalDate', '0'))
                                        if internal_date:
                                            from datetime import datetime
                                            msg_datetime = datetime.fromtimestamp(internal_date / 1000)
                                            if target_start <= msg_datetime < target_end:
                                                filtered_messages.append(msg)
                                        else:
                                            # If no internal date, include it to be safe
                                            filtered_messages.append(msg)
                                    except:
                                        # If we can't get message details, include it to be safe
                                        filtered_messages.append(msg)
                                
                                print(f"DEBUG: Filtered to {len(filtered_messages)} messages from {target_start} to {target_end}")
                                all_messages.extend(filtered_messages)
                            else:
                                all_messages.extend(messages)
                            
                            page_token = results.get('nextPageToken')
                            if not page_token:
                                break
                        except Exception as e:
                            err_text = str(e)
                            if (("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in err_text or
                                 "timeout" in err_text.lower() or "connection" in err_text.lower() or
                                 "WinError 10060" in err_text or "failed to respond" in err_text.lower())
                                and attempt < max_retries - 1):
                                print(f"Connection error in archive_emails_by_sender_from_time (attempt {attempt + 1}): {err_text}")
                                time.sleep(retry_delay)
                                retry_delay *= 2
                                raise e  # Re-raise to trigger outer retry logic
                            else:
                                raise e
                    
                    # If we get here, the operation was successful
                    break
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"Error in archive_emails_by_sender_from_time (attempt {attempt + 1}): {error_msg}")
                    if ("timeout" in error_msg.lower() or "connection" in error_msg.lower() or
                        "WinError 10060" in error_msg or "failed to respond" in error_msg.lower() or
                        "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg):
                        if attempt < max_retries - 1:
                            print(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            continue  # Continue to next attempt in outer for loop
                    return {"error": f"Error archiving emails by sender from time: {error_msg}"}
            
            # If we get here, the message fetching was successful, now do the archiving
            print(f"DEBUG: Total messages found for {sender_email}: {len(all_messages)}")
            if not all_messages:
                return {"status": "success", "message": _("No emails found from %(sender)s from %(time)s.") % {"sender": sender_email, "time": time_period}, "archived_count": 0}
            
            if not confirm:
                return {
                    "status": "confirmation_required", 
                    "message": _("Found %(count)d emails from %(sender)s from %(time)s. Do you want to archive them?") % {"count": len(all_messages), "sender": sender_email, "time": time_period},
                    "count": len(all_messages),
                    "total_estimated": len(all_messages),
                    "preview": self._build_preview(all_messages),
                    "action_details": {"action": "archive_by_sender_from_time", "sender": sender_email, "time_period": time_period}
                }
            
            # Archive in batches
            total_processed = 0
            total_emails = len(all_messages)
            message_ids = [m['id'] for m in all_messages]
            
            # Update progress with total emails
            if hasattr(self, 'command_id') and self.command_id:
                from agent.views import update_email_progress
                update_email_progress(self.command_id, 0, total_emails)
            
            for i in range(0, len(all_messages), 100):
                batch = all_messages[i:i+100]
                batch_ids = [m['id'] for m in batch]
                try:
                    self.service.users().messages().batchModify(
                    userId='me', 
                        body={'ids': batch_ids, 'removeLabelIds': ['INBOX']}
                ).execute()
                    total_processed += len(batch)
                    
                    # Update progress
                    if hasattr(self, 'command_id') and self.command_id:
                        update_email_progress(self.command_id, total_processed, total_emails)
                except HttpError:
                    # Fallback to single modify
                    for m in batch:
                        try:
                            self.service.users().messages().modify(userId='me', id=m['id'], body={'removeLabelIds': ['INBOX']}).execute()
                            total_processed += 1
                            
                            # Update progress
                            if hasattr(self, 'command_id') and self.command_id:
                                update_email_progress(self.command_id, total_processed, total_emails)
                        except HttpError:
                            continue
            action_id = self._record_undo('archive', message_ids)
            return {"status": "success", "message": _("Archived %(count)d emails from %(sender)s from %(time)s.") % {"count": total_processed, "sender": sender_email, "time": time_period}, "archived_count": total_processed, "undo_action_id": action_id}
            
        except HttpError as error:
            return {"status": "error", "message": f"Error archiving emails: {error}"}
        except Exception as e:
            print(f"Unexpected error in archive_emails_by_sender_from_time: {e}")
            return {"error": f"Unexpected error: {str(e)}"}
    
    def list_archived_emails(self, max_results=None, page_token=None):
        """List archived emails (messages not in Inbox, excluding Sent/Drafts/Spam/Trash)."""
        from agent.views import update_email_progress  # Import at function start
        
        max_retries = 5
        retry_delay = 1
        results = None  # Initialize to prevent UnboundLocalError
        batch_processing_attempted = False
        batch_processing_successful = False
        
        # Prevent bouncing progress bar - only reset for new commands
        if not hasattr(self, '_last_command_id') or self._last_command_id != getattr(self, 'command_id', None):
            self._progress_initialized = False
            self._last_command_id = getattr(self, 'command_id', None)
        
        for attempt in range(max_retries):
            try:
                if max_results is None:
                    max_results = self.default_max_results
                # Archived = messages not in Inbox. Also exclude Spam/Trash/Sent/Drafts/Chats.
                # Note: default Gmail search already excludes Spam/Trash, but we keep them explicit.
                q = '-in:inbox -in:spam -in:trash -in:chats -in:sent -in:drafts'
                kwargs = {"userId": 'me', "q": q, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
                if page_token:
                    kwargs["pageToken"] = page_token
                
                # Retry logic for the Gmail API call
                while True:
                    try:
                        results = self.service.users().messages().list(**kwargs).execute()
                        break  # Success, break out of inner while loop
                    except Exception as e:
                        err_text = str(e)
                        if (("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in err_text or
                             "timeout" in err_text.lower() or "connection" in err_text.lower() or
                             "WinError 10060" in err_text or "failed to respond" in err_text.lower()) 
                            and attempt < max_retries - 1):
                            print(f"Connection error in list_archived_emails (attempt {attempt + 1}): {err_text}")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            break  # Break out of while loop to retry the whole function
                        else:
                            # For SSL/Connection errors, don't immediately return error - try to continue processing
                            if ("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in err_text or
                                "timeout" in err_text.lower() or "connection" in err_text.lower() or
                                "WinError 10060" in err_text or "failed to respond" in err_text.lower()):
                                print(f"SSL/Connection error in list_archived_emails (attempt {attempt + 1}): {err_text}")
                                if attempt < max_retries - 1:
                                    time.sleep(retry_delay)
                                    retry_delay *= 2
                                    break  # Break out of while loop to retry the whole function
                                else:
                                    # Last attempt failed, but try to continue with fallback
                                    print("SSL error on final attempt, trying fallback query...")
                                    try:
                                        # Try a simpler fallback query
                                        fallback_kwargs = {"userId": 'me', "q": 'in:inbox', "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
                                        if page_token:
                                            fallback_kwargs["pageToken"] = page_token
                                        results = self.service.users().messages().list(**fallback_kwargs).execute()
                                        print("Fallback query succeeded")
                                        break
                                    except Exception as fallback_e:
                                        print(f"Fallback query also failed: {fallback_e}")
                                        results = {"messages": [], "nextPageToken": None}
                                        break
                            else:
                                raise e  # Re-raise if not connection error
                
                # If we get here, the operation was successful (inner while loop broke)
                break  # Break from outer for loop
            except Exception as e:
                error_msg = str(e)
                print(f"Error in list_archived_emails (attempt {attempt + 1}): {error_msg}")
                if ("timeout" in error_msg.lower() or "connection" in error_msg.lower() or 
                    "WinError 10060" in error_msg or "failed to respond" in error_msg.lower() or
                    "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg):
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue  # Continue to next attempt in outer for loop
                    else:
                        # Final attempt failed, try fallback
                        print("All retries failed, trying fallback query...")
                        try:
                            fallback_kwargs = {"userId": 'me', "q": 'in:inbox', "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
                            if page_token:
                                fallback_kwargs["pageToken"] = page_token
                            results = self.service.users().messages().list(**fallback_kwargs).execute()
                            print("Fallback query succeeded")
                            break
                        except Exception as fallback_e:
                            print(f"Fallback query also failed: {fallback_e}")
                            return {"error": f"Error listing archived emails: {error_msg}"}
                else:
                    return {"error": f"Error listing archived emails: {error_msg}"}
        
        # Process the results
        if results is None:
            return {"error": "Failed to retrieve archived emails"}
            
        messages = results.get('messages', [])
        next_token = results.get('nextPageToken')
        if not messages:
            return {"emails": [], "next_page_token": None}
        archived_emails = []
        
        # Use batch processing for much faster execution
        if len(messages) > 10:  # Only batch if we have enough messages
            batch_size = 100  # Gmail batch API limit
            total_emails = len(messages)
            batch_processing_attempted = True
            
            # Update progress with total emails
            if hasattr(self, 'command_id') and self.command_id:
                update_email_progress(self.command_id, 0, total_emails)
            
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
                            fields='payload/headers,id,labelIds,internalDate'
                        )
                    )
                
                try:
                    # Execute batch request
                    batch_responses = batch.execute()
                    
                    # Process batch responses
                    for j, (message, response) in enumerate(zip(batch_messages, batch_responses)):
                        try:
                            if isinstance(response, Exception):
                                continue  # Skip failed requests
                                
                            msg = response
                            headers = msg.get('payload', {}).get('headers', [])
                            subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                            sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                            
                            # Add date information
                            date_str = ''
                            try:
                                ts_ms = int(msg.get('internalDate', '0') or '0')
                                if ts_ms:
                                    date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                            except Exception:
                                date_str = ''
                            
                            archived_emails.append({
                                'id': message['id'],
                                'subject': subject,
                                'sender': sender,
                                'date': date_str,
                                'snippet': ''
                            })
                            
                            # Update progress
                            if hasattr(self, 'command_id') and self.command_id:
                                update_email_progress(self.command_id, i + j + 1, total_emails)
                            
                        except Exception as e:
                            print(f"Error processing message {message['id']}: {e}")
                            continue
                            
                except Exception as e:
                    print(f"Batch request failed: {e}")
                    # Fallback to individual requests for this batch
                    for k, message in enumerate(batch_messages):
                        try:
                            msg = self.api_get_message(
                                userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                                fields='payload/headers,id,labelIds,internalDate'
                            ).execute()
                            headers = msg.get('payload', {}).get('headers', [])
                            subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                            sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                            
                            # Add date information
                            date_str = ''
                            try:
                                ts_ms = int(msg.get('internalDate', '0') or '0')
                                if ts_ms:
                                    date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                            except Exception:
                                date_str = ''
                            
                            archived_emails.append({
                                'id': message['id'],
                                'subject': subject,
                                'sender': sender,
                                'date': date_str,
                                'snippet': ''
                            })
                            
                            # Update progress
                            if hasattr(self, 'command_id') and self.command_id:
                                update_email_progress(self.command_id, i + k + 1, total_emails)
                        except Exception as e:
                            print(f"Error processing individual message {message['id']}: {e}")
                            continue
            
            # Mark batch processing as successful if we processed any emails
            if archived_emails:
                batch_processing_successful = True
        
        # If batch processing failed but we have some emails, return them
        if batch_processing_attempted and not batch_processing_successful and archived_emails:
            return {"emails": archived_emails, "next_page_token": next_token}
        
        # Always return emails if we have any, regardless of processing method
        if archived_emails:
            return {"emails": archived_emails, "next_page_token": next_token}
        
        if not batch_processing_attempted or not batch_processing_successful:
            # For small numbers, use individual requests
            total_emails = len(messages)
            
            # Update progress with total emails
            if hasattr(self, 'command_id') and self.command_id:
                update_email_progress(self.command_id, 0, total_emails)
            
            for i, message in enumerate(messages):
                try:
                    msg = self.api_get_message(
                        message['id'], format='metadata', metadataHeaders=['From','Subject'],
                        fields='payload/headers,id,labelIds,internalDate').execute()
                    headers = msg.get('payload', {}).get('headers', [])
                    subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                    sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                    
                    # Add date information
                    date_str = ''
                    try:
                        ts_ms = int(msg.get('internalDate', '0') or '0')
                        if ts_ms:
                            date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                    except Exception:
                        date_str = ''
                    
                    archived_emails.append({
                        'id': message['id'],
                        'subject': subject,
                        'sender': sender,
                        'date': date_str,
                        'snippet': ''
                    })
                    
                    # Update progress
                    if hasattr(self, 'command_id') and self.command_id:
                        update_email_progress(self.command_id, i + 1, total_emails)
                except Exception as e:
                    print(f"Error processing individual message {message['id']}: {e}")
                    continue
        
        # Always return emails if we have any, regardless of processing method
        if archived_emails:
            return {"emails": archived_emails, "next_page_token": next_token}
        
        return {"emails": [], "next_page_token": next_token}

    def list_all_emails(self, max_results=None, page_token=None):
        """List all emails in All Mail (excludes Spam/Trash/Chats)."""
        from agent.views import update_email_progress  # Import at function start
        
        max_retries = 5
        retry_delay = 1
        results = None  # Initialize to prevent UnboundLocalError
        
        # Prevent bouncing progress bar - only reset for new commands
        if not hasattr(self, '_last_command_id') or self._last_command_id != getattr(self, 'command_id', None):
            self._progress_initialized = False
            self._last_command_id = getattr(self, 'command_id', None)
        
        for attempt in range(max_retries):
            try:
                if max_results is None:
                    max_results = self.default_max_results
                q = '-in:spam -in:trash -in:chats'
                kwargs = {"userId": 'me', "q": q, "maxResults": max_results, "fields": 'messages/id,nextPageToken'}
                if page_token:
                    kwargs["pageToken"] = page_token
                
                # Retry logic for the Gmail API call
                while True:
                    try:
                        results = self.service.users().messages().list(**kwargs).execute()
                        break  # Success, break out of inner while loop
                    except Exception as e:
                        err_text = str(e)
                        if (("SSL" in err_text or "WRONG_VERSION_NUMBER" in err_text or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in err_text or
                             "timeout" in err_text.lower() or "connection" in err_text.lower() or
                             "WinError 10060" in err_text or "failed to respond" in err_text.lower()) 
                            and attempt < max_retries - 1):
                            print(f"Connection error in list_all_emails (attempt {attempt + 1}): {err_text}")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            break  # Break out of while loop to retry the whole function
                        else:
                            raise e  # Re-raise if not connection error or max retries reached
                
                # If we get here, the operation was successful (inner while loop broke)
                if results is not None:
                    break  # Break from outer for loop only if we have results
            except Exception as e:
                error_msg = str(e)
                print(f"Error in list_all_emails (attempt {attempt + 1}): {error_msg}")
                if ("timeout" in error_msg.lower() or "connection" in error_msg.lower() or 
                    "WinError 10060" in error_msg or "failed to respond" in error_msg.lower() or
                    "SSL" in error_msg or "WRONG_VERSION_NUMBER" in error_msg or "DECRYPTION_FAILED_OR_BAD_RECORD_MAC" in error_msg):
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue  # Continue to next attempt in outer for loop
                # Don't return error immediately - try to process any emails we can get
                print(f"SSL/Connection error in list_all_emails: {error_msg}")
                # Continue to try to process emails even with SSL errors
        
        # If we get here, the operation was successful OR we had SSL errors but want to try processing anyway
        if results is None:
            # Try one more time with a simple query to get at least some emails
            try:
                simple_query = "in:inbox"  # Simple query that's more likely to work
                results = self.service.users().messages().list(
                    userId='me', q=simple_query, maxResults=25, fields='messages/id,nextPageToken'
                ).execute()
            except Exception as e:
                return {"error": "Failed to retrieve emails after all retry attempts"}
        
        # Ensure results is a valid dictionary before calling .get()
        if not isinstance(results, dict):
            return {"error": "Invalid response format from Gmail API"}
            
        messages = results.get('messages', [])
        next_token = results.get('nextPageToken')
        if not messages:
            return {"emails": [], "next_page_token": None}
        
        emails = []
        
        # Use batch processing for much faster execution
        batch_processing_attempted = False
        batch_processing_successful = False
        if len(messages) > 10:  # Only batch if we have enough messages
            batch_size = 100  # Gmail batch API limit
            total_emails = len(messages)
            
            # Update progress with total emails (only once)
            if hasattr(self, 'command_id') and self.command_id and not self._progress_initialized:
                update_email_progress(self.command_id, 0, total_emails)
                self._progress_initialized = True
            
            batch_processing_attempted = True
            
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
                                fields='payload/headers,id,internalDate'
                            )
                        )
                    
                    try:
                        # Execute batch request
                        batch_responses = batch.execute()
                        
                        # Check if batch_responses is valid
                        if not batch_responses or not isinstance(batch_responses, list):
                            print(f"Batch request failed: Invalid response format")
                            raise Exception("Invalid batch response")
                        
                        # Process batch responses
                        for j, (message, response) in enumerate(zip(batch_messages, batch_responses)):
                            try:
                                if isinstance(response, Exception):
                                    continue  # Skip failed requests
                                
                                # Check if response is valid
                                if not response or not isinstance(response, dict):
                                    print(f"Warning: Invalid response for message {message['id']}")
                                    continue
                                    
                                msg = response
                                headers = msg.get('payload', {}).get('headers', [])
                                subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                                sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                                
                                # Add date information
                                date_str = ''
                                try:
                                    ts_ms = int(msg.get('internalDate', '0') or '0')
                                    if ts_ms:
                                        date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                                except Exception:
                                    date_str = ''
                                
                                emails.append({'id': message['id'], 'subject': subject, 'sender': sender, 'date': date_str, 'snippet': ''})
                                
                                # Update progress
                                if hasattr(self, 'command_id') and self.command_id:
                                    update_email_progress(self.command_id, i + j + 1, total_emails)
                                
                            except Exception as e:
                                print(f"Error processing message {message['id']}: {e}")
                                continue
                                
                    except Exception as e:
                        print(f"Batch request failed: {e}")
                        # Fallback to individual requests for this batch
                        for k, message in enumerate(batch_messages):
                            try:
                                msg = self.service.users().messages().get(
                                    userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                                    fields='payload/headers,id,internalDate'
                                ).execute()
                                
                                # Check if msg is valid
                                if not msg or not isinstance(msg, dict):
                                    print(f"Warning: Invalid response for message {message['id']}")
                                    continue
                                    
                                headers = msg.get('payload', {}).get('headers', [])
                                subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                                sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                                
                                # Add date information
                                date_str = ''
                                try:
                                    ts_ms = int(msg.get('internalDate', '0') or '0')
                                    if ts_ms:
                                        date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                                except Exception:
                                    date_str = ''
                                
                                emails.append({'id': message['id'], 'subject': subject, 'sender': sender, 'date': date_str, 'snippet': ''})
                                
                                # Update progress
                                if hasattr(self, 'command_id') and self.command_id:
                                    update_email_progress(self.command_id, i + k + 1, total_emails)
                            except HttpError:
                                continue
            
            # Mark batch processing as successful if we processed any emails
            if emails:
                batch_processing_successful = True
        
        # If batch processing failed but we have some emails, return them
        if batch_processing_attempted and not batch_processing_successful and emails:
            return {"emails": emails, "next_page_token": next_token}
        
        # Always return emails if we have any, regardless of processing method
        if emails:
            return {"emails": emails, "next_page_token": next_token}
        
        if not batch_processing_attempted or not batch_processing_successful:
            # For small numbers, use individual requests
            total_emails = len(messages)
            
            # Update progress with total emails
            if hasattr(self, 'command_id') and self.command_id:
                from agent.views import update_email_progress
                update_email_progress(self.command_id, 0, total_emails)
            
            for i, message in enumerate(messages):
                try:
                    msg = self.service.users().messages().get(
                        userId='me', id=message['id'], format='metadata', metadataHeaders=['From','Subject'],
                        fields='payload/headers,id,internalDate').execute()
                    
                    # Check if msg is None or doesn't have expected structure
                    if not msg or not isinstance(msg, dict):
                        print(f"Warning: Invalid response for message {message['id']}")
                        continue
                        
                    headers = msg.get('payload', {}).get('headers', [])
                    subject = next((h['value'] for h in headers if h.get('name') == 'Subject'), 'No Subject')
                    sender = next((h['value'] for h in headers if h.get('name') == 'From'), 'Unknown Sender')
                    
                    # Add date information
                    date_str = ''
                    try:
                        ts_ms = int(msg.get('internalDate', '0') or '0')
                        if ts_ms:
                            date_str = datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')
                    except Exception:
                        date_str = ''
                    
                    emails.append({'id': message['id'], 'subject': subject, 'sender': sender, 'date': date_str, 'snippet': ''})
                    
                    # Update progress
                    if hasattr(self, 'command_id') and self.command_id:
                        update_email_progress(self.command_id, i + 1, total_emails)
                        
                except Exception as e:
                    print(f"Error processing message {message['id']}: {e}")
                    continue
            return {"emails": emails, "next_page_token": next_token}
    
    def restore_emails_from_sender(self, sender_email, confirm=False):
        """Restore archived emails from a specific sender back to inbox"""
        try:
            # Search for emails from the sender
            # Quote multi-word senders so both words are matched as a single unit
            sender_term = f'"{sender_email}"' if ' ' in str(sender_email).strip() else sender_email
            query = f"from:{sender_term}"
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
            labels = self.api_list_labels()
            existing_labels = [label['name'] for label in labels.get('labels', [])]
            
            if label_name in existing_labels:
                return label_name
            
            # Create new label
            label_object = {
                'name': label_name,
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show'
            }
            
            created_label = self.api_create_label(label_object)
            
            return label_name
            
        except HttpError as error:
            print(f"❌ Error creating label: {error}")
            return None

    def get_label_id(self, label_name):
        """Get the ID of a label by name"""
        try:
            labels = self.api_list_labels()
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
            sender_term = f'"{sender_email}"' if ' ' in str(sender_email).strip() else sender_email
            query = f"from:{sender_term}"
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
                    self.api_batch_modify(batch_ids, add_label_ids=['INBOX'])
                del self._undo_store[action_id]
                return {"status": "success", "message": _("Undo complete. Restored %(count)d emails to Inbox.") % {"count": len(msg_ids)}, "undone_count": len(msg_ids)}
            elif action_type == 'trash':
                # Untrash and add INBOX
                restored = 0
                for msg_id in msg_ids:
                    try:
                        self.api_untrash(msg_id)
                        self.api_modify(msg_id, add_label_ids=['INBOX'])
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
                    self.api_batch_modify(batch_ids, remove_label_ids=[label_id])
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
                res = self.api_list_messages(**kwargs)
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
                    msg = self.api_get_message(
                        mid,
                        format='metadata',
                        metadataHeaders=['To', 'Cc', 'Bcc'],
                        fields='payload/headers,id'
                    )
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
