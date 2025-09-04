from django.shortcuts import render
from django.http import JsonResponse, StreamingHttpResponse
from django.utils.translation import gettext as _
from django.utils import translation
from .email_ai_agent import GmailAIAgent
import os
import json
import re
import time
import threading
import random
import string

def get_progress_message(message_key, current_processed=None, total_emails=None, language_code=None):
    """
    Get language-appropriate progress messages.
    """
    if not language_code:
        # Use the same language detection as the rest of the code
        language_code = translation.get_language() or 'en'
    
    is_hebrew = language_code.startswith('he')
    
    messages = {
        'processing_emails_count': {
            'en': f"Processing emails... ({current_processed}/{total_emails})",
            'he': f"מעבד מיילים... ({current_processed}/{total_emails})"
        },
        'processing_emails': {
            'en': "Processing emails...",
            'he': "מעבד מיילים..."
        },
        'analyzing_data': {
            'en': "Analyzing data...",
            'he': "מנתח נתונים..."
        },
        'finalizing': {
            'en': "Finalizing...",
            'he': "מסיים..."
        },
        'processing_command': {
            'en': "Processing command...",
            'he': "מעבד פקודה..."
        },
        'starting_analysis': {
            'en': "Starting analysis...",
            'he': "מתחיל ניתוח..."
        },
        'finalizing_analysis': {
            'en': "Finalizing analysis...",
            'he': "מסיים ניתוח..."
        },
        'fetching_emails': {
            'en': "Fetching emails...",
            'he': "מביא מיילים..."
        },
        'processing_results': {
            'en': "Processing results...",
            'he': "מעבד תוצאות..."
        },
        'identifying_emails_delete': {
            'en': "Identifying emails to delete...",
            'he': "מזהה מיילים למחיקה..."
        },
        'processing_deletion': {
            'en': "Processing deletion...",
            'he': "מעבד מחיקה..."
        },
        'identifying_emails_archive': {
            'en': "Identifying emails to archive...",
            'he': "מזהה מיילים לארכיון..."
        },
        'processing_archive': {
            'en': "Processing archive...",
            'he': "מעבד ארכיון..."
        },
        'identifying_emails_label': {
            'en': "Identifying emails to label...",
            'he': "מזהה מיילים לתיוג..."
        },
        'applying_labels': {
            'en': "Applying labels...",
            'he': "מחיל תוויות..."
        },
        'sending_email': {
            'en': "Sending email...",
            'he': "שולח מייל..."
        },
        'identifying_emails_restore': {
            'en': "Identifying emails to restore...",
            'he': "מזהה מיילים לשחזור..."
        },
        'processing_restoration': {
            'en': "Processing restoration...",
            'he': "מעבד שחזור..."
        },
        'searching_emails': {
            'en': "Searching emails...",
            'he': "מחפש מיילים..."
        },
        'preparing_email': {
            'en': "Preparing email...",
            'he': "מכין מייל..."
        }
    }
    
    return messages.get(message_key, {}).get('he' if is_hebrew else 'en', messages[message_key]['en'])

def get_random_fun_fact(language_code=None):
    """Get a random fun fact in the appropriate language"""
    if not language_code:
        language_code = translation.get_language() or 'en'
    
    is_hebrew = language_code.startswith('he')
    
    fun_facts = {
        'en': [
            "💡 Did you know? The first email was sent in 1971 by Ray Tomlinson to himself!",
            "📧 Fun fact: The '@' symbol in email addresses was chosen because it means 'at' in English.",
            "🌐 Interesting: Over 300 billion emails are sent every day worldwide!",
            "📱 Cool fact: The first mobile email was sent in 1996 using a Nokia 9000 Communicator.",
            "🔒 Security tip: The first spam email was sent in 1978 to 400 people advertising a computer.",
            "📊 Amazing: The average person receives 121 emails per day!",
            "⚡ Quick fact: Email was invented before the World Wide Web!",
            "🎯 Fun fact: The word 'email' was added to the Oxford English Dictionary in 1998.",
            "📈 Interesting: 99% of all email traffic is spam or marketing emails.",
            "🕒 Cool fact: The first email service provider was CompuServe, launched in 1989.",
            "📧 Did you know? The longest email address allowed is 320 characters!",
            "🌍 Amazing: Email is used by 4.1 billion people worldwide.",
            "💾 Fun fact: The first email attachment was sent in 1992.",
            "📱 Interesting: 60% of emails are opened on mobile devices.",
            "🔍 Cool fact: The first email search engine was created in 1995.",
            "📧 Fun fact: The first email client was called 'Mailbox' and ran on MIT's CTSS system.",
            "🌐 Interesting: The term 'spam' for unwanted emails comes from a Monty Python sketch!",
            "📱 Cool fact: The first webmail service was Hotmail, launched in 1996.",
            "💡 Did you know? The first email virus was called 'ILOVEYOU' and spread in 2000.",
            "📊 Amazing: The average office worker spends 2.5 hours per day on email!",
            "⚡ Quick fact: The first email was sent between two computers sitting next to each other.",
            "🎯 Fun fact: Gmail was launched on April 1st, 2004 - many thought it was an April Fool's joke!",
            "📈 Interesting: The first email marketing campaign was sent in 1978 to 400 people.",
            "🕒 Cool fact: The first email emoticon was :-) created in 1982 by Scott Fahlman.",
            "📧 Did you know? The first email was sent over ARPANET, the precursor to the internet.",
            "🌍 Amazing: The first email sent from space was in 1991 by the STS-43 crew.",
            "💾 Fun fact: The first email attachment was a picture of a band called 'Severe Tire Damage'.",
            "📱 Interesting: The first email sent from a mobile phone was in 1996 using a Nokia 9000.",
            "🔍 Cool fact: The first email service to offer 1GB of storage was Gmail in 2004.",
            "📧 Fun fact: The first email was sent at 10:30 PM on October 29, 1969.",
            "🌐 Interesting: The first email was sent between UCLA and Stanford University.",
            "📱 Cool fact: The first email app for iPhone was released in 2007.",
            "💡 Did you know? The first email was sent using the @ symbol to separate user and host.",
            "📊 Amazing: The first email was sent over a 50-kilobit connection!",
            "⚡ Quick fact: The first email was sent using the SNDMSG command.",
            "🎯 Fun fact: The first email was sent using the TENEX operating system.",
            "📈 Interesting: The first email was sent using the ARPANET protocol.",
            "🕒 Cool fact: The first email was sent using the IMP (Interface Message Processor).",
            "📧 Did you know? The first email was sent using the 1822 protocol.",
            "🌍 Amazing: The first email was sent using the BBN IMP-0 computer.",
            "💾 Fun fact: The first email was sent using the SDS Sigma 7 computer.",
            "📱 Interesting: The first email was sent using the SDS 940 computer.",
            "🔍 Cool fact: The first email was sent using the PDP-10 computer.",
            "📧 Fun fact: The first email was sent using the TOPS-10 operating system."
        ],
        'he': [
            "💡 ידעת? המייל הראשון נשלח ב-1971 על ידי ריי טומלינסון לעצמו!",
            "📧 עובדה מעניינת: הסימן '@' בכתובות מייל נבחר כי הוא אומר 'ב' בעברית.",
            "🌐 מעניין: יותר מ-300 מיליארד מיילים נשלחים מדי יום ברחבי העולם!",
            "📱 עובדה מגניבה: המייל הנייד הראשון נשלח ב-1996 באמצעות Nokia 9000 Communicator.",
            "🔒 טיפ אבטחה: הספאם הראשון נשלח ב-1978 ל-400 אנשים לפרסום מחשב.",
            "📊 מדהים: האדם הממוצע מקבל 121 מיילים ביום!",
            "⚡ עובדה מהירה: המייל הומצא לפני האינטרנט!",
            "🎯 עובדה מעניינת: המילה 'מייל' נוספה למילון אוקספורד ב-1998.",
            "📈 מעניין: 99% מכל תעבורת המיילים היא ספאם או מיילים שיווקיים.",
            "🕒 עובדה מגניבה: ספק המיילים הראשון היה CompuServe, שהושק ב-1989.",
            "📧 ידעת? כתובת המייל הארוכה ביותר המותרת היא 320 תווים!",
            "🌍 מדהים: מייל משמש 4.1 מיליארד אנשים ברחבי העולם.",
            "💾 עובדה מעניינת: הקובץ המצורף הראשון נשלח ב-1992.",
            "📱 מעניין: 60% מהמיילים נפתחים במכשירים ניידים.",
            "🔍 עובדה מגניבה: מנוע החיפוש הראשון למיילים נוצר ב-1995.",
            "📧 עובדה מעניינת: הלקוח הראשון למייל נקרא 'Mailbox' ורץ על מערכת CTSS של MIT.",
            "🌐 מעניין: המונח 'ספאם' למיילים לא רצויים מגיע מסקיצה של מונטי פייתון!",
            "📱 עובדה מגניבה: שירות הדוא\"ל הראשון באינטרנט היה Hotmail, שהושק ב-1996.",
            "💡 ידעת? הנגיף הראשון במייל נקרא 'ILOVEYOU' והתפשט ב-2000.",
            "📊 מדהים: העובד הממוצע במשרד מבלה 2.5 שעות ביום במייל!",
            "⚡ עובדה מהירה: המייל הראשון נשלח בין שני מחשבים שישבו זה ליד זה.",
            "🎯 עובדה מעניינת: Gmail הושק ב-1 באפריל 2004 - רבים חשבו שזה בדיחת אחד באפריל!",
            "📈 מעניין: קמפיין השיווק הראשון במייל נשלח ב-1978 ל-400 אנשים.",
            "🕒 עובדה מגניבה: האמוג'י הראשון במייל היה :-) שנוצר ב-1982 על ידי סקוט פאלמן.",
            "📧 ידעת? המייל הראשון נשלח דרך ARPANET, הקודם לאינטרנט.",
            "🌍 מדהים: המייל הראשון מהחלל נשלח ב-1991 על ידי צוות STS-43.",
            "💾 עובדה מעניינת: הקובץ המצורף הראשון במייל היה תמונה של להקה בשם 'Severe Tire Damage'.",
            "📱 מעניין: המייל הראשון מהטלפון הנייד נשלח ב-1996 באמצעות Nokia 9000.",
            "🔍 עובדה מגניבה: שירות המייל הראשון שהציע 1GB אחסון היה Gmail ב-2004.",
            "📧 עובדה מעניינת: המייל הראשון נשלח ב-22:30 ב-29 באוקטובר 1969.",
            "🌐 מעניין: המייל הראשון נשלח בין UCLA לאוניברסיטת סטנפורד.",
            "📱 עובדה מגניבה: האפליקציה הראשונה למייל לאייפון שוחררה ב-2007.",
            "💡 ידעת? המייל הראשון נשלח באמצעות הסימן @ להפרדה בין משתמש למארח.",
            "📊 מדהים: המייל הראשון נשלח דרך חיבור של 50 קילוביט!",
            "⚡ עובדה מהירה: המייל הראשון נשלח באמצעות פקודת SNDMSG.",
            "🎯 עובדה מעניינת: המייל הראשון נשלח באמצעות מערכת ההפעלה TENEX.",
            "📈 מעניין: המייל הראשון נשלח באמצעות פרוטוקול ARPANET.",
            "🕒 עובדה מגניבה: המייל הראשון נשלח באמצעות IMP (מעבד הודעות ממשק).",
            "📧 ידעת? המייל הראשון נשלח באמצעות פרוטוקול 1822.",
            "🌍 מדהים: המייל הראשון נשלח באמצעות מחשב BBN IMP-0.",
            "💾 עובדה מעניינת: המייל הראשון נשלח באמצעות מחשב SDS Sigma 7.",
            "📱 מעניין: המייל הראשון נשלח באמצעות מחשב SDS 940.",
            "🔍 עובדה מגניבה: המייל הראשון נשלח באמצעות מחשב PDP-10.",
            "📧 עובדה מעניינת: המייל הראשון נשלח באמצעות מערכת ההפעלה TOPS-10."
        ]
    }
    
    facts = fun_facts.get('he' if is_hebrew else 'en', fun_facts['en'])
    return random.choice(facts)



# Instantiate the agent once.
# In a real app, you might use a singleton pattern or Django's app registry
# to manage the agent's lifecycle.
gemini_api_key = os.getenv('GEMINI_API_KEY')
if not gemini_api_key:
    gemini_api_key = "AIzaSyCLldSk-Pv0X6-nPOOjjYMEbB0AsuatmJc"
agent_instance = GmailAIAgent(gemini_api_key=gemini_api_key)

def process_with_detailed_progress(agent, command, command_id, start_progress, end_progress, language_code=None):
    """
    Process command with detailed progress updates during execution.
    """
    import threading
    import time
    
    # Determine command types
    is_stats_command = 'stats' in command.lower()
    is_full_analysis = 'full analysis' in command.lower()
    
    if is_stats_command or is_full_analysis:
        # Both stats and full analysis use real progress tracking (separate instances)
        return process_with_real_progress(agent, command, command_id, start_progress, end_progress, language_code)
    else:
        # For other commands, use simulated progress
        progress_thread = threading.Thread(target=simulate_progress, args=(command_id, start_progress, end_progress, is_stats_command, language_code))
        progress_thread.daemon = True
        progress_thread.start()
        
        # Execute the actual command
        result = agent.process_natural_language_command(command)
        
        # Stop the progress simulation
        if command_id in progress_data:
            progress_data[command_id]['stop_simulation'] = True
        
        return result

def process_with_real_progress(agent, command, command_id, start_progress, end_progress, language_code=None):
    """
    Process stats and full analysis commands with real progress tracking from email processing.
    """
    import threading
    import time
    
    # Set up real progress tracking
    progress_data[command_id]['real_progress'] = True
    progress_data[command_id]['current_processed'] = 0
    progress_data[command_id]['total_emails'] = 0
    progress_data[command_id]['language_code'] = language_code or translation.get_language() or 'en'
    
    # Set command_id in agent for progress updates
    agent.current_command_id = command_id
    
    # Start a progress monitoring thread
    progress_thread = threading.Thread(target=monitor_real_progress, args=(command_id, start_progress, end_progress, command))
    progress_thread.daemon = True
    progress_thread.start()
    
    # Execute the actual command
    result = agent.process_natural_language_command(command)
    
    # Stop the progress monitoring
    if command_id in progress_data:
        progress_data[command_id]['stop_simulation'] = True
    
    # Clear command_id from agent
    agent.current_command_id = None
    
    return result

def monitor_real_progress(command_id, start_progress, end_progress, command=None):
    """
    Monitor real progress for stats and full analysis commands.
    """
    import time
    
    last_fun_fact_time = 0
    # Use different timing for full analysis vs show email stats
    is_full_analysis = command and 'full analysis' in command.lower()
    fun_fact_interval = 6.0 if is_full_analysis else 4.5  # 6.0s for full analysis, 4.5s for stats
    current_fun_fact = None
    

    
    while True:
        if command_id not in progress_data:
            break
            
        if progress_data[command_id].get('stop_simulation'):
            break
            
        # Get current progress data
        current_processed = progress_data[command_id].get('current_processed', 0)
        total_emails = progress_data[command_id].get('total_emails', 0)
        
        if total_emails > 0:
            # Calculate progress percentage based on real email processing
            # Use the full progress range (10% to 99%) for email processing
            email_progress = current_processed / total_emails  # Use full range for email processing
            final_progress = start_progress + (end_progress - start_progress) * email_progress
            
            # Show fun facts that change every 2.5 seconds
            current_time = time.time()
            language_code = progress_data[command_id].get('language_code', 'en')
            
            # Show fun facts
            if current_time - last_fun_fact_time >= fun_fact_interval or current_fun_fact is None:
                current_fun_fact = get_random_fun_fact(language_code)
                last_fun_fact_time = current_time
            
            update_progress(command_id, int(final_progress), current_fun_fact)
        
        # Slightly less frequent to reduce overhead while staying smooth
        time.sleep(0.3)

def simulate_progress(command_id, start_progress, end_progress, is_stats_command=False, language_code=None):
    """
    Simulate smooth progress updates between start and end percentages.
    """
    current_progress = start_progress
    target_progress = end_progress
    
    while current_progress < target_progress:
        if command_id in progress_data and progress_data[command_id].get('stop_simulation'):
            break
            
        # Increment progress smoothly - slower for stats commands
        if is_stats_command:
            increment = (target_progress - start_progress) / 60  # 60 steps for stats (slower)
            sleep_time = 0.2  # Longer delay for stats
        else:
            increment = (target_progress - start_progress) / 20  # 20 steps for regular commands
            sleep_time = 0.1  # Normal delay for regular commands
            
        current_progress += increment
        
        if current_progress > target_progress:
            current_progress = target_progress
            
        # Update progress with appropriate message (no fun facts for normal commands)
        if not language_code:
            language_code = progress_data[command_id].get('language_code', 'en')
        if current_progress < start_progress + (target_progress - start_progress) * 0.3:
            message = get_progress_message('processing_emails', language_code=language_code)
        elif current_progress < start_progress + (target_progress - start_progress) * 0.7:
            message = get_progress_message('analyzing_data', language_code=language_code)
        else:
            message = get_progress_message('finalizing', language_code=language_code)
            
        update_progress(command_id, int(current_progress), message)
        time.sleep(sleep_time)  # Variable delay based on command type

def process_command_with_progress(agent, command, command_id, language_code=None):
    """
    Process a command with real-time progress updates.
    """
    start_time = time.time()
    
    try:
        # Start progress immediately
        update_progress(command_id, 5, "Starting command...")
        
        update_progress(command_id, 10, "Parsing command...")
        
        # Parse the command to understand what we're doing
        parsed = agent.parse_command_manually(command)
        action = parsed.get('action', '')
        
        update_progress(command_id, 15, f"Command parsed: {action}")
        
        # Update progress based on command type with more granular updates
        if action == 'list':
            update_progress(command_id, 20, get_progress_message('fetching_emails', language_code=language_code))
            result = process_with_detailed_progress(agent, command, command_id, 20, 99, language_code)
            update_progress(command_id, 99, get_progress_message('processing_results', language_code=language_code))
        elif action == 'search':
            update_progress(command_id, 25, get_progress_message('searching_emails', language_code=language_code))
            result = process_with_detailed_progress(agent, command, command_id, 25, 99, language_code)
            update_progress(command_id, 99, get_progress_message('processing_results', language_code=language_code))
        elif action == 'delete':
            update_progress(command_id, 30, get_progress_message('identifying_emails_delete', language_code=language_code))
            result = process_with_detailed_progress(agent, command, command_id, 30, 99, language_code)
            update_progress(command_id, 99, get_progress_message('processing_deletion', language_code=language_code))
        elif action == 'archive':
            update_progress(command_id, 30, get_progress_message('identifying_emails_archive', language_code=language_code))
            result = process_with_detailed_progress(agent, command, command_id, 30, 99, language_code)
            update_progress(command_id, 99, get_progress_message('processing_archive', language_code=language_code))
        elif action == 'label':
            update_progress(command_id, 30, get_progress_message('identifying_emails_label', language_code=language_code))
            result = process_with_detailed_progress(agent, command, command_id, 30, 99, language_code)
            update_progress(command_id, 99, get_progress_message('applying_labels', language_code=language_code))
        elif action == 'send':
            update_progress(command_id, 40, get_progress_message('preparing_email', language_code=language_code))
            result = process_with_detailed_progress(agent, command, command_id, 40, 99, language_code)
            update_progress(command_id, 99, get_progress_message('sending_email', language_code=language_code))
        elif action == 'restore':
            update_progress(command_id, 30, get_progress_message('identifying_emails_restore', language_code=language_code))
            result = process_with_detailed_progress(agent, command, command_id, 30, 99, language_code)
            update_progress(command_id, 99, get_progress_message('processing_restoration', language_code=language_code))
        elif action == 'stats' or 'full analysis' in command.lower():
            # Both stats and full analysis use real progress (X/Y) - separate instances
            update_progress(command_id, 10, get_progress_message('starting_analysis', language_code=language_code))
            result = process_with_detailed_progress(agent, command, command_id, 10, 99, language_code)
            update_progress(command_id, 99, get_progress_message('finalizing_analysis', language_code=language_code))
        else:
            # Generic command processing
            update_progress(command_id, 20, get_progress_message('processing_command', language_code=language_code))
            result = process_with_detailed_progress(agent, command, command_id, 20, 99, language_code)
            update_progress(command_id, 99, get_progress_message('finalizing', language_code=language_code))
        
        # Calculate total execution time
        end_time = time.time()
        total_time = end_time - start_time
        
        # Print timing info to terminal
        print(f"⏱️  Command '{command}' completed in {total_time:.2f} seconds")
        
        # Complete progress with timing info
        update_progress(command_id, 100, f"Complete! ({total_time:.1f}s)", complete=True)
        return result
        
    except Exception as e:
        # Calculate time even for errors
        end_time = time.time()
        total_time = end_time - start_time
        
        # Print timing info to terminal
        print(f"❌ Command '{command}' failed after {total_time:.2f} seconds: {str(e)}")
        
        # Error progress
        update_progress(command_id, 100, f"Error: {str(e)}", complete=True)
        return {"status": "error", "message": str(e)}

def index(request):
    """
    Main view for the email agent.
    Handles command processing and displays results.
    """
    # Attempt to set up the Gmail API on each request if not already done.
    # The setup_gmail_api method is now idempotent.
    is_agent_ready = agent_instance.setup_gmail_api()
    
    context = {
        'is_agent_ready': is_agent_ready,
        'example_commands': [
            _("delete all promotions older than 30 days"),
            _("list emails from google.com"),
            _("show email stats"),
            _("send email"),
            _("label emails from amazon.com as Shopping"),
        ]
    }

    if not is_agent_ready:
        context['error_message'] = "Failed to connect to Gmail API. Please ensure 'credentials.json' is present and that you have authenticated at least once if 'token.pickle' does not exist."
        return render(request, 'agent/index.html', context)

    if request.method == 'POST':
        # Hide a contact (stored in session) - AJAX
        if request.POST.get('hide_contact') == '1':
            try:
                addr = (request.POST.get('email') or '').strip().lower()
                if addr:
                    hidden = request.session.get('hidden_contacts', [])
                    if addr not in hidden:
                        hidden.append(addr)
                        # keep at most 500 hidden contacts
                        if len(hidden) > 500:
                            hidden = hidden[-500:]
                        request.session['hidden_contacts'] = hidden
                return JsonResponse({"status": "ok"})
            except Exception as e:
                return JsonResponse({"status": "error", "error": str(e)}, status=500)

        # Contacts suggestions (AJAX)
        if request.POST.get('get_contacts') == '1':
            try:
                q = (request.POST.get('q') or '').strip().lower()
                try:
                    limit = int(request.POST.get('limit', '200'))
                except Exception:
                    limit = 200
                # Fetch and filter recent contacts from Sent mailbox
                contacts = agent_instance.get_recent_contacts(
                    max_messages=min(600, max(50, limit*2)),
                    max_results=min(600, max(50, limit*2))
                )
                hidden = set(addr.lower() for addr in request.session.get('hidden_contacts', []) if isinstance(addr, str))
                # Merge session-saved custom contacts at the top
                custom_saved = request.session.get('custom_contacts', [])
                try:
                    for addr in custom_saved:
                        if not isinstance(addr, str):
                            continue
                        em = (addr or '').strip()
                        if not em or em.lower() in hidden:
                            continue
                        if not any((c.get('email', '').strip().lower() == em.lower()) for c in contacts):
                            contacts.insert(0, {"email": em, "name": "", "count": 999999})
                except Exception:
                    pass
                results = []
                seen = set()
                for c in contacts:
                    email_addr = (c.get('email', '') or '').strip()
                    name = c.get('name', '')
                    key = email_addr.lower()
                    if not email_addr or key in hidden or key in seen:
                        continue
                    if not q or (q in key) or (name and q in name.lower()):
                        results.append({"email": email_addr, "name": name})
                        seen.add(key)
                    if len(results) >= min(20, max(10, limit)):
                        break
                return JsonResponse({"contacts": results})
            except Exception as e:
                return JsonResponse({"contacts": [], "error": str(e)}, status=500)

        # Handle Load More (AJAX)
        if request.POST.get('load_more') == '1':
            try:
                token = request.POST.get('load_more_token')
                list_context_raw = request.POST.get('list_context', '{}')
                list_context = json.loads(list_context_raw)
                mode = list_context.get('mode')
                if not token or not mode:
                    return JsonResponse({"error": "Invalid pagination request"}, status=400)
                if mode == 'label':
                    label = list_context.get('label')
                    per_page = request.session.get('per_page', 50)
                    res = agent_instance.get_emails_by_label(label, max_results=per_page, page_token=token)
                    emails = res.get('emails', []) if isinstance(res, dict) else []
                    next_token = res.get('next_page_token') if isinstance(res, dict) else None
                    return JsonResponse({"data": emails, "next_page_token": next_token})
                if mode == 'date_range':
                    target = list_context.get('target')
                    per_page = request.session.get('per_page', 50)
                    res = agent_instance.list_emails_by_date_range(target, max_results=per_page, page_token=token)
                    emails = res.get('emails', [])
                    next_token = res.get('next_page_token')
                    return JsonResponse({"data": emails, "next_page_token": next_token})
                if mode == 'category':
                    category = list_context.get('category')
                    per_page = request.session.get('per_page', 50)
                    res = agent_instance.list_emails_by_category(category, max_results=per_page, page_token=token)
                    emails = res.get('emails', []) if isinstance(res, dict) else []
                    next_token = res.get('next_page_token') if isinstance(res, dict) else None
                    return JsonResponse({"data": emails, "next_page_token": next_token})
                if mode == 'older_than':
                    target = list_context.get('target')
                    per_page = request.session.get('per_page', 50)
                    res = agent_instance.list_emails_older_than(target, max_results=per_page, page_token=token)
                    emails = res.get('emails', [])
                    next_token = res.get('next_page_token')
                    return JsonResponse({"data": emails, "next_page_token": next_token})
                if mode == 'recent':
                    per_page = request.session.get('per_page', 50)
                    res = agent_instance.list_recent_emails(max_results=per_page, page_token=token)
                    return JsonResponse({"data": res.get('emails', []), "next_page_token": res.get('next_page_token')})
                if mode == 'archived':
                    per_page = request.session.get('per_page', 50)
                    res = agent_instance.list_archived_emails(max_results=per_page, page_token=token)
                    return JsonResponse({"data": res.get('emails', []), "next_page_token": res.get('next_page_token')})
                if mode == 'all_mail':
                    per_page = request.session.get('per_page', 50)
                    res = agent_instance.list_all_emails(max_results=per_page, page_token=token)
                    return JsonResponse({"data": res.get('emails', []), "next_page_token": res.get('next_page_token')})
                if mode == 'custom_category':
                    per_page = request.session.get('per_page', 50)
                    category_key = list_context.get('category') or list_context.get('category_key')
                    older_than_days = list_context.get('older_than_days')
                    res = agent_instance.list_emails_by_custom_category(category_key, max_results=per_page, page_token=token, older_than_days=older_than_days)
                    return JsonResponse({"data": res.get('emails', []), "next_page_token": res.get('next_page_token')})
                if mode == 'domain':
                    target = list_context.get('target')
                    per_page = request.session.get('per_page', 50)
                    older = list_context.get('older_than_days')
                    res = agent_instance.list_emails_by_domain(target, max_results=per_page, page_token=token, older_than_days=older)
                    return JsonResponse({"data": res.get('emails', []), "next_page_token": res.get('next_page_token')})
                if mode == 'sender':
                    target = list_context.get('target')
                    per_page = request.session.get('per_page', 50)
                    older = list_context.get('older_than_days')
                    res = agent_instance.list_emails_by_sender(target, max_results=per_page, page_token=token, older_than_days=older)
                    return JsonResponse({"data": res.get('emails', []), "next_page_token": res.get('next_page_token')})
                return JsonResponse({"error": "Unsupported pagination mode"}, status=400)
            except Exception as e:
                return JsonResponse({"error": str(e)}, status=500)

        # Favorites management (add/remove)
        if request.POST.get('add_favorite') == '1':
            fav_cmd = request.POST.get('fav_command', '').strip()
            if fav_cmd:
                favs = request.session.get('favorite_commands', [])
                # Guard against overly long junk favorites and de-dup by moving to front
                if len(fav_cmd) > 200:
                    context['result'] = {"status": "error", "message": _("Favorite too long (max 200 chars).")}
                else:
                    if fav_cmd in favs:
                        favs = [c for c in favs if c != fav_cmd]
                    favs.insert(0, fav_cmd)
                    favs = favs[:12]
                    request.session['favorite_commands'] = favs
            if 'result' not in context:
                context['result'] = {"status": "success", "message": _("Added to favorites.")}
        elif request.POST.get('remove_favorite') == '1':
            fav_cmd = request.POST.get('fav_command', '').strip()
            favs = request.session.get('favorite_commands', [])
            if fav_cmd in favs:
                favs = [c for c in favs if c != fav_cmd]
                request.session['favorite_commands'] = favs
            context['result'] = {"status": "success", "message": _("Removed from favorites.")}
        elif request.POST.get('clear_favorites') == '1':
            request.session['favorite_commands'] = []
            context['result'] = {"status": "success", "message": _("Cleared all favorites.")}

        # unified handling for actions/commands
        result = None
        last_command = ''

        if request.POST.get('stats_full') == '1':
            # Use the new progress system for full analysis
            command_id = request.POST.get('command_id')
            if not command_id:
                command_id = f"cmd_{int(time.time() * 1000)}_{''.join(random.choices(string.ascii_lowercase + string.digits, k=10))}"
            progress_data[command_id] = {
                'progress': 0,
                'message': 'Starting full analysis...',
                'complete': False,
                'real_progress': True,
                'current_processed': 0,
                'total_emails': 0
            }
            # Get language code for progress messages
            language_code = getattr(request, 'LANGUAGE_CODE', None) or translation.get_language() or 'en'
            result = process_command_with_progress(agent_instance, "full analysis", command_id, language_code)
        else:
            undo_action_id = request.POST.get('undo_action_id')
            confirmation_data_str = request.POST.get('confirmation_data', '')
            command = request.POST.get('command', '').strip()
            last_command = command
            print(f"Received command: '{command}'")  # Debug log
            print(f"Language code: {getattr(request, 'LANGUAGE_CODE', None)}")  # Debug log
            print(f"Translation language: {translation.get_language()}")  # Debug log
            # If Hebrew locale, map common Hebrew phrases to English before parsing
            if (getattr(request, 'LANGUAGE_CODE', None) or translation.get_language() or '').startswith('he') and command:
                print("Hebrew locale detected, translating command...")  # Debug log
                original_command = command
                command = _translate_hebrew_command_to_english(command)
                print(f"Hebrew command translation: '{original_command}' -> '{command}'")  # Debug log
            else:
                print("Not Hebrew locale or no command")  # Debug log
            # Results-per-page selection (persist in session)
            per_page_raw = request.POST.get('per_page')
            if per_page_raw:
                try:
                    per_page_val = int(per_page_raw)
                    if per_page_val in [10,25,50,100]:
                        request.session['per_page'] = per_page_val
                except Exception:
                    pass
            per_page = request.session.get('per_page', 50)

            # Compose email submit handler
            if request.POST.get('compose_send') == '1':
                to_addr = request.POST.get('compose_to', '').strip()
                subject = request.POST.get('compose_subject', '').strip()
                body = request.POST.get('compose_body', '').strip()
                if not to_addr:
                    result = {"status": "error", "message": _("Recipient is required.")}
                else:
                    send_res = agent_instance.send_email(to_addr, subject, body)
                    if isinstance(send_res, dict) and send_res.get('success'):
                        result = {"status": "success", "message": _("Email sent.")}
                        # Save valid recipient to custom contacts for autocomplete
                        try:
                            addr_norm = (to_addr or '').strip()
                            if hasattr(agent_instance, '_is_valid_email_address') and agent_instance._is_valid_email_address(addr_norm):
                                hidden = set(a.lower() for a in request.session.get('hidden_contacts', []) if isinstance(a, str))
                                if addr_norm.lower() not in hidden:
                                    saved = request.session.get('custom_contacts', [])
                                    lower_set = set(a.lower() for a in saved if isinstance(a, str))
                                    if addr_norm.lower() not in lower_set:
                                        saved.insert(0, addr_norm)
                                        request.session['custom_contacts'] = saved[:200]
                        except Exception:
                            pass
                    else:
                        err = ''
                        if isinstance(send_res, dict):
                            err = send_res.get('error', '')
                        result = {"status": "error", "message": _("Failed to send email: %(err)s") % {"err": err}}
            elif undo_action_id:
                result = agent_instance.undo_action(undo_action_id)
            elif confirmation_data_str:
                try:
                    confirmation_data = json.loads(confirmation_data_str)
                    result = agent_instance.process_natural_language_command(command=None, confirmation_data=confirmation_data)
                except json.JSONDecodeError:
                    result = {"status": "error", "message": "Invalid confirmation data."}
            elif command:
                # Get command ID from form or generate new one
                command_id = request.POST.get('command_id', '')
                if not command_id:
                    import uuid
                    command_id = str(uuid.uuid4())
                
                # Inject per-page into list commands by temporarily setting default
                prev_default = getattr(agent_instance, 'default_max_results', 50)
                agent_instance.default_max_results = per_page
                try:
                    # If user asked to compose without details, show compose UI instead of parsing
                    normalized = (command or '').strip().lower()
                    if normalized in [
                        'send email', 'compose email', 'send mail', 'compose'
                    ]:
                        context['show_compose'] = True
                        result = {"status": "info", "message": _("Compose Email")}
                    else:
                        # Process command with progress updates
                        language_code = getattr(request, 'LANGUAGE_CODE', None) or translation.get_language() or 'en'
                        result = process_command_with_progress(agent_instance, command, command_id, language_code)
                finally:
                    agent_instance.default_max_results = prev_default
                # Track history in session for autocomplete
                try:
                    hist = request.session.get('command_history', [])
                    if command and command not in hist:
                        hist.insert(0, command)
                        hist = hist[:20]
                        request.session['command_history'] = hist
                except Exception:
                    pass

        if isinstance(result, dict) and result.get('list_context'):
            try:
                result['list_context_json'] = json.dumps(result.get('list_context'))
            except Exception:
                result['list_context_json'] = ''

        if result is not None:
            context['result'] = result
        if last_command:
            context['last_command'] = last_command

        # If the action requires a second step (confirmation), prepare data for the template.
        result_obj = context.get('result')
        if isinstance(result_obj, dict) and result_obj.get('status') == 'confirmation_required':
            context['confirmation_data'] = json.dumps(result_obj.get('action_details'))
            context['confirmation_message'] = result_obj.get('message')
            # Pass preview and total_estimated for the dialog
            context['result']['preview'] = result_obj.get('preview')
            context['result']['total_estimated'] = result_obj.get('total_estimated')

    # Add example commands for the user interface
    example_commands = [
        _("list recent emails"),
        _("list emails from [domain]"),
        _("list emails from last week"),
        _("list emails older than 6 months"),
        _("list archived emails"),
        _("list all mail"),
        _("search emails with subject \"[keyword]\""),
        _("show email stats"),
        _("delete emails from [domain]"),
        _("delete all promotions older than 30 days"),
        _("archive all emails older than 1 year"),
        _("label emails from [domain] as \"[label]\""),
        _("list labels"),
        _("show label \"[label]\""),
        _("restore emails from [sender]"),
        _("send email")
    ]
    context['example_commands'] = example_commands

    # Provide favorites and history for quick commands and autocomplete
    favorites = request.session.get('favorite_commands', [])
    history = request.session.get('command_history', [])
    context['favorite_commands'] = favorites
    context['command_history'] = history

    # Build autocomplete list: commands only (no history/favorites)
    curated_extras = [
        _("list emails from today"),
        _("list emails from yesterday"),
        _("list emails from last week"),
        _("list emails from last month"),
        _("list emails from last year"),
        _("list emails before a week"),
        _("list emails before a month"),
        _("list emails before a year"),
        _("list emails older than [duration]"),
        _("list emails older than 1 year"),
        _("list emails older than 30 days"),
        _("list emails before [duration]"),
        _("list emails from [duration] ago"),
        _("list emails before 6 months"),
        _("list emails before 2 weeks"),
        _("list emails from 4 months ago"),
        _("list archived emails"),
        _("list all mail"),
        _("list emails from [domain] older than [duration]"),
        _("list emails from [sender] older than [duration]"),
        _("delete emails from [domain] older than 30 days"),
        _("archive emails from [sender]"),
        _("archive emails from [sender] older than 6 months"),
        _("list shipping emails"),
        _("list verification codes"),
        _("archive verification codes"),
        _("delete verification codes"),
        _("list verification codes older than [duration]"),
        _("delete verification codes older than 14 days"),
        _("archive verification codes older than 30 days"),
        _("list shipping emails older than [duration]"),
        _("archive shipping emails"),
        _("delete shipping emails"),
        _("archive shipping emails older than 90 days"),
        _("delete shipping emails older than 180 days"),
        _("list account security emails"),
        _("list account security emails older than [duration]"),
        _("archive account security emails older than 90 days"),
        _("delete account security emails older than 180 days"),
        _("label emails from [domain] as \"[label]\""),
        _("search emails with subject \"[keyword]\""),
        _("send email")
    ]
    seen_lower = set()
    autocomplete_list = []
    for arr in [example_commands, curated_extras]:
        for c in arr:
            key = c.lower()
            if key not in seen_lower:
                seen_lower.add(key)
                autocomplete_list.append(c)
    try:
        context['autocomplete_commands_json'] = json.dumps(autocomplete_list)
    except Exception:
        context['autocomplete_commands_json'] = json.dumps(example_commands)

    # Provide recent undoable actions and per-page to template
    context['recent_actions'] = agent_instance.get_recent_actions()
    context['per_page'] = request.session.get('per_page', 50)
    
    return render(request, 'agent/index.html', context)


def _translate_hebrew_command_to_english(command: str) -> str:
    """Best-effort mapping from common Hebrew phrases to the English grammar our parser expects."""
    if not command:
        return command
    c = command.strip()
    orig = c  # keep original for context-sensitive decisions
    print(f"Translating Hebrew command: '{orig}'")  # Debug log
    # Normalize punctuation styles
    # Longer phrases first
    replacements = [
        # Actions (line-start variants)
        (r"^[\s]*העבר\s+לארכיון", "archive "),
        (r"^[\s]*הצג\s+סטטיסטיק(?:ה|ות|ת)(?:\s+דוא[\"']?ל)?", "show email stats"),
        (r"^[\s]*הראה\s+סטטיסטיק(?:ה|ות|ת)(?:\s+דוא[\"']?ל)?", "show email stats"),
        (r"^[\s]*סטטיסטיק(?:ה|ות|ת)(?:\s+דוא[\"']?ל)?$", "show email stats"),
        # More flexible patterns for Hebrew stats commands
        (r"^[\s]*הצג\s+סטטיסטיק", "show email stats"),
        (r"^[\s]*הראה\s+סטטיסטיק", "show email stats"),
        (r"^[\s]*סטטיסטיק", "show email stats"),
        (r"^[\s]*(שלח|כתוב|חבר)\s+(דוא\"?ל|דואל|מייל)\s*$", "send email"),
        (r"^[\s]*שלח\s+(דוא""ל|מייל)\s+ל", "send email to "),
        (r"^[\s]*הצ[א]?ג\s+תווית\s+\"([^\"]+)\"", r'show label "\1"'),
        (r"^[\s]*הצ[א]?ג\s+תווית\s+'([^']+)'", r'show label "\1"'),
        (r"^[\s]*הצ[א]?ג\s+תווית\s+([^\s\"']+)", r'show label "\1"'),
        (r'^[\s]*([^"\s\']+)\s+הצ[א]?ג\s+תווית\b', r'show label "\1"'),
        (r"^[\s]*הצ[א]?ג|^[\s]*הראה|^[\s]*רש(?:ו)?ם", "list "),
        (r"^[\s]*מחק", "delete "),
        (r"^[\s]*ארכב", "archive "),
        (r"^[\s]*תייג|^[\s]*הוסף\s+תווית", "label "),
        (r"^[\s]*שחזר", "restore "),
        (r"^[\s]*חפש", "search "),

        # Time phrases and ranges
        (r"שבוע\s+שעבר", "last week"),
        (r"חודש\s+שעבר", "last month"),
        (r"שנה\s+שעברה", "last year"),
        (r"מהשבוע\s+שעבר", "from last week"),
        (r"מהחודש\s+שעבר", "from last month"),
        (r"מהשנה\s+שעברה", "from last year"),
        # 'before' without a number should mean the previous calendar period
        (r"\bלפני\s*שבוע(?:\b|$)", "last week"),
        (r"\bלפני\s*חודש(?:\b|$)", "last month"),
        (r"\bלפני\s*שנה(?:\b|$)", "last year"),
        (r"\bמלפני\s*שבוע(?:\b|$)", "last week"),
        (r"\bמלפני\s*חודש(?:\b|$)", "last month"),
        (r"\bמלפני\s*שנה(?:\b|$)", "last year"),
        (r"אתמול", "yesterday"),
        (r"היום", "today"),

        # 'older than' Hebrew forms (singular/plural) optionally with 'יותר' and with/without hyphen after 'מ'
        (r"ישנ(?:ה|ים|ות)?(?:\s+יותר)?\s+מ[-–—־]?\s*", "older than "),
        (r"לפני\s+", "older than "),

        # Units
        (r"\bיומיים\b", "2 days"),
        (r"\bיום\b", "day"),
        (r"\bימים\b", "days"),
        (r"\bשבועיים\b", "2 weeks"),
        (r"\bשבוע\b", "week"),
        (r"\bשבועות\b", "weeks"),
        (r"\bחודשיים\b", "2 months"),
        (r"\bחודש\b", "month"),
        (r"\bחודשים\b", "months"),
        (r"\bשנתיים\b", "2 years"),
        (r"\bשנה\b", "year"),
        (r"\bשנים\b", "years"),

        # Keywords and targets
        (r"אימיילים|מיילים|דואר|דוא""ל", "emails"),
        (r"\bאת\s*כל\b|\bכל\b", "all "),
        (r"עם\s+נושא", "with subject"),
        (r"קודים\s+לאימות|קוד(?:י)?\s+אימות", "verification codes"),
        (r"משלוח(?:ים)?|שילוח|משלוחים", "shipping emails"),
        (r"קידומי\s*המכירות|קידומי\s*מכירות|קידומי\s*שיווק|קידומי\s*המחירות|קידומי\s*המחירו?ת|קידומי\s*מכירה", "promotions"),
        (r"אבטחת\s+חשבון|אבטחה", "account security emails"),
        (r"קידומים|מבצעים|פרסומים", "promotions"),
        (r"תוויות", "labels"),
        (r"\bהצ[א]?ג\s+תווית\b", "show label"),

        # 'from' forms
        (r"\bמאת\s+", "from "),
        (r"\bמ\s+", "from "),
        # standalone prefix form with hyphen: 'מ-<token>' or 'מ־<token>' etc.
        (r"\bמ[-–—־]", "from "),
    ]
    for pattern, repl in replacements:
        try:
            if re.search(pattern, c, flags=re.IGNORECASE):
                print(f"Pattern matched: '{pattern}' -> '{repl}'")  # Debug log
                c = re.sub(pattern, repl, c, flags=re.IGNORECASE)
                print(f"After replacement: '{c}'")  # Debug log
        except re.error:
            continue
    # Convert 'מלפני/לפני' forms to 'from N unit ago' when written as a compound with 'מ'
    def _he_to_en_unit(unit_he: str, qty: int) -> str:
        u = unit_he
        if u.startswith('יום'):
            return 'day' if qty == 1 else 'days'
        if u.startswith('שבוע'):
            return 'week' if qty == 1 else 'weeks'
        if u.startswith('חודש'):
            return 'month' if qty == 1 else 'months'
        if u.startswith('שנה') or u.startswith('שנים'):
            return 'year' if qty == 1 else 'years'
        return 'days'
    def _mlifnei_repl(m):
        qty = int(m.group(1))
        unit_he = m.group(2)
        unit_en = _he_to_en_unit(unit_he, qty)
        return f"from {qty} {unit_en} ago"
    # 'מלפני 4 חודשים' or 'מ לפני 4 חודשים'
    c = re.sub(r"\bמ\s*לפני\s*(\d+)\s*(יום(?:ים)?|שבוע(?:ות)?|חודש(?:ים)?|שנה(?:ים)?)\b", _mlifnei_repl, c, flags=re.IGNORECASE)
    # For Hebrew 'לפני ...' we prefer a bounded window ("from N <unit> ago")
    # EXCEPT for custom categories (verification/shipping/security), where we keep 'older than'
    custom_hebrew_tokens = [
        r"קודים\s*לאימות", r"קוד\s*אימות", r"קודי\s*אימות",
        r"משלוח", r"שילוח", r"משלוחים",
        r"אבטחת\s*חשבון", r"אבטחה"
    ]
    is_custom_category_he = any(re.search(tok, orig) for tok in custom_hebrew_tokens)
    if (re.search(r"\bמלפני\b", orig) or re.search(r"\bמ\s*לפני\b", orig) or re.search(r"\bלפני\b", orig)) and not is_custom_category_he:
        c = re.sub(
            r"^(list\b.*)older than\s*(\d+)\s*(day|days|week|weeks|month|months|year|years)",
            lambda m: f"{m.group(1)}from {m.group(2)} {m.group(3)} ago",
            c,
            flags=re.IGNORECASE,
        )
    # If we have 'older than <unit>' without a number, assume 1 unit
    c = re.sub(r"\b(older than|before)\s+(day|days|week|weeks|month|months|year|years)\b",
               lambda m: f"{m.group(1)} 1 {m.group(2)}", c)
    # Normalize multiple spaces that can result from replacements
    c = re.sub(r"\s{2,}", " ", c).strip()
    print(f"Final translated command: '{c}'")  # Debug log
    return c

# Global progress tracking for SSE
progress_data = {}

def progress_stream(request, command_id):
    """
    Server-Sent Events endpoint for real-time progress updates.
    """
    # SSE endpoint called for progress updates
    
    def event_stream():
        # Starting event stream for progress updates
        while True:
            if command_id in progress_data:
                data = progress_data[command_id]
                # Debug logging removed for cleaner terminal output
                if data.get('complete'):
                    # Send final update and clean up
                    yield f"data: {json.dumps(data)}\n\n"
                    del progress_data[command_id]
                    # Progress tracking completed and cleaned up
                    break
                else:
                    yield f"data: {json.dumps(data)}\n\n"
            time.sleep(0.1)  # Check every 100ms
    
    response = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['Access-Control-Allow-Origin'] = '*'
    response['Access-Control-Allow-Headers'] = 'Cache-Control'
    return response

def update_progress(command_id, progress, message, complete=False):
    """
    Update progress for a specific command.
    """
    
    # Preserve existing data if it exists
    if command_id in progress_data:
        progress_data[command_id].update({
            'progress': progress,
            'message': message,
            'complete': complete,
            'timestamp': time.time()
        })
    else:
        progress_data[command_id] = {
            'progress': progress,
            'message': message,
            'complete': complete,
            'timestamp': time.time()
        }

def update_email_progress(command_id, current_processed, total_emails):
    """
    Update real email processing progress for full analysis commands.
    """
    if command_id in progress_data and progress_data[command_id].get('real_progress'):
        progress_data[command_id]['current_processed'] = current_processed
        progress_data[command_id]['total_emails'] = total_emails
        
        # Debug logging removed for cleaner terminal output
