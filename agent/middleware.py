"""
Middleware to handle session cookie security for both HTTP (localhost) and HTTPS (ngrok)
"""
from django.conf import settings


class DynamicSessionCookieSecureMiddleware:
    """
    Middleware to set SESSION_COOKIE_SECURE dynamically based on request.
    This allows cookies to work for both HTTP (localhost) and HTTPS (ngrok).
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Temporarily set SESSION_COOKIE_SECURE based on whether request is secure
        # This works for both localhost (HTTP) and ngrok (HTTPS via proxy)
        original_secure = settings.SESSION_COOKIE_SECURE
        if request.is_secure() or 'ngrok' in request.get_host().lower():
            # Request is HTTPS (via ngrok or direct HTTPS)
            settings.SESSION_COOKIE_SECURE = True
        else:
            # Request is HTTP (localhost)
            settings.SESSION_COOKIE_SECURE = False
        
        try:
            response = self.get_response(request)
            # Modify session cookie in response if needed
            session_cookie_name = settings.SESSION_COOKIE_NAME
            if session_cookie_name in response.cookies:
                cookie = response.cookies[session_cookie_name]
                # Set secure flag based on request
                cookie['secure'] = settings.SESSION_COOKIE_SECURE
            return response
        finally:
            # Restore original setting
            settings.SESSION_COOKIE_SECURE = original_secure

