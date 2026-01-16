"""
Text normalization utilities for cleaning and standardizing Reddit posts.

Provides high-precision regex patterns and normalization functions to replace
non-stylometric elements (URLs, emails, mentions, etc.) with standardized tokens,
preserving only the stylistic signals needed for authorship attribution.

Key Features:
    - Django-inspired URL/email validation patterns for high precision
    - Reddit-specific patterns (u/username, r/subreddit)
    - Markdown and special character cleanup
    - Token-based replacement to preserve text structure

Module-level regex patterns are defined for reuse across normalizer instances.
"""

import html
import re
import unicodedata

from pydantic import BaseModel, Field

# ----------------------------
# URL (Django URLValidator core pieces)
# ----------------------------
UL = "\u00a1-\uffff"  # Django's unicode letters range

regex_url_ipv4 = (
    r"(?:25[0-5]|2[0-4]\d|[0-1]?\d?\d)" r"(?:\.(?:25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}"
)
regex_url_ipv6_bracketed = r"\[[0-9a-f:\.]+\]"  # Django: simple, validated later
regex_url_hostname = r"[a-z" + UL + r"0-9](?:[a-z" + UL + r"0-9-]*[a-z" + UL + r"0-9])?"
regex_url_domain = r"(?:\.(?!-)[a-z" + UL + r"0-9-]+(?<!-))*"
regex_url_tld = r"\.(?!-)(?:[a-z" + UL + r"-]{2,}|xn--[a-z0-9]+)(?<!-)\.?"
regex_url_host = (
    r"(" + regex_url_hostname + regex_url_domain + regex_url_tld + r"|localhost)"
)

regex_url = (
    r"(?i)\b(?:https?|ftps?)://"
    r"(?:\S+(?::\S*)?@)?"
    r"(?:"
    + regex_url_ipv4
    + r"|"
    + regex_url_ipv6_bracketed
    + r"|"
    + regex_url_host
    + r")"
    r"(?::\d{2,5})?"
    r"(?:[/?#][^\s]*)?"
)

# ----------------------------
# Email (Django EmailValidator core pieces)
# ----------------------------
regex_email_user = (
    r"(?:"
    r"[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*"
    r'|"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"'
    r")"
)
regex_email_domain = (
    r"(?:"
    r"(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z0-9-]{2,63}(?<!-))"
    r"|\[([A-f0-9:\.]+)\]"
    r"|localhost"
    r")"
)

# In-text email finder
regex_email = (
    r"(?i)(?<![\w@])" + regex_email_user + r"@" + regex_email_domain + r"(?![\w@])"
)

# ----------------------------
# Reddit mentions
# ----------------------------
# Username mention: u/<name> (3–20 chars; letters/digits/_/-)
regex_reddit_user = r"(?i)(?<!\w)u/[a-z0-9_-]{3,20}\b"

# Subreddit mention: /r/<name> or r/<name>; cannot start with "_" and is 3–21 chars
regex_reddit_subreddit = r"(?i)\/?r\/([a-z0-9][_a-z0-9]{2,20})(?:\b|$)"


# Practical in-text IP finder: IPv4 (Django) OR a conservative IPv6 matcher
regex_ip_address = (
    r"(?i)\b(?:" + regex_url_ipv4 + r"|"
    r"(?:"
    r"(?:[0-9a-f]{1,4}:){2,7}[0-9a-f]{1,4}"
    r"|"
    r"(?:[0-9a-f]{1,4}:){1,7}:"
    r"|"
    r":(?:[0-9a-f]{1,4}:){1,7}"
    r")"
    r")\b"
)

regex_phone = r"(?<!\w)\+[1-9]\d{1,14}\b"
# ----------------------------
# File paths (Windows absolute/UNC + POSIX absolute)
# ----------------------------
regex_filepaths = (
    r"(?:(?:"
    # Windows drive path: C:\dir\file
    r"[A-Za-z]:\\(?:[^\\\r\n:*?\"<>|]+\\)*[^\\\r\n:*?\"<>|]+"
    r")|(?:"
    # Windows UNC path: \\server\share\dir\file
    r"\\\\[^\\\r\n:*?\"<>|]+\\[^\\\r\n:*?\"<>|]+(?:\\[^\\\r\n:*?\"<>|]+)*"
    r")|(?:"
    # POSIX absolute: /usr/local/bin (require at least 2 components and start with /)
    # Avoid matching reddit's /r/ or /u/
    r"/(?!r/|u/)(?:[^/\0\r\n\s]+/)+[^/\0\r\n\s]+"
    r"))"
)


class TextNormalizer(BaseModel):
    """
    Text normalizer for stylometric preprocessing of Reddit posts.

    Replaces non-stylometric content (URLs, emails, usernames) with standardized
    tokens while preserving linguistic style markers. Cleans Reddit-specific
    Markdown formatting to ensure consistent text representation.

    Example:
        >>> normalizer = TextNormalizer()
        >>> text = "Check out https://example.com!"
        >>> normalizer.normalize_text(text)
        'Check out [URL]!'
    """

    prefix: str = Field(default="[", description="Token prefix")
    suffix: str = Field(default="]", description="Token suffix")

    non_stylometric_matching: dict[str, str] = {
        "URL": regex_url,
        "EMAIL": regex_email,
        "USER": regex_reddit_user,
        "SUBREDDIT": regex_reddit_subreddit,
        "IP_ADDRESS": regex_ip_address,
        "PHONE": regex_phone,
        "FILEPATH": regex_filepaths,
    }

    reddit_formatting_matching: dict[str, str] = {
        # Markdown links: [text](url) -> text
        r"\[([^\]\n]+)\]\(([^\s\)\n]+)\)": r"\1",
        # Markdown bold/italic/strikethrough: **text**, *text*, __text__, _text_, ~~text~~
        r"(\*\*|__|\*|_|~~)(.*?)\1": r"\2",
        # Markdown blockquotes: > text -> text
        r"(?m)^>\s*": "",
        # Markdown headers: # text -> text
        r"(?m)^#{1,6}\s*": "",
        # Markdown code blocks: ```text``` or `text` -> text
        r"```.*?```": "",
        r"`([^`]+)`": r"\1",
        # Special zero-width / control
        # TODO: ​ and   are still not filtered properly
        r"[\u200b\u200c\u200d\u00ad\ufeff\u00a0]": "",
    }

    def normalize_text(self, text: str) -> str:
        """
        Applies normalization steps to the input text.

        Includes:
        1. HTML unescaping (e.g., &amp; -> &).
        2. Unicode normalization (NFKD).
        3. Reddit-specific formatting removal (Markdown links, headers, etc.).
        4. Replacement of non-stylometric elements (URLs, emails, etc.) with tokens.

        Args:
            text: The raw text to normalize.

        Returns:
            The normalized text with tokens and cleaned formatting.
        """
        if not isinstance(text, str):
            return ""

        # Unescape HTML entities (e.g., &amp; -> &, &gt; -> >)
        text = html.unescape(text)
        text = unicodedata.normalize("NFKD", text)

        for pattern, replacement in self.reddit_formatting_matching.items():
            text = re.sub(pattern, replacement, text)

        for key, pattern in self.non_stylometric_matching.items():
            replacement_token = f"{self.prefix}{key}{self.suffix}"
            text = re.sub(pattern, replacement_token, text)

        return text.strip()
