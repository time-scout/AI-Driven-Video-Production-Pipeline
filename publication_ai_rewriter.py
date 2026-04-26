# publication_ai_rewriter.py (v1.1 - with approved prompts)

import google.generativeai as genai
import re


# --- Helper function for API calls ---
def _call_gemini_api(api_key: str, prompt: str):
    """
    Universal function to call Gemini API.
    Returns response text or None in case of error.
    """
    try:
        genai.configure(api_key=api_key)
        safety_settings = [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
        ]
        model = genai.GenerativeModel('models/gemini-flash-latest', safety_settings=safety_settings)

        response = model.generate_content(prompt)

        if not response.parts:
            reason = response.prompt_feedback.block_reason.name if response.prompt_feedback else "UNKNOWN"
            return f"API_ERROR: Response was blocked by safety settings. Reason: {reason}"

        return response.text.strip()
    except Exception as e:
        return f"API_ERROR: {e}"


# --- Functions for specific tasks ---

def rewrite_title(api_key: str, original_title: str) -> list[str]:
    """
    Generates 10 variant titles for YouTube video.
    """
    # --- APPROVED PROMPT 1 ---
    prompt = f"""You are an expert in creating viral YouTube titles. 
Given the original title below, generate 10 alternative titles.

Each title must be on a new line. Your response must contain ONLY the titles.

Original Title: "{original_title}"
"""

    response_text = _call_gemini_api(api_key, prompt)

    if response_text is None or response_text.startswith("API_ERROR:"):
        return [response_text or "API_ERROR: Unknown error"]

    titles = [line.strip() for line in response_text.split('\n') if line.strip()]
    return titles


def rewrite_preview_text(api_key: str, original_text: str) -> list[str]:
    """
    Generates 10 variant texts for thumbnail.
    """
    # --- APPROVED PROMPT 2 ---
    prompt = f"""Rewrite this short text for a YouTube video thumbnail.
Provide 10 distinct options. Each option must be on a new line.
Your response must contain ONLY the rewritten text options.

Original Text: "{original_text}"
"""

    response_text = _call_gemini_api(api_key, prompt)

    if response_text is None or response_text.startswith("API_ERROR:"):
        return [response_text or "API_ERROR: Unknown error"]

    preview_texts = [line.strip() for line in response_text.split('\n') if line.strip()]
    return preview_texts


def rewrite_description(api_key: str, original_description: str) -> str:
    """
    Rewrites video description, removing hashtags.
    """
    # --- APPROVED PROMPT 3 ---
    prompt = f"""Perform a professional rewrite of this YouTube video description.
Preserve all key facts, names, and any links, but rephrase the text in a more engaging and well-structured language.
Your response must be ONLY the rewritten description text.
You must remove the hashtags starting with "#" symbol.

Original Description:
---
{original_description}
---
"""

    response_text = _call_gemini_api(api_key, prompt)

    return response_text