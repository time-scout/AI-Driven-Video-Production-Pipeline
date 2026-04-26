from pathlib import Path
from typing import Optional

def rewrite_text_with_ai_manager(text_to_rewrite: str, prompt_path: Path, ai_manager) -> Optional[str]:
    """
    Rewrites text using AI Manager with category "version_creation".

    Args:
        text_to_rewrite: Original text to rewrite
        prompt_path: Path to prompt file
        ai_manager: AI Manager instance for processing requests

    Returns:
        Rewritten text or None in case of error
    """

    try:
        system_prompt = prompt_path.read_text(encoding='utf-8')
    except FileNotFoundError:
        print(f"ERROR: Prompt file not found: {prompt_path}");
        return None
    except Exception as e:
        print(f"ERROR: Failed to read prompt file: {e}");
        return None

    try:
        # Form complete prompt
        full_prompt = f"{system_prompt}\n\n{text_to_rewrite}"

        # Use AI Manager with category "version_creation"
        response = ai_manager.execute_ai_task(
            task_category="version_creation",
            input_data={"prompt": full_prompt}
        )

        if response.get("error"):
            print(f"WARNING: AI Manager returned error: {response['error']}")
            return None

        result_text = response.get("text")
        if result_text is None:
            print(f"WARNING: AI Manager returned empty text")
            return None

        return result_text.strip()

    except Exception as e:
        # Output more detailed error info
        print(f"ERROR: An error occurred during AI Manager call. Details: {e}")
        import traceback
        print(traceback.format_exc())
        return None