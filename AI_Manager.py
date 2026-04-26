# AI_Manager.py
"""
Centralized AI Manager for managing various AI providers.
Supports flexible configuration of models and providers via config.json.
"""

import json
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib
from pathlib import Path
from typing import Dict, Any, Optional
import requests


class AIManager:
    """
    Central manager for working with AI APIs.
    Manages providers, models, and tasks via configuration.
    """

    def __init__(self, config_path: Path, secrets_path: Path):
        """
        AI Manager initialization.

        Args:
            config_path: Path to config.json with AI settings
            secrets_path: Path to secrets.toml with API keys
        """
        self.config_path = config_path
        self.secrets_path = secrets_path
        self.config = self._load_config()
        self.secrets = self._load_secrets()

    def _load_config(self) -> Dict[str, Any]:
        """Loading configuration from config.json"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise Exception(f"Error loading config.json: {e}")

    def _load_secrets(self) -> Dict[str, Any]:
        """Loading secrets from secrets.toml"""
        try:
            with open(self.secrets_path, 'rb') as f:
                return tomllib.load(f)
        except (FileNotFoundError, tomllib.TOMLKitError) as e:
            raise Exception(f"Error loading secrets.toml: {e}")

    def execute_ai_task(self, task_category: str, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executing AI task via assigned model.

        Args:
            task_category: Task category (e.g., "text_processing")
            input_data: Input data for the task

        Returns:
            Dict with result or error
        """
        try:
            # Get assigned model for task category
            ai_config = self.config.get('ai_settings', {})
            task_assignments = ai_config.get('task_assignments', {})
            model_id = task_assignments.get(task_category)

            if not model_id:
                return {"error": f"Model assignment not found for category '{task_category}'"}

            # Find model in configuration
            models = ai_config.get('models', [])
            model_config = None
            for model in models:
                if model.get('id') == model_id:
                    model_config = model
                    break

            if not model_config:
                return {"error": f"Model with ID '{model_id}' not found in configuration"}

            # Get provider for the model
            provider_name = model_config.get('provider')
            providers = ai_config.get('providers', {})
            provider_config = providers.get(provider_name)

            if not provider_config:
                return {"error": f"Provider '{provider_name}' not found in configuration"}

            # Get API key for provider
            api_key_name = self._get_api_key_name_for_provider(provider_name)
            api_key = self.secrets.get(api_key_name)

            if not api_key:
                return {"error": f"API key '{api_key_name}' not found in secrets.toml"}

            # Execute API request
            return self._execute_api_request(
                provider_config,
                model_config,
                api_key,
                input_data
            )

        except Exception as e:
            return {"error": f"Error executing AI task: {str(e)}"}

    def _get_api_key_name_for_provider(self, provider_name: str) -> str:
        """Get API key name for provider"""
        # Mapping providers to key names in secrets.toml
        key_mapping = {
            "OpenRouter": "OPENROUTER_API_KEY",
            "Google": "GOOGLE_API_KEY",
            "Google Account_1": "GOOGLE_API_KEY",
            "Google Account_2": "GOOGLE_API_KEY",
            "Googler": "GOOGLER_API_KEY",
            "Z AI": "Z_AI_API_KEY",
            # Add other providers as needed
        }
        return key_mapping.get(provider_name, f"{provider_name.upper().replace(' ', '_')}_API_KEY")

    def _execute_api_request(self, provider_config: Dict, model_config: Dict,
                           api_key: str, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executing HTTP request to provider API.
        """
        provider_name = provider_config.get('name')
        base_url = provider_config.get('base_url')
        model_string = model_config.get('model_string')

        if provider_name == "OpenRouter":
            return self._execute_openrouter_request(base_url, model_string, api_key, input_data)
        elif provider_name == "Google" or provider_name.startswith("Google "):
            return self._execute_google_request(base_url, model_string, api_key, input_data)
        elif provider_name == "Googler":
            return self._execute_googler_request(base_url, model_string, api_key, input_data)
        else:
            # For unknown providers, try standard OpenAI-compatible format
            return self._execute_openai_compatible_request(base_url, model_string, api_key, input_data, provider_name)

    def _execute_openrouter_request(self, base_url: str, model_string: str,
                                  api_key: str, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Executing request to OpenRouter API"""
        url = f"{base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost",
            "X-Title": "Centralized Montage"
        }

        prompt = input_data.get('prompt', '')
        data = {
            "model": model_string,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "max_tokens": 128000
        }

        try:
            response = requests.post(url, headers=headers, json=data, timeout=30)
            response.raise_for_status()

            result = response.json()
            print(f"\n[DEBUG OpenRouter] RAW RESPONSE: {result}\n")

            # Try to extract the response from different possible formats
            if 'choices' in result and len(result['choices']) > 0:
                choice = result['choices'][0]
                message = choice.get('message', {})
                # First try content
                content = message.get('content', '')
                if not content:
                    # If content is empty, try reasoning_content
                    content = message.get('reasoning_content', '')
                return {"text": content, "error": None}

        except requests.RequestException as e:
            return {"error": f"OpenRouter request error: {str(e)}"}

    def _execute_google_request(self, base_url: str, model_string: str,
                                api_key: str, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Executing request to Google AI API"""
        import google.generativeai as genai

        try:
            # API key configuration
            genai.configure(api_key=api_key)

            # Safety settings
            safety_settings = [
                {"category": c, "threshold": "BLOCK_NONE"} for c in
                ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH",
                 "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]
            ]

            # Model creation
            model = genai.GenerativeModel(model_string, safety_settings=safety_settings)

            # Extract prompt from input data
            prompt = input_data.get('prompt', '')

            # Generating content
            response = model.generate_content(prompt)
            print(f"\n[DEBUG Google] RAW RESPONSE: {response}\n")

            # Checking for blocks
            if not response.parts:
                reason = response.prompt_feedback.block_reason.name if response.prompt_feedback else "UNKNOWN"
                return {"error": f"Google AI blocked request: {reason}"}

            # Extracting response text
            content = response.text.strip()

            return {"text": content, "error": None}

        except Exception as e:
            return {"error": f"Google AI request error: {str(e)}"}

    def _execute_googler_request(self, base_url: str, model_string: str,
                                 api_key: str, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Executing request to Googler API"""
        url = f"{base_url}/api/chat"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        prompt = input_data.get('prompt', '')
        data = {
            "model": model_string,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "max_tokens": 128000
        }

        try:
            response = requests.post(url, headers=headers, json=data, timeout=30)
            response.raise_for_status()

            result = response.json()
            print(f"\n[DEBUG Googler] RAW RESPONSE: {result}\n")
            if 'response' in result:
                content = result['response']
                return {"text": content, "error": None}
            else:
                return {"error": "Invalid response format from Googler API"}

        except requests.RequestException as e:
            return {"error": f"Googler request error: {str(e)}"}

    def _execute_openai_compatible_request(self, base_url: str, model_string: str,
                                         api_key: str, input_data: Dict[str, Any],
                                         provider_name: str) -> Dict[str, Any]:
        """Executing request to OpenAI-compatible API (for unknown providers)"""
        # Try standard OpenAI endpoint
        url = f"{base_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        prompt = input_data.get('prompt', '')
        data = {
            "model": model_string,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "max_tokens": 128000
        }

        print(f"\n=== Z AI DEBUG ===")
        print(f"URL: {url}")
        print(f"Headers: {headers}")
        print(f"Data: {data}")
        print(f"Sending request...")

        import time
        start_time = time.time()

        try:
            response = requests.post(url, headers=headers, json=data, timeout=30)
            elapsed = time.time() - start_time
            print(f"Response received in {elapsed:.2f} seconds")
            print(f"Status code: {response.status_code}")
            print(f"Response text: {response.text[:500]}...")  # First 500 characters

            response.raise_for_status()

            result = response.json()
            print(f"Parsed JSON: {result}")
            print(f"\n[DEBUG Z_AI/OpenAI] RAW RESPONSE: {result}\n")

            # Try to extract the response from different possible formats
            if 'choices' in result and len(result['choices']) > 0:
                choice = result['choices'][0]
                message = choice.get('message', {})
                # First try content
                content = message.get('content', '')
                if not content:
                    # If content is empty, try reasoning_content
                    content = message.get('reasoning_content', '')
                print(f"Extracted content: {content[:200]}...")
                return {"text": content, "error": None}
            elif 'response' in result:
                # Alternative format (like Googler)
                content = result['response']
                return {"text": content, "error": None}
            elif 'text' in result:
                # Another possible format
                content = result['text']
                return {"text": content, "error": None}
            else:
                return {"error": f"Unknown response format from {provider_name} API"}

        except requests.exceptions.Timeout:
            elapsed = time.time() - start_time
            print(f"TIMEOUT after {elapsed:.2f} seconds")
            return {"error": f"Connection timeout for {provider_name} API (30 seconds)."}
        except requests.exceptions.ConnectionError as e:
            elapsed = time.time() - start_time
            print(f"CONNECTION ERROR after {elapsed:.2f} seconds: {e}")
            return {"error": f"Connection error for {provider_name}: {str(e)}. URL: {url}"}
        except requests.RequestException as e:
            elapsed = time.time() - start_time
            print(f"REQUEST ERROR after {elapsed:.2f} seconds: {e}")
            return {"error": f"Request error for {provider_name}: {str(e)}"}
        finally:
            print(f"=== END Z AI DEBUG ===\n")

    def get_model_string_for_task(self, task_category: str) -> str:
        """
        Retrieves the friendly model string (e.g. 'google/gemini-2.0-flash-exp') for a given task.
        """
        try:
            ai_config = self.config.get('ai_settings', {})
            task_assignments = ai_config.get('task_assignments', {})
            model_id = task_assignments.get(task_category)
            if not model_id: return "Unknown Model"

            models = ai_config.get('models', [])
            for model in models:
                if model.get('id') == model_id:
                    return model.get('model_string', "Unknown Model String")
            return "Unknown Model ID"
        except: return "Error Retrieval"

    def get_available_models_for_task(self, task_category: str) -> list:
        """
        Get list of available models for task category.

        Returns:
            List of dictionaries with model information
        """
        ai_config = self.config.get('ai_settings', {})
        models = ai_config.get('models', [])
        providers = ai_config.get('providers', {})

        available_models = []
        for model in models:
            provider_name = model.get('provider')
            if provider_name in providers:
                available_models.append({
                    'id': model.get('id'),
                    'name': model.get('model_name'),
                    'provider': provider_name,
                    'model_string': model.get('model_string')
                })

        return available_models

    def validate_configuration(self) -> Dict[str, Any]:
        """
        AI configuration validation.

        Returns:
            Dict with validation results
        """
        issues = []

        ai_config = self.config.get('ai_settings', {})
        if not ai_config:
            issues.append("Section 'ai_settings' missing in config.json")

        providers = ai_config.get('providers', {})
        if not providers:
            issues.append("Provider list is empty")

        models = ai_config.get('models', [])
        if not models:
            issues.append("Model list is empty")

        task_assignments = ai_config.get('task_assignments', {})
        if not task_assignments:
            issues.append("Task assignments missing")

        # Checking API keys
        for provider_name in providers.keys():
            api_key_name = self._get_api_key_name_for_provider(provider_name)
            if api_key_name not in self.secrets:
                issues.append(f"API key '{api_key_name}' for provider '{provider_name}' missing in secrets.toml")

        return {
            "valid": len(issues) == 0,
            "issues": issues
        }