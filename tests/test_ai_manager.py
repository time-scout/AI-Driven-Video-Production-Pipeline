import unittest
import json
from pathlib import Path
from AI_Manager import AIManager

class TestAIManager(unittest.TestCase):
    def setUp(self):
        # Create temporary config and secrets for testing
        self.test_dir = Path("test_temp")
        self.test_dir.mkdir(exist_ok=True)
        
        self.config_path = self.test_dir / "config.json"
        self.secrets_path = self.test_dir / "secrets.toml"
        
        self.dummy_config = {
            "ai_settings": {
                "providers": {
                    "Google": {"name": "Google", "base_url": ""},
                    "OpenRouter": {"name": "OpenRouter", "base_url": "https://openrouter.ai/api/v1"}
                },
                "models": [
                    {"id": "gemini", "provider": "Google", "model_string": "models/gemini-1.5-flash"},
                    {"id": "claude", "provider": "OpenRouter", "model_string": "anthropic/claude-3-sonnet"}
                ],
                "task_assignments": {
                    "text_processing": "gemini",
                    "script_writing": "claude"
                }
            }
        }
        
        with open(self.config_path, 'w') as f:
            json.dump(self.dummy_config, f)
            
        with open(self.secrets_path, 'w') as f:
            f.write('GOOGLE_API_KEY = "dummy_google_key"\n')
            f.write('OPENROUTER_API_KEY = "dummy_or_key"\n')

    def tearDown(self):
        # Cleanup
        if self.config_path.exists(): self.config_path.unlink()
        if self.secrets_path.exists(): self.secrets_path.unlink()
        if self.test_dir.exists(): self.test_dir.rmdir()

    def test_initialization(self):
        manager = AIManager(self.config_path, self.secrets_path)
        self.assertEqual(manager.config, self.dummy_config)
        self.assertIn("GOOGLE_API_KEY", manager.secrets)

    def test_key_mapping(self):
        manager = AIManager(self.config_path, self.secrets_path)
        self.assertEqual(manager._get_api_key_name_for_provider("Google"), "GOOGLE_API_KEY")
        self.assertEqual(manager._get_api_key_name_for_provider("OpenRouter"), "OPENROUTER_API_KEY")
        # Test default fallback
        self.assertEqual(manager._get_api_key_name_for_provider("Custom AI"), "CUSTOM_AI_API_KEY")

    def test_model_string_retrieval(self):
        manager = AIManager(self.config_path, self.secrets_path)
        self.assertEqual(manager.get_model_string_for_task("text_processing"), "models/gemini-1.5-flash")
        self.assertEqual(manager.get_model_string_for_task("script_writing"), "anthropic/claude-3-sonnet")

    def test_validation(self):
        manager = AIManager(self.config_path, self.secrets_path)
        result = manager.validate_configuration()
        self.assertTrue(result["valid"])
        self.assertEqual(len(result["issues"]), 0)

if __name__ == '__main__':
    unittest.main()
