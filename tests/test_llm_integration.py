import json
import unittest
import urllib.request

from llm_integration import OpenAICompatibleLLMClient


class OpenAICompatibleLLMClientTests(unittest.TestCase):
    def test_uses_openai_compatible_endpoint_and_headers(self):
        captured = {}

        def transport(req: urllib.request.Request, timeout_seconds: int):
            captured["url"] = req.full_url
            captured["auth"] = req.headers.get("Authorization")
            captured["body"] = json.loads(req.data.decode("utf-8"))
            captured["timeout"] = timeout_seconds
            return {
                "id": "req_1",
                "choices": [{"message": {"content": '{"ok":true}'}}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
            }

        client = OpenAICompatibleLLMClient(
            api_key="secret",
            model="moonshot-v1",
            prompt_version="v2",
            transport=transport,
        )

        result = client.run_job("system", "user", max_tokens=300)

        self.assertEqual(captured["url"], "https://api.moonshot.ai/v1/chat/completions")
        self.assertEqual(captured["auth"], "Bearer secret")
        self.assertEqual(captured["body"]["temperature"], 0.2)
        self.assertEqual(captured["body"]["max_tokens"], 300)
        self.assertEqual(captured["body"]["response_format"], {"type": "json_object"})
        self.assertEqual(result.payload, {"ok": True})
        self.assertEqual(result.telemetry.model, "moonshot-v1")
        self.assertEqual(result.telemetry.prompt_version, "v2")
        self.assertEqual(result.telemetry.request_id, "req_1")
        self.assertEqual(result.telemetry.total_tokens, 15)

    def test_fallback_when_response_format_is_not_supported(self):
        calls = []

        def transport(req: urllib.request.Request, timeout_seconds: int):
            body = json.loads(req.data.decode("utf-8"))
            calls.append(body)
            if len(calls) == 1:
                return {"error": {"code": "unsupported_parameter", "message": "response_format not supported"}}
            return {"choices": [{"message": {"content": "```json\n{\"answer\": 42}\n```"}}]}

        client = OpenAICompatibleLLMClient(
            api_key="secret",
            model="moonshot-v1",
            prompt_version="v3",
            base_url="https://api.moonshot.cn/v1",
            transport=transport,
        )

        result = client.run_job("original", "question", max_tokens=120)

        self.assertEqual(len(calls), 2)
        self.assertNotIn("response_format", calls[1])
        self.assertIn("valid JSON object only", calls[1]["messages"][0]["content"])
        self.assertEqual(result.payload, {"answer": 42})


if __name__ == "__main__":
    unittest.main()
