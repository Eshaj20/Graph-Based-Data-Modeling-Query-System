import json

import app


def main() -> None:
    provider = app.query_engine.llm.provider or "<not set>"
    print(f"LLM provider: {provider}")
    print(f"LLM available: {app.query_engine.llm.available()}")
    if not app.query_engine.llm.available():
        print("No LLM API key is configured. Set one of GROQ_API_KEY, OPENROUTER_API_KEY, or GEMINI_API_KEY first.")
        return

    prompts = [
        "Which products are associated with the highest number of billing documents?",
        "Trace the full flow of billing document 90504219",
        "Find the address for customer 310000108",
    ]
    for prompt in prompts:
        print(f"\nPrompt: {prompt}")
        result = app.query_engine.run(prompt)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
