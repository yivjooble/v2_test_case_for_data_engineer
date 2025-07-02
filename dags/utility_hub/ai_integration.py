"""Simple AI integration utility for OpenAI API calls.
"""

from openai import OpenAI
from typing import Optional

from utility_hub import logger


def call_openai_api(
    api_key: str,
    content: str,
    system_prompt: str,
    model: str = "gpt-4o-mini",
    max_tokens: int = 2000,
    temperature: float = 0.7
) -> Optional[str]:
    """Make a call to OpenAI API with the provided parameters.
    
    Args:
        api_key: OpenAI API key
        content: User content to send to the API
        system_prompt: System prompt for the AI
        model: OpenAI model to use (default: gpt-4o-mini)
        max_tokens: Maximum tokens in response (default: 2000)
        temperature: Sampling temperature (default: 0.7)
        
    Returns:
        Response content from OpenAI API or None if failed
    """
    try:
        client = OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content}
            ],
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        result = response.choices[0].message.content.strip()
        logger.info(f"OpenAI API call successful")
        return result
        
    except Exception as e:
        logger.error(f"OpenAI API call failed: {str(e)}")
        return None