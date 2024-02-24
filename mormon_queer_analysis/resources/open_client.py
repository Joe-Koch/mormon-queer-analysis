from typing import List

import backoff
from dagster import ConfigurableResource
from openai import OpenAI, RateLimitError

from mormon_queer_analysis.utils.embeddings_utils import get_embedding


def chat_completions(
    openai_api_key: str, model: str, prompt: str, texts: List[str]
) -> str:
    """Use OpenAI's chat completions API. Handles the formatting more intuitively."""

    client = OpenAI(api_key=openai_api_key)

    # Format the messages for openAI
    messages = [{"role": "system", "content": prompt + texts}]

    response = client.chat.completions.create(
        model=model,
        messages=messages,
    )

    # Return the message content
    content = response.choices[0].message.content

    return content


class OpenAIClientResource(ConfigurableResource):
    openai_api_key: str

    @backoff.on_exception(backoff.expo, RateLimitError, max_tries=8)
    def completions_with_backoff(
        self, model: str, prompt: str, texts: List[str]
    ) -> str:
        """Use OpenAI's chat completions API, but back off if it gets a rate limit error."""
        return chat_completions(self.openai_api_key, model, prompt, texts)

    def get_model_embedding(self, x, model):
        client = OpenAI(api_key=self.openai_api_key)
        return get_embedding(client, x, model=model)


class OpenAISubsampleClientResource(OpenAIClientResource):
    openai_api_key: str

    @backoff.on_exception(backoff.expo, RateLimitError, max_tries=8)
    def completions_with_backoff(
        self, model: str, prompt: str, texts: List[str]
    ) -> str:
        """
        Overrides the base method to only use a subsample of the texts when making the API call.
        """
        n_texts = 5
        texts = texts[:n_texts]
        return chat_completions(self.openai_api_key, model, prompt, texts)
