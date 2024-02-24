import backoff
from dagster import ConfigurableResource
from openai import OpenAI, RateLimitError

from mormon_queer_analysis.utils.embeddings_utils import get_embedding


class OpenAIClientResource(ConfigurableResource):
    openai_api_key: str

    @backoff.on_exception(backoff.expo, RateLimitError, max_tries=8)
    def completions_with_backoff(self, model, messages):
        """Use OpenAI's chat completions API, but back off if it gets a rate limit error."""
        client = OpenAI(api_key=self.openai_api_key)
        response = client.Completion.create(
            model=model,
            messages=messages,
        )
        return response

    def get_model_embedding(self, x, model):
        client = OpenAI(api_key=self.openai_api_key)
        return get_embedding(client, x, model=model)
